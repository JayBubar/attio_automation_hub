"""
Daily outreach batch generator.

For one rep, pulls up to BATCH_SIZE "Never Contacted" people who share the
same company vertical + size, drafts 3 emails per contact via Claude, creates
the Email 1 -> Email 2 -> Call -> Left-a-VM task cadence in Attio, writes the
email drafts to MotherDuck so the Task Runner can show them, and flips each
contact's Prospect Path to "In Outreach".

Segmentation, verified directly against your workspace:
  - People.prospect_path (status): Never Contacted / In Outreach / Cold-Retry
    Pending / Engaged / Lead / Opportunity / Client / Not Interested /
    Partner / Dormant
  - People.contact_owner (select): Jay Bubar / Kurt Haas / Joel Weinbach /
    Edna Stone
  - People.ac_prospect_suppression (select): Unsubscribed / Prospect /
    Cold Lead / Warm Lead  -- Unsubscribed is always excluded
  - Companies.vertical_market (select): Community College / Faith Based /
    General Non-Profit / HealthCare / Higher Ed / Higher Ed Foundation /
    K-12 / Major Account / Other / Partner-Consultant
  - Companies.employee_range (select): 1-10 / 11-50 / 51-250 / 251-1K / ...

Edna Stone's contacts are being redistributed to Jay (per your migration
notes), so running this with --rep jay also pulls anything still owned by
"Edna Stone".

Usage:
  py scripts/outreach.py --rep kurt --dry-run
  py scripts/outreach.py --rep kurt
  py scripts/outreach.py --rep jay --batch-size 25

Also triggered over HTTP by the Ops Center via POST /trigger/outreach-batch
on this same service (see ops_center_routes.py), which shells out to this
file. It lives under automation_server/ so that it ships inside the Railway
container -- the service's Root Directory is automation_server, so anything
outside that folder is not present at runtime.

Requires:
  ATTIO_API_KEY, ANTHROPIC_API_KEY, MOTHERDUCK_TOKEN environment variables
  pip install requests anthropic duckdb --break-system-packages

Cadence created per contact (all tasks linked to the person record):
  Day 0            Email 1  (content: "Email 1/3 · Company: Name")
  Day 3            Email 2  (content: "Email 2/3 · Company: Name")
  Day 4            Call     (content: "Call 1/1 · Company: Name")
  Day 4            Left a VM email (content: "Left a VM · Company: Name")
                    -- created alongside the call but meant to be skipped in
                    the Task Runner if the call actually connects

MotherDuck table this writes to (created automatically if missing):
  hubspot_email_archive.main.outreach_email_drafts
    task_id, contact_record_id, sequence_position, subject, body, created_at

Checkpoint table: hubspot_email_archive.main.outreach_batch_checkpoint
  Tracks every contact_record_id ever pulled into a batch, so re-running
  never re-selects the same contact. Written one row at a time, immediately
  after each contact's tasks are created -- so a crash, a timeout, or a
  Railway redeploy mid-batch leaves the already-processed contacts recorded
  and they are not re-selected on the next run.

  This used to be a local CSV. On Railway the container filesystem does not
  survive a redeploy, so the CSV would silently reset and the next run would
  re-email everyone.
"""

import os
import json
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import duckdb
import requests

ATTIO_API_BASE = "https://api.attio.com/v2"
MOTHERDUCK_DB = "hubspot_email_archive"

# How many not-yet-batched candidates to enrich before bucketing by segment.
# The pool this selects from is five figures per owner; enriching all of them
# costs one Attio company read each and is what the batch is actually sliced
# from, so it is capped. Raise it for a wider segment search at the cost of a
# slower run.
ENRICH_POOL_SIZE = 400
COMPANY_LOOKUP_WORKERS = 8

_thread_local = threading.local()

REPS = {
    "kurt": {"member_id": "928e9d43-504b-4e51-8db2-54c4c40d0ecf", "name": "Kurt Haas",
             "owner_names": ["Kurt Haas"]},
    "jay": {"member_id": "acc65c82-459c-46a7-bd87-da84d6c4fcd5", "name": "Jay Bubar",
            "owner_names": ["Jay Bubar", "Edna Stone"]},  # Edna's contacts redistribute to Jay
    "joel": {"member_id": "ca4b7dfe-0f51-4631-89df-8b2d8583cd8d", "name": "Joel Weinbach",
              "owner_names": ["Joel Weinbach"]},
}

CLAUDE_MODEL = "claude-sonnet-5"  # change if you'd rather use a different model


def attio_session():
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {os.environ['ATTIO_API_KEY']}",
        "Content-Type": "application/json",
    })
    return s


def md_connection():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")


def ensure_checkpoint_table(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MOTHERDUCK_DB}.main.outreach_batch_checkpoint (
            contact_record_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            company VARCHAR,
            batch_date DATE,
            rep VARCHAR
        )
    """)


def load_checkpoint(con):
    """Returns set(contact_record_id) -- same shape build_batch() already
    expects, so the selection logic needs no changes."""
    rows = con.execute(
        f"SELECT contact_record_id FROM {MOTHERDUCK_DB}.main.outreach_batch_checkpoint"
    ).fetchall()
    return {r[0] for r in rows}


def append_checkpoint(con, row):
    """One row: [contact_record_id, name, company, batch_date_iso, rep_name].

    Called once per contact, immediately after that contact's tasks exist in
    Attio -- deliberately not batched to the end of the run, because a
    partial run that loses its checkpoint means re-emailing real people."""
    con.execute(
        f"""INSERT OR REPLACE INTO {MOTHERDUCK_DB}.main.outreach_batch_checkpoint
            (contact_record_id, name, company, batch_date, rep)
            VALUES (?, ?, ?, ?, ?)""",
        row,
    )


def select_value(values, slug):
    """Defensive read for select/status attributes -- Attio returns these as
    {"option": {"title": ...}} for select and {"status": {"title": ...}} for
    status types. Falls back gracefully if the shape differs."""
    v = values.get(slug)
    if not v:
        return None
    entry = v[0]
    if "option" in entry:
        return entry["option"].get("title")
    if "status" in entry:
        return entry["status"].get("title")
    return entry.get("value")


def fetch_candidates(session, rep, already_seen, pool_size=ENRICH_POOL_SIZE):
    """Query People: prospect_path = Never Contacted, owned by this rep
    (or Edna Stone for Jay), excluding unsubscribed. Attio caps at 500/page.

    Stops paging as soon as pool_size not-yet-batched candidates are in hand.
    The Never Contacted pool runs to five figures per owner, and every
    candidate we keep costs a company lookup later -- pulling all of them to
    then slice off 25 meant the run took the same time whether batch_size was
    2 or 25, and blew past the Railway edge timeout either way.
    """
    owner_or = [{"contact_owner": {"$eq": name}} for name in rep["owner_names"]]
    filter_body = {
        "$and": [
            {"prospect_path": {"$eq": "Never Contacted"}},
            {"$or": owner_or},
            {"$not": {"ac_prospect_suppression": {"$eq": "Unsubscribed"}}},
        ]
    }
    unseen = []
    scanned = 0
    offset = 0
    limit = 500
    while True:
        resp = session.post(
            f"{ATTIO_API_BASE}/objects/people/records/query",
            json={"filter": filter_body, "limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        page = resp.json().get("data", [])
        scanned += len(page)
        unseen.extend(r for r in page if r["id"]["record_id"] not in already_seen)
        if len(unseen) >= pool_size or len(page) < limit:
            break
        offset += limit
    return unseen[:pool_size], scanned


def extract_ref_ids(ref_value):
    """A record-reference value's exact key names aren't 100% confirmed from
    docs alone, so try every plausible shape rather than guessing once."""
    ref = ref_value[0] if isinstance(ref_value, list) else ref_value
    object_id = ref.get("target_object_id") or ref.get("object_id") or ref.get("target_object")
    record_id = ref.get("target_record_id") or ref.get("record_id")
    return object_id, record_id


def fetch_company_segment(company_object_id, company_record_id):
    """Returns (vertical_market, employee_range, company_name) for one company.

    Uses a thread-local Session: requests.Session is not thread-safe, and
    these run concurrently.
    """
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = _thread_local.session = attio_session()
    try:
        resp = session.get(f"{ATTIO_API_BASE}/objects/{company_object_id}/records/{company_record_id}")
        resp.raise_for_status()
        values = resp.json()["data"]["values"]
        vertical = select_value(values, "vertical_market")
        size = select_value(values, "employee_range")
        name_val = values.get("name")
        name = name_val[0]["value"] if name_val else "(unknown company)"
        return vertical, size, name
    except Exception as e:
        print(f"    [company lookup failed for object_id={company_object_id} record_id={company_record_id}: {e}]")
        return None, None, "(unknown company)"


def fetch_company_segments(refs):
    """refs: set of (company_object_id, company_record_id). Returns a dict
    keyed by company_record_id. Fetched concurrently -- these are the bulk of
    the wall time and Attio has no batch-read endpoint for them."""
    out = {}
    if not refs:
        return out
    with ThreadPoolExecutor(max_workers=COMPANY_LOOKUP_WORKERS) as pool:
        futures = {
            pool.submit(fetch_company_segment, obj_id, rec_id): rec_id
            for obj_id, rec_id in refs
        }
        for fut in as_completed(futures):
            out[futures[fut]] = fut.result()
    return out


def build_batch(session, rep, batch_size, already_seen):
    raw, scanned = fetch_candidates(session, rep, already_seen)
    print(f"Scanned {scanned} Never Contacted records, {len(raw)} not yet batched "
          f"(capped at {ENRICH_POOL_SIZE} for segmentation).")

    # First pass: parse the person payloads we already have. No HTTP here.
    parsed = []
    company_refs = set()
    for r in raw:
        values = r.get("values", {})
        name_val = values.get("name")
        name = name_val[0].get("full_name") if name_val else "(unknown)"
        email_val = values.get("email_addresses")
        email = email_val[0].get("email_address") if email_val else None
        if not email:
            continue  # can't email someone with no email on file
        job_title = select_value(values, "job_title") or (values.get("job_title", [{}])[0].get("value") if values.get("job_title") else "")
        persona = select_value(values, "persona")
        company_ref = values.get("company")
        if not company_ref:
            continue
        company_object_id, company_record_id = extract_ref_ids(company_ref)
        if not company_object_id or not company_record_id:
            print(f"    [could not parse company reference for {name}: {company_ref}]")
            continue
        company_refs.add((company_object_id, company_record_id))
        parsed.append({
            "record_id": r["id"]["record_id"], "name": name, "email": email,
            "job_title": job_title, "persona": persona,
            "company_record_id": company_record_id,
        })

    # Second pass: one concurrent round of company lookups for the distinct
    # companies only -- many contacts share one.
    print(f"Looking up {len(company_refs)} distinct companies...")
    segments = fetch_company_segments(company_refs)

    enriched = []
    for c in parsed:
        vertical, size, company_name = segments.get(
            c["company_record_id"], (None, None, "(unknown company)")
        )
        enriched.append({
            "record_id": c["record_id"], "name": c["name"], "email": c["email"],
            "job_title": c["job_title"], "persona": c["persona"],
            "company_name": company_name, "vertical": vertical, "size": size,
        })

    # Bucket by (vertical, size); take the largest bucket, filling from the
    # next-largest if it doesn't reach batch_size.
    buckets = {}
    for c in enriched:
        key = (c["vertical"], c["size"])
        buckets.setdefault(key, []).append(c)

    ordered_buckets = sorted(buckets.items(), key=lambda kv: -len(kv[1]))
    batch = []
    for key, members in ordered_buckets:
        if len(batch) >= batch_size:
            break
        take = members[: batch_size - len(batch)]
        batch.extend(take)

    return batch, ordered_buckets


def generate_emails(contact, rep_name):
    """Calls Claude to draft Email 1, Email 2, and the left-a-VM follow-up.
    Returns a dict keyed by sequence_position. Retries on transient
    server-side errors (529 overloaded, 5xx) since these clear up quickly
    and shouldn't cost you most of a batch."""
    import anthropic
    client = anthropic.Anthropic()

    prompt = f"""You are drafting cold outreach emails for a RaiseTell sales rep named {rep_name}.
RaiseTell sells fundraising analytics/reporting software to nonprofits.

Contact: {contact['name']}, {contact['job_title']} at {contact['company_name']}
Org type: {contact['vertical'] or 'nonprofit'}, size: {contact['size'] or 'unknown'}

Write three short, plain-spoken emails (under 120 words each, no corporate jargon,
one clear call to action, signed "{rep_name}"):
1. email_1: first outreach email, a specific pain point relevant to this org type
2. email_2: a brief follow-up 3 days later, different angle, still short
3. left_vm: a short "sorry I missed you" note to send right after leaving a voicemail

Return ONLY valid JSON, no markdown fences, no preamble, in this exact shape:
{{"email_1": {{"subject": "...", "body": "..."}}, "email_2": {{"subject": "...", "body": "..."}}, "left_vm": {{"subject": "...", "body": "..."}}}}"""

    max_retries = 4
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1200,
                messages=[{"role": "user", "content": prompt}],
            )
            text_parts = [block.text for block in resp.content if getattr(block, "type", None) == "text"]
            text = "".join(text_parts).strip()
            text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            return json.loads(text)
        except anthropic.APIStatusError as e:
            if e.status_code in (429, 500, 502, 503, 529) and attempt < max_retries:
                wait = 2 ** attempt
                print(f"    Claude API busy ({e.status_code}), retrying in {wait}s (attempt {attempt}/{max_retries})")
                time.sleep(wait)
                continue
            raise


def create_task(session, content, deadline, rep_member_id, person_record_id):
    resp = session.post(
        f"{ATTIO_API_BASE}/tasks",
        json={
            "data": {
                "content": content,
                "format": "plaintext",
                "deadline_at": deadline.isoformat(),
                "is_completed": False,
                "linked_records": [{"target_object": "people", "target_record_id": person_record_id}],
                "assignees": [{"referenced_actor_type": "workspace-member", "referenced_actor_id": rep_member_id}],
            }
        },
    )
    resp.raise_for_status()
    return resp.json()["data"]["id"]["task_id"]


def write_draft(con, task_id, contact_record_id, sequence_position, draft):
    # INSERT OR REPLACE rather than plain INSERT: task_id is the primary key,
    # and a retried contact would otherwise hard-fail on the constraint.
    con.execute(
        f"""INSERT OR REPLACE INTO {MOTHERDUCK_DB}.main.outreach_email_drafts
            (task_id, contact_record_id, sequence_position, subject, body, created_at)
            VALUES (?, ?, ?, ?, ?, ?)""",
        [task_id, contact_record_id, sequence_position, draft["subject"], draft["body"],
         datetime.now(timezone.utc)],
    )


def ensure_drafts_table(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MOTHERDUCK_DB}.main.outreach_email_drafts (
            task_id VARCHAR PRIMARY KEY,
            contact_record_id VARCHAR,
            sequence_position VARCHAR,
            subject VARCHAR,
            body VARCHAR,
            created_at TIMESTAMP
        )
    """)


def update_prospect_path(session, record_id, new_status):
    """Value shape matches what smartlead_routes.py / activecampaign_routes.py
    already send in production -- a list of {"value": ...}, not a bare string.
    Raises rather than returning False: silently failing to move someone to
    "In Outreach" leaves them in the Never Contacted pool with live tasks
    against them."""
    resp = session.patch(
        f"{ATTIO_API_BASE}/objects/people/records/{record_id}",
        json={"data": {"values": {
            "prospect_path": [{"value": new_status}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }}},
    )
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rep", required=True, choices=list(REPS.keys()))
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rep = REPS[args.rep]
    session = attio_session()

    # One MotherDuck connection for the whole run -- checkpoint reads/writes
    # and draft writes share it. Opened before candidate selection because
    # the checkpoint now lives here, so even --dry-run needs it to know who
    # has already been batched.
    con = md_connection()
    ensure_checkpoint_table(con)
    ensure_drafts_table(con)
    already_seen = load_checkpoint(con)

    print(f"Pulling 'Never Contacted' candidates for {rep['name']}...")
    print(f"Checkpoint: {len(already_seen)} contacts previously batched, excluded.")
    batch, ordered_buckets = build_batch(session, rep, args.batch_size, already_seen)

    print("\nSegment mix found (vertical, size -> candidate count):")
    for key, members in ordered_buckets[:8]:
        print(f"  {key} -> {len(members)}")
    print(f"\nSelected batch: {len(batch)} contacts, segment {(batch[0]['vertical'], batch[0]['size']) if batch else 'n/a'}")

    if not batch:
        print("No eligible candidates found (check that this rep has unpicked 'Never Contacted' people).")
        con.close()
        return

    if args.dry_run:
        print("\nDry run -- contacts that WOULD be batched:")
        for c in batch:
            print(f"  {c['name']} ({c['job_title']}) at {c['company_name']} <{c['email']}>")
        print("\nNo emails generated, no tasks created, no drafts written. Re-run without --dry-run to proceed.")
        con.close()
        return

    today = datetime.now(timezone.utc).replace(hour=17, minute=0, second=0, microsecond=0)
    completed = 0
    failed = []

    for i, contact in enumerate(batch, 1):
        print(f"[{i}/{len(batch)}] {contact['name']} @ {contact['company_name']}")
        try:
            drafts = generate_emails(contact, rep["name"])

            email1_task = create_task(
                session, f"Email 1/3 · {contact['company_name']}: {contact['name']}",
                today, rep["member_id"], contact["record_id"],
            )
            write_draft(con, email1_task, contact["record_id"], "email_1", drafts["email_1"])

            email2_task = create_task(
                session, f"Email 2/3 · {contact['company_name']}: {contact['name']}",
                today + timedelta(days=3), rep["member_id"], contact["record_id"],
            )
            write_draft(con, email2_task, contact["record_id"], "email_2", drafts["email_2"])

            create_task(
                session, f"Call 1/1 · {contact['company_name']}: {contact['name']}",
                today + timedelta(days=4), rep["member_id"], contact["record_id"],
            )

            vm_task = create_task(
                session, f"Left a VM · {contact['company_name']}: {contact['name']}",
                today + timedelta(days=4), rep["member_id"], contact["record_id"],
            )
            write_draft(con, vm_task, contact["record_id"], "left_vm", drafts["left_vm"])

            update_prospect_path(session, contact["record_id"], "In Outreach")

            # Checkpoint immediately: from here on this contact has real
            # Attio tasks against them and must never be re-selected, even
            # if the process dies on the next iteration.
            append_checkpoint(con, [
                contact["record_id"], contact["name"], contact["company_name"],
                today.date().isoformat(), rep["name"],
            ])
            completed += 1
        except Exception as e:
            print(f"  FAILED: {e}")
            failed.append(f"{contact['name']} @ {contact['company_name']}: {e}")

    con.close()
    print(f"\nDone. {completed}/{len(batch)} contacts moved into outreach for {rep['name']}.")
    if failed:
        print(f"{len(failed)} failed (not checkpointed, will be retried on the next run):")
        for f in failed:
            print(f"  - {f}")


if __name__ == "__main__":
    main()