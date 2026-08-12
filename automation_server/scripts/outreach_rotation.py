"""
outreach_rotation.py

Daily job that rotates contacts through ActiveCampaign and Smartlead outreach,
keeping Attio as the source of truth. Run once per day (Windows Task Scheduler
or manually via `py outreach_rotation.py`).

What it does, in order:
  1. Reads current capacity for AC (5,000 contact cap) and Smartlead
     (per-campaign max_leads_per_day, read live from the Smartlead API).
  2. Evicts stale contacts from each channel to free up room:
       - Smartlead: sequence exhausted with no reply after N days
       - AC: no engagement after N days on the tag
     Evicted contacts get pulled off the channel, flagged off, and moved to
     Prospect Path "Cold/Retry Pending" in Attio.
  3. Fills the freed + any open headroom with new contacts pulled from Attio's
     "Never Contacted" pool.
  4. Pushes the new batch to the channel and immediately updates Attio state:
     Prospect Path -> "In Outreach", channel flag -> True, list membership,
     Last Path Change Date -> today.

Idempotency: every run logs pushed record IDs + channel + date to a MotherDuck
checkpoint table (`outreach_rotation_log`) before touching any API, so a
crashed/re-run script does not double-push. All bulk Attio operations use the
two-phase pattern: collect IDs first with a stable query, then process.

Environment variables required (set with `set VAR=value`, no quotes):
  ATTIO_API_KEY
  ACTIVECAMPAIGN_API_URL       e.g. https://raisetell.api-us1.com
  ACTIVECAMPAIGN_API_KEY
  ACTIVECAMPAIGN_LIST_ID       AC's own numeric list id, used against AC's API
  ATTIO_AC_TARGET_LIST_ID      Attio list UUID for "Current Marketing Prospects
                               in ActiveCampaign" -- a different identifier from
                               the line above, see the note by the constants
  ACTIVECAMPAIGN_TAG_ID        "Attio Marketing Contact" tag id (3)
  ACTIVECAMPAIGN_MAX_CONTACTS  e.g. 5000
  ACTIVECAMPAIGN_STALE_DAYS    days on the tag with no engagement before evict
  SMARTLEAD_API_KEY
  SMARTLEAD_CAMPAIGN_ID        the campaign your 15 warmed inboxes send from
  MOTHERDUCK_TOKEN
  MOTHERDUCK_DATABASE          e.g. hubspot_email_archive
  DRY_RUN                      "true" to log actions without calling write APIs
  AC_PAUSED                    starting value for the pause flag; the Ops Center
                               toggle in MotherDuck overrides it (see
                               automation_config.py)
"""

import os
import sys
import time
import json
import random
from datetime import datetime, timedelta, timezone

import requests
import duckdb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import automation_config  # noqa: E402  -- needs the path insert above

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"

AC_BASE = os.environ["ACTIVECAMPAIGN_API_URL"].rstrip("/")
AC_API_KEY = os.environ["ACTIVECAMPAIGN_API_KEY"]
AC_TAG_ID = os.environ["ACTIVECAMPAIGN_TAG_ID"]
AC_MAX_CONTACTS = int(os.environ.get("ACTIVECAMPAIGN_MAX_CONTACTS", "5000"))

# Days on the tag without engagement before a contact is evicted back to
# Cold/Retry Pending. Was referenced by evict_stale_ac() but never defined --
# the function raised NameError on its first call, hidden only because
# AC_PAUSED kept it unreachable.
AC_STALE_DAYS = int(os.environ.get("ACTIVECAMPAIGN_STALE_DAYS", "45"))

# There is no `ac_contact_id` attribute on People in the live workspace, so
# writing it 400s the entire PATCH -- taking the path and flag updates down
# with it. Left unset, the AC contact id is simply not mirrored into Attio and
# eviction falls back to an email lookup. Create the attribute in Attio, set
# this to its slug, and the round-trip works. See README "Manual prerequisites".
AC_CONTACT_ID_SLUG = os.environ.get("ATTIO_AC_CONTACT_ID_SLUG", "")

# Two different systems, two different identifiers, and they were sharing one
# env var: AC_LIST_ID goes to AC's /api/3/contactLists, ATTIO_AC_LIST_ID is the
# Attio list UUID for "Current Marketing Prospects in ActiveCampaign". Whichever
# value ACTIVECAMPAIGN_LIST_ID held, one of the two call sites was wrong.
AC_LIST_ID = os.environ["ACTIVECAMPAIGN_LIST_ID"]
ATTIO_AC_LIST_ID = os.environ.get(
    "ATTIO_AC_TARGET_LIST_ID", "e65a24d3-711f-410c-94aa-83a4738aaad6"
)

# Attio list "Add to Smartlead" -- a hand-curated intake queue. Anyone dropped
# in here by a human is pushed to Smartlead ahead of the Never Contacted pool,
# and their entry is removed once pushed, so the list drains as it is worked.
#
# It had been sitting at 90+ people since 2026-07-29 with nothing reading it:
# push_smartlead_batch only ever pulled from the Never Contacted pool, so a
# deliberate human "put these in outreach" did nothing at all and looked
# exactly like a queue that was being processed.
ATTIO_SMARTLEAD_INTAKE_LIST_ID = os.environ.get(
    "ATTIO_SMARTLEAD_INTAKE_LIST_ID", "bb25b08c-a228-40f3-aa64-1fbad9bbfee3"
)

# There is deliberately no Attio "Smartlead Target List" write here, though the
# AC path does add to its equivalent. That list (smartlead_target_list) holds no
# custom attributes, so membership carried nothing that the
# active_cold_outreach_contact checkbox does not already carry -- and that
# checkbox is what the Smartlead webhook handlers clear and what sizes the pool
# below, making it the one signal that actually decays. The "when did this
# person enter outreach" question the list might have answered is served by the
# outreach_rotation_log checkpoint table instead. Two sources of truth for one
# fact is how they drift.

SMARTLEAD_API_KEY = os.environ["SMARTLEAD_API_KEY"]
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"
SMARTLEAD_CAMPAIGN_ID = os.environ["SMARTLEAD_CAMPAIGN_ID"]

MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"

# AC pause is a runtime flag now, not a constant -- resuming is an Ops Center
# toggle rather than an edit and redeploy. Resolved MotherDuck -> AC_PAUSED env
# var -> default, and the default is True: if the config lookup fails we skip
# the push. A missed day of outreach is recoverable, an unintended push to real
# people is not. Read once here so a mid-run toggle can't split a single run's
# behaviour between eviction and push.
AC_PAUSED = automation_config.get_flag("ac_paused", default=True, env_var="AC_PAUSED")

# Smartlead no longer uses a day-based staleness guess. SEQUENCE_COMPLETED,
# EMAIL_REPLIED, EMAIL_BOUNCED, and LEAD_UNSUBSCRIBED webhooks (handled in
# ../smartlead_routes.py) flag contacts the instant the event happens, so this
# script only needs to top up open capacity. Note "flag", not "evict": those
# handlers clear active_cold_outreach_contact in Attio but leave the lead in
# the Smartlead campaign, which is why the pool is counted from Attio below.
# Target standing pool size = max_leads_per_day x sequence length, so the
# campaign always has roughly one full sequence's worth of leads loaded.
SMARTLEAD_SEQUENCE_LENGTH_DAYS = int(os.environ.get("SMARTLEAD_SEQUENCE_LENGTH_DAYS", "10"))

CHECKPOINT_TABLE = "outreach_rotation_log"


# ---------------------------------------------------------------------------
# Retry wrapper (matches your existing exponential backoff pattern)
# ---------------------------------------------------------------------------

def with_backoff(fn, *args, max_attempts=4, **kwargs):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code in (429, 500, 502, 503):
                raise requests.HTTPError(f"transient {resp.status_code}")
            resp.raise_for_status()
            return resp
        except (requests.HTTPError, requests.ConnectionError) as e:
            if attempt == max_attempts:
                raise
            sleep_s = (2 ** attempt) + random.uniform(0, 1)
            print(f"  retry {attempt}/{max_attempts} after error ({e}), sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# MotherDuck helpers (fetchdf() not available -- use description + fetchall)
# ---------------------------------------------------------------------------

def get_duck_conn():
    return duckdb.connect(f"md:{MOTHERDUCK_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")


def fetch_dicts(cursor):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def ensure_checkpoint_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            attio_record_id VARCHAR,
            channel VARCHAR,          -- 'activecampaign' or 'smartlead'
            action VARCHAR,           -- 'push' or 'evict'
            run_date DATE,
            created_at TIMESTAMP DEFAULT now()
        )
    """)


def already_processed_today(conn, attio_record_id, channel, action):
    cur = conn.execute(f"""
        SELECT 1 FROM {CHECKPOINT_TABLE}
        WHERE attio_record_id = ? AND channel = ? AND action = ? AND run_date = CURRENT_DATE
    """, [attio_record_id, channel, action])
    return cur.fetchone() is not None


def log_checkpoint(conn, rows):
    """rows: list of (attio_record_id, channel, action) tuples"""
    if DRY_RUN or not rows:
        return
    conn.executemany(f"""
        INSERT INTO {CHECKPOINT_TABLE} (attio_record_id, channel, action, run_date)
        VALUES (?, ?, ?, CURRENT_DATE)
    """, [(r[0], r[1], r[2]) for r in rows])


# ---------------------------------------------------------------------------
# Attio helpers
# ---------------------------------------------------------------------------

def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_query_people(filter_body, limit=500):
    """Two-phase collect: page through with a stable filter, return all IDs + key fields."""
    results = []
    offset = 0
    while True:
        body = {**filter_body, "limit": limit, "offset": offset}
        resp = with_backoff(
            requests.post,
            f"{ATTIO_BASE}/objects/people/records/query",
            headers=attio_headers(),
            json=body,
        )
        data = resp.json().get("data", [])
        if not data:
            break
        results.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return results


def attio_update_person(record_id, values, dry_run_label=""):
    if DRY_RUN:
        print(f"  [DRY RUN] would update {record_id}: {json.dumps(values)} {dry_run_label}")
        return
    with_backoff(
        requests.patch,
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": values}},
    )


def attio_add_to_list(list_id, record_id):
    """Add a person to an Attio list.

    A contact re-entering outreach after a previous eviction is normal, so an
    existing entry (409) is success. with_backoff raises on anything else --
    this call used to swallow every failure, so a list that quietly stopped
    accepting entries looked identical to a clean run.
    """
    if DRY_RUN:
        print(f"  [DRY RUN] would add {record_id} to list {list_id}")
        return
    resp = requests.post(
        f"{ATTIO_BASE}/lists/{list_id}/entries",
        headers=attio_headers(),
        json={"data": {"parent_record_id": record_id, "parent_object": "people"}},
        timeout=30,
    )
    if resp.status_code == 409:
        return
    if resp.status_code in (429, 500, 502, 503):
        with_backoff(
            requests.post,
            f"{ATTIO_BASE}/lists/{list_id}/entries",
            headers=attio_headers(),
            json={"data": {"parent_record_id": record_id, "parent_object": "people"}},
            timeout=30,
        )
        return
    resp.raise_for_status()


def attio_remove_from_list(list_id, entry_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove entry {entry_id} from list {list_id}")
        return
    with_backoff(
        requests.delete,
        f"{ATTIO_BASE}/lists/{list_id}/entries/{entry_id}",
        headers=attio_headers(),
    )


# ---------------------------------------------------------------------------
# ActiveCampaign helpers
# ---------------------------------------------------------------------------

def ac_headers():
    return {"Api-Token": AC_API_KEY, "Content-Type": "application/json"}


def ac_current_tag_count():
    resp = with_backoff(
        requests.get,
        f"{AC_BASE}/api/3/contacts",
        headers=ac_headers(),
        params={"tagid": AC_TAG_ID, "limit": 1},
    )
    return int(resp.json()["meta"]["total"])


def ac_add_contact(email, first_name, last_name):
    if DRY_RUN:
        print(f"  [DRY RUN] would upsert AC contact {email}")
        return "dry-run-id"
    resp = with_backoff(
        requests.post,
        f"{AC_BASE}/api/3/contact/sync",
        headers=ac_headers(),
        json={"contact": {"email": email, "firstName": first_name, "lastName": last_name}},
    )
    return resp.json()["contact"]["id"]

def ac_tag_and_list(contact_id):
    if DRY_RUN:
        return
    with_backoff(requests.post, f"{AC_BASE}/api/3/contactTags", headers=ac_headers(),
                 json={"contactTag": {"contact": contact_id, "tag": AC_TAG_ID}})
    with_backoff(requests.post, f"{AC_BASE}/api/3/contactLists", headers=ac_headers(),
                 json={"contactList": {"list": AC_LIST_ID, "contact": contact_id, "status": 1}})


def ac_find_contact_id_by_email(email):
    """Fallback for when the AC contact id is not mirrored on the Attio record
    (see AC_CONTACT_ID_SLUG). Returns None if AC does not know the address."""
    resp = with_backoff(
        requests.get,
        f"{AC_BASE}/api/3/contacts",
        headers=ac_headers(),
        params={"email": email, "limit": 1},
    )
    contacts = resp.json().get("contacts", [])
    return contacts[0]["id"] if contacts else None


def ac_remove_tag(contact_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove AC tag from contact {contact_id}")
        return
    with_backoff(requests.delete, f"{AC_BASE}/api/3/contactTags/{contact_id}", headers=ac_headers())


# ---------------------------------------------------------------------------
# Smartlead helpers
# ---------------------------------------------------------------------------

def smartlead_campaign_settings():
    resp = with_backoff(
        requests.get,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}",
        params={"api_key": SMARTLEAD_API_KEY},
    )
    return resp.json()


def smartlead_total_leads():
    """Cumulative leads on the campaign -- NOT the active pool.

    Smartlead documents `total_leads` as the count of leads matching the
    filter criteria, and no status filter is sent here, so it spans every
    status the campaign holds: STARTED, INPROGRESS, COMPLETED, PAUSED,
    STOPPED. Nothing in this repo ever removes a lead from the campaign --
    smartlead_routes.py handles SEQUENCE_COMPLETED / EMAIL_REPLIED /
    EMAIL_BOUNCED / LEAD_UNSUBSCRIBED by writing to Attio only -- so this
    number is monotonic and only goes up.

    It was previously read as the *active* count and subtracted from the
    target pool. That math inverts itself over time: once the campaign's
    lifetime total passes max_leads_per_day x sequence length, headroom pins
    to zero permanently and the campaign silently stops being topped up,
    exactly when the sending inboxes are emptiest. Kept as a diagnostic --
    see attio_active_cold_outreach_count() for the number to plan against.
    """
    resp = with_backoff(
        requests.get,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads",
        params={"api_key": SMARTLEAD_API_KEY, "limit": 1},
    )
    return int(resp.json().get("total_leads", 0))


def attio_active_cold_outreach_count():
    """Size of the live Smartlead pool, counted in Attio rather than Smartlead.

    Attio is this system's source of truth for channel membership, and
    `active_cold_outreach_contact` is the flag the Smartlead webhook handlers
    already clear the moment a lead completes, replies, bounces, or
    unsubscribes. That makes it the one place where "still in sequence"
    actually decays -- Smartlead's own total never does.

    Failure direction is deliberate: if the webhooks stop firing, stale True
    flags make this count read high, so the script under-pushes. The opposite
    error emails real people.
    """
    records = attio_query_people({
        "filter": {"active_cold_outreach_contact": {"$eq": True}}
    })
    return len(records)


def smartlead_add_leads(leads):
    """leads: list of dicts with email/first_name/last_name/company_name.
    Max 400 per request per Smartlead's API limit."""
    if DRY_RUN:
        print(f"  [DRY RUN] would push {len(leads)} leads to Smartlead campaign {SMARTLEAD_CAMPAIGN_ID}")
        return
    for i in range(0, len(leads), 400):
        chunk = leads[i:i + 400]
        with_backoff(
            requests.post,
            f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads",
            params={"api_key": SMARTLEAD_API_KEY},
            json={"lead_list": chunk},
        )


def smartlead_remove_lead(lead_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove Smartlead lead {lead_id} from campaign")
        return
    with_backoff(
        requests.post,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads/{lead_id}/remove",
        params={"api_key": SMARTLEAD_API_KEY},
    )


# ---------------------------------------------------------------------------
# Step 2 + 3: Eviction
# ---------------------------------------------------------------------------

def evict_stale_ac(conn):
    """Evict AC contacts whose last touch is older than AC_STALE_DAYS.
    Relies on `AC Touch Count` + `last_interaction` NOT being used for this --
    per your own rule, use MotherDuck activity_date, not Attio's native
    interaction field, for date-based staleness."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=AC_STALE_DAYS)).isoformat()
    filter_body = {
        "filter": {
            "$and": [
                {"active_marketing_contact": {"$eq": True}},
                {"prospect_path": {"$eq": "In Outreach"}},
                {"last_path_change_date": {"$lt": cutoff}},
            ]
        }
    }
    candidates = attio_query_people(filter_body)
    print(f"AC eviction candidates: {len(candidates)}")

    checkpoint_rows = []
    for rec in candidates:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "activecampaign", "evict"):
            continue
        ac_contact_id = None
        if AC_CONTACT_ID_SLUG:
            ac_contact_id = (rec["values"].get(AC_CONTACT_ID_SLUG) or [{}])[0].get("value")
        if not ac_contact_id:
            email = (rec["values"].get("email_addresses") or [{}])[0].get("email_address")
            ac_contact_id = ac_find_contact_id_by_email(email) if email else None
        if ac_contact_id:
            ac_remove_tag(ac_contact_id)
        else:
            print(f"  no AC contact found for {record_id}; clearing Attio flags only")
        attio_update_person(record_id, {
            "active_marketing_contact": [{"value": False}],
            "prospect_path": [{"value": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }, dry_run_label="(AC evict)")
        checkpoint_rows.append((record_id, "activecampaign", "evict"))

    log_checkpoint(conn, checkpoint_rows)
    return len(checkpoint_rows)


# Eviction is now handled entirely by the SEQUENCE_COMPLETED / EMAIL_REPLIED /
# EMAIL_BOUNCED / LEAD_UNSUBSCRIBED webhook handlers in ../smartlead_routes.py
# -- no day-based sweep needed here. This script's only Smartlead job is
# topping up the standing pool below.


# ---------------------------------------------------------------------------
# Step 4 + 5 + 6: Fill and push
# ---------------------------------------------------------------------------

def attio_list_entries(list_id, limit=500):
    """All entries in a list, as (entry_id, parent_record_id) pairs."""
    entries = []
    offset = 0
    while True:
        resp = with_backoff(
            requests.post,
            f"{ATTIO_BASE}/lists/{list_id}/entries/query",
            headers=attio_headers(),
            json={"limit": limit, "offset": offset},
        )
        page = resp.json().get("data", [])
        for e in page:
            entries.append((e["id"]["entry_id"], e.get("parent_record_id")))
        if len(page) < limit:
            break
        offset += limit
    return entries


def attio_get_person(record_id):
    resp = with_backoff(
        requests.get,
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
    )
    return resp.json()["data"]


def _first_value(rec, field, key="value"):
    return (rec["values"].get(field) or [{}])[0].get(key)


def smartlead_intake_batch(size):
    """Pull up to `size` people off the hand-curated "Add to Smartlead" list.

    Returns (records, entry_by_record_id) so the caller can drop the list entry
    once the push actually succeeds -- draining the queue is the whole point of
    an intake list, but only for people who really went out.

    A human putting someone here is an instruction, not a suggestion, so this
    runs ahead of the Never Contacted pool. It is still not a blank cheque:
    anyone marked do_not_migrate or without an email is skipped and *left on
    the list*, because those are the two cases a person needs to look at, and
    silently dropping them is how a request disappears with nobody noticing.

    Someone already in cold outreach has had their request fulfilled, so their
    entry is dropped here rather than left. Otherwise a push that succeeded but
    failed before the drain below would strand the entry permanently -- the
    flag it sets is exactly what makes this branch skip it next run.

    The whole scan runs even once `size` is reached, so those two kinds of
    housekeeping don't depend on how much headroom a given day happens to have.
    """
    if not ATTIO_SMARTLEAD_INTAKE_LIST_ID or size <= 0:
        return [], {}

    records = []
    entry_by_record = {}
    for entry_id, record_id in attio_list_entries(ATTIO_SMARTLEAD_INTAKE_LIST_ID):
        if not record_id:
            continue
        rec = attio_get_person(record_id)

        if _first_value(rec, "active_cold_outreach_contact") is True:
            print(f"  intake: {record_id} already in cold outreach, dropping stale entry")
            attio_remove_from_list(ATTIO_SMARTLEAD_INTAKE_LIST_ID, entry_id)
            continue
        if _first_value(rec, "do_not_migrate") is True:
            print(f"  intake: {record_id} is do_not_migrate, leaving on list for review")
            continue
        if not _first_value(rec, "email_addresses", key="email_address"):
            print(f"  intake: {record_id} has no email, leaving on list for review")
            continue

        if len(records) >= size:
            continue  # No headroom today; leave queued for the next run.

        records.append(rec)
        entry_by_record[record_id] = entry_id

    return records, entry_by_record


def never_contacted_batch(size, exclude_flag_field):
    """Pull `size` records from the Never Contacted pool, excluding anyone
    already flagged for this channel. Uses a stable created_at sort so
    pagination/offset doesn't skip records if the pool shrinks mid-run."""
    filter_body = {
        "filter": {
            "$and": [
                {"prospect_path": {"$eq": "Never Contacted"}},
                {exclude_flag_field: {"$eq": False}},
            ]
        },
        "sorts": [{"attribute": "created_at", "direction": "asc"}],
    }
    all_ids = attio_query_people(filter_body, limit=min(size, 500))
    return all_ids[:size]


def push_ac_batch(conn, headroom):
    if headroom <= 0:
        print("AC at capacity, nothing to push.")
        return
    batch = never_contacted_batch(headroom, "active_marketing_contact")
    print(f"Pushing {len(batch)} contacts to AC (headroom {headroom}).")

    checkpoint_rows = []
    for rec in batch:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "activecampaign", "push"):
            continue
        email = rec["values"].get("email_addresses", [{}])[0].get("email_address")
        first = rec["values"].get("first_name", [{}])[0].get("value", "")
        last = rec["values"].get("last_name", [{}])[0].get("value", "")
        if not email:
            continue

        ac_contact_id = ac_add_contact(email, first, last)
        ac_tag_and_list(ac_contact_id)

        values = {
            "active_marketing_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }
        if AC_CONTACT_ID_SLUG:
            values[AC_CONTACT_ID_SLUG] = [{"value": str(ac_contact_id)}]
        attio_update_person(record_id, values, dry_run_label="(AC push)")
        attio_add_to_list(ATTIO_AC_LIST_ID, record_id)

        checkpoint_rows.append((record_id, "activecampaign", "push"))

    log_checkpoint(conn, checkpoint_rows)
    print(f"AC push complete: {len(checkpoint_rows)} pushed.")


def push_smartlead_batch(conn, headroom):
    if headroom <= 0:
        print("Smartlead at capacity, nothing to push.")
        return
    # Hand-picked first, pool second. Someone was deliberately put on the
    # intake list; the Never Contacted pool is just whoever sorts earliest.
    intake, intake_entries = smartlead_intake_batch(headroom)
    pool = never_contacted_batch(headroom - len(intake), "active_cold_outreach_contact")

    # A person can sit on the intake list *and* match the Never Contacted
    # filter, which would otherwise queue them twice in one payload.
    intake_ids = {r["id"]["record_id"] for r in intake}
    pool = [r for r in pool if r["id"]["record_id"] not in intake_ids]

    batch = intake + pool
    print(f"Pushing {len(batch)} contacts to Smartlead (headroom {headroom}): "
          f"{len(intake)} from the intake list, {len(pool)} from Never Contacted.")

    leads_payload = []
    record_map = {}
    for rec in batch:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "smartlead", "push"):
            continue
        email = rec["values"].get("email_addresses", [{}])[0].get("email_address")
        if not email:
            continue
        leads_payload.append({
            "email": email,
            "first_name": rec["values"].get("first_name", [{}])[0].get("value", ""),
            "last_name": rec["values"].get("last_name", [{}])[0].get("value", ""),
            "custom_fields": {"attio_record_id": record_id},
        })
        record_map[email] = record_id

    if not leads_payload:
        print("Nothing new to push to Smartlead.")
        return

    smartlead_add_leads(leads_payload)

    checkpoint_rows = []
    drained = 0
    for email, record_id in record_map.items():
        attio_update_person(record_id, {
            "active_cold_outreach_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }, dry_run_label="(Smartlead push)")

        # Drain the intake entry only now, after the lead is in Smartlead and
        # the Attio flag is set. Removing it earlier would lose the request if
        # the push failed -- and the list is the only record that a human ever
        # asked for this person.
        entry_id = intake_entries.get(record_id)
        if entry_id:
            attio_remove_from_list(ATTIO_SMARTLEAD_INTAKE_LIST_ID, entry_id)
            drained += 1

        checkpoint_rows.append((record_id, "smartlead", "push"))

    log_checkpoint(conn, checkpoint_rows)
    print(f"Smartlead push complete: {len(checkpoint_rows)} pushed, "
          f"{drained} removed from the intake list.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"=== Outreach rotation run: {datetime.now(timezone.utc).isoformat()} (DRY_RUN={DRY_RUN}) ===")

    conn = get_duck_conn()
    ensure_checkpoint_table(conn)

    # --- AC: pause state comes from automation_config (Ops Center toggle ->
    # MotherDuck, falling back to the AC_PAUSED env var). While paused, AC is
    # working the contacts already loaded via its own automations and the
    # tag<->list webhook sync stays live; this script just leaves AC alone. ---
    if not AC_PAUSED:
        # Evict first so the freed capacity is counted in the headroom below.
        evicted = evict_stale_ac(conn)
        print(f"AC evicted {evicted} stale contacts (>{AC_STALE_DAYS} days).")
        ac_count = ac_current_tag_count()
        print(f"AC current active marketing contacts: {ac_count} / {AC_MAX_CONTACTS}")
        ac_headroom = AC_MAX_CONTACTS - ac_count
        push_ac_batch(conn, ac_headroom)
    else:
        print("AC push/evict is paused (automation_config flag 'ac_paused') -- skipping.")

    # --- Smartlead: event-driven eviction (see webhook handler), this
    # script only tops up the standing pool to keep inboxes fed. ---
    settings = smartlead_campaign_settings()
    max_per_day = int(settings.get("max_leads_per_day", 0))
    if max_per_day == 0:
        print("WARNING: could not read max_leads_per_day from Smartlead; skipping push.")
    else:
        target_pool = max_per_day * SMARTLEAD_SEQUENCE_LENGTH_DAYS
        current_active = attio_active_cold_outreach_count()
        sl_headroom = max(0, target_pool - current_active)
        # Both numbers get printed because their gap is the useful diagnostic:
        # lifetime should sit well above active, and the two converging means
        # the webhook handlers have stopped clearing the flag.
        lifetime = smartlead_total_leads()
        print(f"Smartlead max/day: {max_per_day}, target pool: {target_pool}, "
              f"active pool (Attio): {current_active}, pushing: {sl_headroom} "
              f"[campaign lifetime total: {lifetime}]")
        push_smartlead_batch(conn, sl_headroom)

    print("=== Run complete ===")


if __name__ == "__main__":
    main()
