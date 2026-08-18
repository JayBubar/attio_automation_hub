"""
ops_center_routes.py

Trigger, status, and task routes consumed by the RaiseTell Ops Center
(the Streamlit service in this same Railway project). Registered in main.py
via app.include_router(ops_center_router).

Auth: every route requires `Authorization: Bearer <OPS_CENTER_TOKEN>`.
If OPS_CENTER_TOKEN is unset the routes refuse to serve at all rather than
accepting an empty token -- see _check_auth.

Requires, on top of what the webhook routes already need:
  OPS_CENTER_TOKEN   shared secret, also set on the Streamlit service
  ANTHROPIC_API_KEY  used by scripts/outreach.py for email drafting
  SMARTLEAD_API_KEY  used by /status/smartlead
  SMARTLEAD_CAMPAIGN_ID
"""

import os
import re
import secrets
import socket
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import automation_config
import call_outcomes
import duckdb
import ipv4_only
import requests
import webhook_log
from fastapi import APIRouter, Header, HTTPException

router = APIRouter()

# Flags the Ops Center may toggle, and the default each resolves to when
# neither MotherDuck nor the environment has an opinion. Allow-listed rather
# than free-form: this endpoint writes config the daily jobs obey, so an
# arbitrary key from the caller has no business creating a row.
TOGGLEABLE_FLAGS = {
    "ac_paused": {
        "default": True,
        "env_var": "AC_PAUSED",
        "label": "ActiveCampaign rotation paused",
        "description": "When on, outreach_rotation.py does not push to or evict from AC.",
        "live_warning": (
            "AC rotation is live: the next run will push new contacts into "
            "ActiveCampaign and evict stale ones. The standing decision is to "
            "leave this paused."
        ),
    },
    "smartlead_paused": {
        # Defaults paused for the same reason as ac_paused, and one of its own:
        # the Smartlead half of main() used to have no gate at all, so before
        # this flag existed the only thing standing between a scheduled run and
        # real email was DRY_RUN. Starting paused makes resuming a decision
        # somebody makes on purpose rather than a default nobody chose.
        "default": True,
        "env_var": "SMARTLEAD_PAUSED",
        "label": "Smartlead rotation paused",
        "description": (
            "When on, outreach_rotation.py does not top up the Smartlead pool "
            "or drain the 'Add to Smartlead' intake queue."
        ),
        "live_warning": (
            "Smartlead rotation is live: the next run will send real email. It "
            "drains the hand-curated 'Add to Smartlead' queue first, so check "
            "what's sitting on that list before resuming."
        ),
    },
}

OPS_CENTER_TOKEN = os.environ.get("OPS_CENTER_TOKEN", "")
ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")

SMARTLEAD_API_KEY = os.environ.get("SMARTLEAD_API_KEY", "")
SMARTLEAD_CAMPAIGN_ID = os.environ.get("SMARTLEAD_CAMPAIGN_ID", "")
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"

# The AC bridge's receiver, as mounted by activecampaign_routes.py. Kept here
# as a constant so /status/ac-bridge checks the path that is actually served
# rather than a second copy of the string that can drift out of sync.
AC_WEBHOOK_PATH = "/webhooks/activecampaign"

# Attio web app base for deep links. Workspace slug confirmed via whoami
# (workspace_name "RaiseTell"); override if the URL slug ever differs.
ATTIO_APP_BASE = os.environ.get(
    "ATTIO_APP_BASE", "https://app.attio.com/raisetell"
)

# Verified against the live workspace, not guessed.
SNITCHER_REVIEW_LIST_ID = "6d4dce27-180c-42ed-b722-876f51e7c184"
SNITCHER_STATUS_NEW = "New"

REP_MEMBER_IDS = {
    "kurt": "928e9d43-504b-4e51-8db2-54c4c40d0ecf",  # Kurt Haas
    "jay": "acc65c82-459c-46a7-bd87-da84d6c4fcd5",   # Jay Bubar
    "joel": "ca4b7dfe-0f51-4631-89df-8b2d8583cd8d",  # Joel Weinbach
}

# scripts/ sits next to this file inside the deployed root directory
# (Root Directory = automation_server), so resolve from __file__ rather than
# relying on the process working directory.
OUTREACH_SCRIPT = Path(__file__).resolve().parent / "scripts" / "outreach.py"

# Must stay below the Ops Center's own HTTP timeout, so the caller sees a
# real response rather than giving up while this keeps mutating Attio.
OUTREACH_TIMEOUT_SECONDS = 600

# Parallel Attio reads when building queue context. Every one is
# network-blocked, so this is throughput, not CPU. Matches the pool size
# scripts/outreach.py already uses for its company lookups.
CONTEXT_WORKERS = 10


def _check_auth(authorization: str | None):
    if not OPS_CENTER_TOKEN:
        # Fail closed. Without this, an unset env var makes the empty token
        # valid and every trigger route becomes publicly callable.
        raise HTTPException(status_code=503, detail="OPS_CENTER_TOKEN not configured")
    expected = f"Bearer {OPS_CENTER_TOKEN}"
    if not authorization or not secrets.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _check_rep(rep: str):
    if rep not in REP_MEMBER_IDS:
        raise HTTPException(status_code=400, detail="rep must be kurt, jay, or joel")


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def md_connection():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")


_SECRET_PATTERNS = re.compile(
    r"(sk-ant-[\w\-]+|md_[\w\-]{16,}|Bearer\s+[\w\-\.=]+)", re.IGNORECASE
)


def redact_secrets(text: str) -> str:
    """Scrub credentials out of anything echoed back over HTTP.

    A malformed API key produces an exception whose message quotes the key
    verbatim -- so the error path is exactly where a secret escapes, and
    "it's behind auth" is not a good enough reason to let it.
    """
    return _SECRET_PATTERNS.sub("[REDACTED]", text or "")


def rows_to_dicts(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

@router.post("/trigger/outreach-batch")
def trigger_outreach_batch(
    rep: str,
    batch_size: int = 25,
    dry_run: bool = False,
    authorization: str | None = Header(None),
):
    """Runs scripts/outreach.py synchronously and returns its output.

    This blocks for as long as the batch takes -- roughly 20-30s per contact
    (one Claude call plus four Attio task creates each). FastAPI runs sync
    routes in a threadpool, so this does not stall the webhook receivers,
    but the caller must be prepared to wait.

    The script checkpoints each contact to MotherDuck as it finishes, so a
    timeout here does not cause the next run to re-email anyone already
    processed.
    """
    _check_auth(authorization)
    _check_rep(rep)
    if not 1 <= batch_size <= 100:
        raise HTTPException(status_code=400, detail="batch_size must be between 1 and 100")

    cmd = [sys.executable, str(OUTREACH_SCRIPT), "--rep", rep, "--batch-size", str(batch_size)]
    if dry_run:
        cmd.append("--dry-run")

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=OUTREACH_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as e:
        # Partial work is already checkpointed; surface what we got rather
        # than a bare 500 with no output.
        stdout = (e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        return {
            "ok": False,
            "timed_out": True,
            "rep": rep,
            "dry_run": dry_run,
            "detail": (
                f"Batch exceeded {OUTREACH_TIMEOUT_SECONDS}s and was killed. Contacts "
                "completed before the timeout are checkpointed and will not be "
                "re-selected. Re-run to continue with the remainder."
            ),
            # The script prints tracebacks on failure and those can quote a
            # malformed credential verbatim -- scrub before returning.
            "stdout_tail": redact_secrets(stdout[-3000:]),
        }

    return {
        "ok": result.returncode == 0,
        "timed_out": False,
        "rep": rep,
        "dry_run": dry_run,
        "stdout_tail": redact_secrets(result.stdout[-3000:]),
        "stderr_tail": redact_secrets(result.stderr[-2000:]) if result.returncode != 0 else None,
    }


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@router.get("/tasks/{rep}")
def get_rep_tasks(
    rep: str,
    filter: str = "all",
    type: str = "all",
    limit: int = 50,
    authorization: str | None = Header(None),
):
    """Open Attio tasks for one rep, with the context the queue renders.

    Context is embedded per task rather than fetched as the queue advances. The
    whole point of queue mode is that the next card is already there; a
    round-trip per advance would put the wait back exactly where it was taken
    from. The cost is a few extra Attio calls per task at load time, paid once.

    The AI-draft join is gone -- `outreach_email_drafts` is out of scope for
    this redesign, and keeping it meant every load paid a MotherDuck query for
    a field nothing rendered.

    ## `filter` is mostly aspirational right now

    `deadline_at` is null on every task in the workspace (30 sampled across
    three pages, 2026-08-17), so today/overdue/upcoming each match nothing and
    `all` matches everything. Rather than silently serving an empty queue,
    undated tasks report `due_bucket: "none"` and the response carries
    `bucket_counts`, so the UI can say *why* a filter is empty instead of
    looking broken. The filters start working the moment tasks carry deadlines.
    """
    _check_auth(authorization)
    _check_rep(rep)
    member_id = REP_MEMBER_IDS[rep]

    tasks = []
    offset = 0
    limit = 500
    while True:
        resp = requests.get(
            f"{ATTIO_BASE}/tasks",
            headers=attio_headers(),
            params={"limit": limit, "offset": offset, "is_completed": "false",
                    "sort": "created_at:asc"},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json().get("data", [])
        tasks.extend(page)
        if len(page) < limit:
            break
        offset += limit

    def assigned_to_rep(task):
        return any(
            a.get("referenced_actor_id") == member_id
            for a in (task.get("assignees") or [])
        )

    mine = [t for t in tasks if assigned_to_rep(t)]

    shaped = []
    for t in mine:
        content = t.get("content_plaintext") or t.get("content") or ""
        person_id = _linked_person_id(t.get("linked_records") or [])
        shaped.append({
            "task_id": t["id"]["task_id"],
            "content": content,
            "deadline_at": t.get("deadline_at"),
            "due_bucket": _due_bucket(t.get("deadline_at")),
            "task_type": _task_type(content),
            "company_name": company_from_content(content),
            "linked_record_id": person_id,
        })

    bucket_counts = {}
    type_counts = {}
    for s in shaped:
        bucket_counts[s["due_bucket"]] = bucket_counts.get(s["due_bucket"], 0) + 1
        type_counts[s["task_type"]] = type_counts.get(s["task_type"], 0) + 1

    selected = [
        s for s in shaped
        if (filter == "all" or s["due_bucket"] == filter)
        and (type == "all" or s["task_type"] == type)
    ]

    # Undated tasks have no meaningful due order, so fall back to the order the
    # cadence created them in.
    selected.sort(key=lambda s: (s["deadline_at"] or "9999", s["content"]))

    matched = len(selected)
    # Bounded: context is 3-4 Attio calls per task, so an unbounded queue is an
    # unbounded page load. Run one session's worth; the rep re-queues for more.
    selected = selected[:max(1, limit)]

    # Context in parallel, not in sequence. Serially this was 53-88s for one
    # rep and growing with the task count -- past Streamlit's 120s timeout,
    # which is what "the page loads nothing" actually was. Threads are the
    # right tool here because every one of these is network-blocked, not
    # CPU-bound.
    company_cache = {}
    with ThreadPoolExecutor(max_workers=CONTEXT_WORKERS) as pool:
        contexts = list(pool.map(
            lambda t: _task_context(t["linked_record_id"], company_cache), selected))
    for task, ctx in zip(selected, contexts):
        task["context"] = ctx

    return {
        "rep": rep,
        "filter": filter,
        "type": type,
        "total_open": len(shaped),
        "matched": matched,
        "returned": len(selected),
        "truncated": matched > len(selected),
        "limit": limit,
        "bucket_counts": bucket_counts,
        "type_counts": type_counts,
        "tasks": selected,
    }


def company_from_content(content: str):
    """Cadence tasks are titled "Email 1/3 . {company}: {contact}". Anything
    else is a hand-written task with no company to parse -- return None rather
    than echoing the whole title back as a company name."""
    if "·" not in (content or ""):
        return None
    return content.split("·", 1)[1].split(":", 1)[0].strip() or None


# People object id, for matching task links that identify the object by UUID
# rather than slug.
PEOPLE_OBJECT_ID = "70e3d90c-7685-4058-9a6b-449c4fe5705c"


def _linked_person_id(linked):
    """Person record id off a task's links, or None.

    Deliberately permissive about the key and the value. Attio documents the
    field as `target_object_id`, the earlier code here read `target_object`
    (which is never present, so this returned None for *every* task and the
    whole context panel came back empty), and the value shows up as the object
    slug in some responses and the object UUID in others. Accepting all of
    those costs nothing; guessing one and being wrong silently empties the
    panel, which is exactly what happened.

    Company-linked tasks still return None on purpose -- a company is not a
    person, and the context panel is person-shaped.
    """
    for link in linked or []:
        obj = (link.get("target_object_id") or link.get("target_object")
               or link.get("object_id"))
        if obj in ("people", PEOPLE_OBJECT_ID):
            return link.get("target_record_id") or link.get("record_id")
    return None


def _due_bucket(deadline):
    """today / overdue / upcoming / none. `none` is the honest answer for a
    task with no deadline, and is currently every task in the workspace."""
    if not deadline:
        return "none"
    try:
        cleaned = re.sub(r"\.(\d{6})\d*", r".\1", str(deadline).replace("Z", "+00:00"))
        due = datetime.fromisoformat(cleaned).date()
    except ValueError:
        return "none"
    today = datetime.now(timezone.utc).date()
    if due < today:
        return "overdue"
    return "today" if due == today else "upcoming"


def _task_type(content):
    """Derived from the task title, because Attio tasks have no type field.

    The cadence writes "Call 2/3 - Company: Person" and "Email 1/3 - ...", so
    the leading word is the signal. Anything else is hand-written and lands in
    `followup`, the type whose UI assumes the least.
    """
    head = (content or "").strip().lower()
    if head.startswith("call"):
        return "call"
    if head.startswith("email"):
        return "email"
    return "followup"


def _task_context(person_record_id, company_cache=None):
    """Notes, emails, path, company and phone for one card.

    Never raises. A context panel that fails must not take the task with it, so
    each piece degrades independently and the queue keeps running with whatever
    did load -- `errors` says what didn't.
    """
    ctx = {
        "available": bool(person_record_id),
        "person_name": None, "email": None, "phone": None,
        "prospect_path": None, "company_name": None,
        "notes": [], "emails": [], "attio_url": None,
        "errors": [],
    }
    if not person_record_id:
        ctx["errors"].append("Task has no linked person record.")
        return ctx

    ctx["attio_url"] = f"{ATTIO_APP_BASE}/person/{person_record_id}"

    try:
        r = requests.get(
            f"{ATTIO_BASE}/objects/people/records/{person_record_id}",
            headers=attio_headers(), timeout=20,
        )
        r.raise_for_status()
        v = r.json()["data"]["values"]
        ctx["person_name"] = _first(v, "name", "full_name")
        ctx["email"] = _first(v, "email_addresses", "email_address")
        ctx["phone"] = (_first(v, "cell_phone", "original_phone_number")
                        or _first(v, "phone_numbers", "original_phone_number"))
        ctx["prospect_path"] = _first(v, "prospect_path", "status")
        company = (v.get("company") or [{}])[0].get("target_record_id")
        if company:
            # Cached across the queue: a cadence batch is many contacts at few
            # companies, so this is the most repeated call of the three.
            if company_cache is not None and company in company_cache:
                ctx["company_name"] = company_cache[company]
            else:
                ctx["company_name"] = _company_name(company)
                if company_cache is not None:
                    company_cache[company] = ctx["company_name"]
    except requests.RequestException as e:
        ctx["errors"].append(f"record: {e}")

    try:
        r = requests.get(
            f"{ATTIO_BASE}/notes", headers=attio_headers(),
            params={"parent_object": "people", "parent_record_id": person_record_id,
                    "limit": 3},
            timeout=20,
        )
        r.raise_for_status()
        for n in r.json().get("data", [])[:3]:
            body = (n.get("content_plaintext") or "").strip()
            ctx["notes"].append({
                "title": n.get("title"),
                "created_at": n.get("created_at"),
                "preview": body[:400] + ("..." if len(body) > 400 else ""),
            })
    except requests.RequestException as e:
        ctx["errors"].append(f"notes: {e}")

    try:
        r = requests.get(
            f"{ATTIO_BASE}/emails", headers=attio_headers(),
            params={"linked_object": "people",
                    "linked_record_ids": person_record_id, "limit": 2},
            timeout=20,
        )
        r.raise_for_status()
        for item in r.json().get("data", [])[:2]:
            ctx["emails"].append({
                # No preview available at any price: Attio's email API returns
                # metadata only and states outright that content is never
                # returned. Subject, direction and date are the whole of it.
                "subject": item.get("subject_line"),
                "direction": item.get("direction"),
                "sent_at": item.get("sent_at"),
            })
    except requests.RequestException as e:
        ctx["errors"].append(f"emails: {e}")

    return ctx


def _first(values, slug, key):
    """One value off an Attio attribute, tolerant of the per-type wrapper.

    Status and select nest a whole object under their key
    (`{"status": {"title": "In Outreach", ...}}`), so the dict case is
    unwrapped *before* the direct hit -- returning the wrapper would make
    `prospect_path == "Client"` never true, and Clients would silently be
    offered the prospecting outcome list.
    """
    entry = (values.get(slug) or [{}])[0]
    value = entry.get(key)
    if isinstance(value, dict):
        return value.get("title")
    if value is not None:
        return value
    inner = entry.get("status") or entry.get("option")
    if isinstance(inner, dict):
        return inner.get("title")
    return entry.get("value")


def _company_name(company_record_id):
    try:
        r = requests.get(
            f"{ATTIO_BASE}/objects/companies/records/{company_record_id}",
            headers=attio_headers(), timeout=20,
        )
        r.raise_for_status()
        return _first(r.json()["data"]["values"], "name", "value")
    except requests.RequestException:
        return None


@router.post("/tasks/{task_id}/log-call-outcome")
def log_call_outcome(
    task_id: str,
    record_id: str,
    outcome: str,
    note: str | None = None,
    complete: bool = True,
    authorization: str | None = Header(None),
):
    """Apply a call outcome, then optionally complete the task.

    Delegates to call_outcomes.apply_outcome, which resolves the mapping from
    `allo_tag_registry` -- the same table the Allo webhook reads. That is what
    keeps "what does Left a VM actually do" defined in one place, whether it
    was triggered by Allo's own call system or by a rep working the queue.

    The task is completed only if the outcome actually applied. Completing a
    task whose outcome write failed would bury the failure and lose the call.
    """
    _check_auth(authorization)

    result = call_outcomes.apply_outcome(
        record_id, outcome, note=note, source="task-runner")
    if not result.get("ok"):
        return {"ok": False, "task_completed": False, **result}

    completed = False
    if complete:
        r = requests.patch(
            f"{ATTIO_BASE}/tasks/{task_id}", headers=attio_headers(),
            json={"data": {"is_completed": True}}, timeout=30,
        )
        completed = r.status_code < 400
        if not completed:
            result["complete_error"] = redact_secrets(r.text[:300])

    return {"ok": True, "task_completed": completed, **result}


@router.get("/call-outcomes")
def list_call_outcomes(
    prospect_path: str | None = None,
    authorization: str | None = Header(None),
):
    """Outcome options for a contact on this path -- prospecting by default,
    the maintenance subset for Clients. Served from the registry so the UI
    never carries its own copy of the list."""
    _check_auth(authorization)
    return {"prospect_path": prospect_path,
            "options": call_outcomes.outcome_options(prospect_path)}

@router.post("/tasks/{task_id}/draft-email")
def draft_task_email(
    task_id: str,
    rep: str,
    subject: str | None = None,
    body: str | None = None,
    authorization: str | None = Header(None),
):
    """Creates an Outlook draft in the rep's own mailbox for this task.

    Uses the edited subject/body if the UI passes them, otherwise the stored
    MotherDuck draft. Recipient comes from the person record linked to the
    task. Creates a draft only -- the rep presses Send in Outlook, and
    Attio's native mail-sync logs it on the contact record from there.
    """
    _check_auth(authorization)
    _check_rep(rep)

    if subject is None or body is None:
        con = md_connection()
        try:
            cur = con.execute(
                f"""SELECT subject, body, contact_record_id
                    FROM {MOTHERDUCK_DB}.main.outreach_email_drafts WHERE task_id = ?""",
                [task_id],
            )
            rows = rows_to_dicts(cur)
        finally:
            con.close()
        if not rows:
            raise HTTPException(status_code=404, detail="No stored draft for this task")
        subject = subject if subject is not None else rows[0]["subject"]
        body = body if body is not None else rows[0]["body"]
        contact_record_id = rows[0]["contact_record_id"]
    else:
        t = requests.get(f"{ATTIO_BASE}/tasks/{task_id}", headers=attio_headers(), timeout=30)
        t.raise_for_status()
        linked = t.json()["data"].get("linked_records") or []
        if not linked:
            raise HTTPException(status_code=400, detail="Task has no linked person record")
        contact_record_id = linked[0].get("target_record_id")

    p = requests.get(
        f"{ATTIO_BASE}/objects/people/records/{contact_record_id}",
        headers=attio_headers(), timeout=30,
    )
    p.raise_for_status()
    emails = p.json()["data"]["values"].get("email_addresses") or []
    if not emails:
        raise HTTPException(status_code=400, detail="Linked contact has no email address")
    to_address = emails[0]["email_address"]

    from graph_mail import create_outlook_draft
    return create_outlook_draft(rep, to_address, subject, body)


@router.patch("/tasks/{task_id}/complete")
def complete_task(task_id: str, authorization: str | None = Header(None)):
    _check_auth(authorization)
    resp = requests.patch(
        f"{ATTIO_BASE}/tasks/{task_id}",
        headers=attio_headers(),
        json={"data": {"is_completed": True}},
        timeout=30,
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text[:500])
    return {"ok": True, "task_id": task_id, "is_completed": True}


# ---------------------------------------------------------------------------
# Status (read-only)
# ---------------------------------------------------------------------------

@router.get("/status/smartlead")
def status_smartlead(authorization: str | None = Header(None)):
    _check_auth(authorization)
    if not SMARTLEAD_API_KEY or not SMARTLEAD_CAMPAIGN_ID:
        raise HTTPException(
            status_code=503,
            detail="SMARTLEAD_API_KEY / SMARTLEAD_CAMPAIGN_ID not configured on this service",
        )

    out = {"campaign_id": SMARTLEAD_CAMPAIGN_ID}

    try:
        r = requests.get(
            f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}",
            params={"api_key": SMARTLEAD_API_KEY}, timeout=20,
        )
        r.raise_for_status()
        c = r.json()
        out["campaign_name"] = c.get("name")
        out["campaign_status"] = c.get("status")
        out["max_leads_per_day"] = c.get("max_leads_per_day")
    except requests.exceptions.RequestException as e:
        out["campaign_error"] = str(e)

    try:
        r = requests.get(
            f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads",
            params={"api_key": SMARTLEAD_API_KEY, "offset": 0, "limit": 1}, timeout=20,
        )
        r.raise_for_status()
        out["total_leads"] = r.json().get("total_leads")
    except requests.exceptions.RequestException as e:
        out["leads_error"] = str(e)

    # Last rotation run. outreach_rotation.py's docstring says it logs to
    # outreach_rotation_log, but that table does not exist in MotherDuck yet
    # -- the script has evidently not run against it. Report that honestly
    # rather than returning a misleading null.
    con = md_connection()
    try:
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'outreach_rotation_log'"
        ).fetchone()[0]
        if exists:
            cur = con.execute(
                f"""SELECT max(pushed_at) AS last_run, count(*) AS total_rows
                    FROM {MOTHERDUCK_DB}.main.outreach_rotation_log"""
            )
            out["rotation"] = rows_to_dicts(cur)[0]
        else:
            out["rotation"] = {
                "last_run": None,
                "note": "outreach_rotation_log table does not exist yet -- "
                        "outreach_rotation.py has not logged a run.",
            }
    finally:
        con.close()

    return out


@router.get("/status/anthropic")
def status_anthropic(authorization: str | None = Header(None)):
    """Can this container actually reach the Claude API?

    Exists because a batch failing at the drafting step reports a bare
    "Connection error." from deep inside the SDK, and a --dry-run never
    calls Claude at all -- so there was no way to answer this question
    short of burning a real batch or reading Railway's DNS logs. Sends a
    16-token request; safe to hit any time.
    """
    _check_auth(authorization)

    # ipv4_only replaced socket.getaddrinfo, so asking it for AF_INET6 would
    # just hand back the A record and make this probe lie. Ask the real
    # resolver what DNS actually publishes.
    real_getaddrinfo = getattr(ipv4_only, "_original_getaddrinfo", socket.getaddrinfo)
    resolved = {}
    for fam, label in ((socket.AF_INET, "A"), (socket.AF_INET6, "AAAA")):
        try:
            resolved[label] = real_getaddrinfo("api.anthropic.com", 443, fam)[0][4][0]
        except OSError:
            resolved[label] = None

    raw_key = os.environ.get("ANTHROPIC_API_KEY", "")
    result = {
        "api_key_present": bool(raw_key),
        # Not the key itself. A key that fails to send is usually a key that
        # was pasted with quotes, an inline comment, or a stray newline, and
        # none of that is visible from "present: true".
        "api_key_looks_clean": bool(
            raw_key and raw_key == raw_key.strip()
            and raw_key.startswith("sk-ant-")
            and not any(c in raw_key for c in " '\"#\n\r\t")
        ),
        "api_key_length": len(raw_key),
        "dns_publishes": resolved,
        "ipv6_egress_suppressed_in_process": True,
    }
    if not result["api_key_present"]:
        result.update(ok=False, detail="ANTHROPIC_API_KEY is not set on this service")
        return result

    try:
        import anthropic
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model="claude-sonnet-5", max_tokens=16,
            messages=[{"role": "user", "content": "Reply with exactly: PONG"}],
        )
        result.update(ok=True, model="claude-sonnet-5",
                      reply="".join(b.text for b in resp.content if getattr(b, "type", None) == "text").strip())
    except Exception as e:
        cause = e.__cause__ or e.__context__
        result.update(
            ok=False,
            error_type=type(e).__name__,
            detail=redact_secrets(str(e)),
            caused_by=redact_secrets(f"{type(cause).__name__}: {cause}") if cause is not None else None,
        )
    return result


@router.get("/status/snitcher-review")
def status_snitcher_review(authorization: str | None = Header(None)):
    """Count of Snitcher Review entries still sitting at Status = New.
    Note the list's parent object is companies, not people."""
    _check_auth(authorization)

    new_count = 0
    total = 0
    offset = 0
    limit = 500
    while True:
        resp = requests.post(
            f"{ATTIO_BASE}/lists/{SNITCHER_REVIEW_LIST_ID}/entries/query",
            headers=attio_headers(),
            json={"limit": limit, "offset": offset},
            timeout=30,
        )
        resp.raise_for_status()
        page = resp.json().get("data", [])
        for entry in page:
            total += 1
            status_vals = (entry.get("entry_values") or {}).get("status") or []
            if status_vals:
                title = (status_vals[0].get("status") or status_vals[0].get("option") or {}).get("title")
                if title == SNITCHER_STATUS_NEW:
                    new_count += 1
        if len(page) < limit:
            break
        offset += limit

    return {
        "list": "Snitcher Review",
        "list_id": SNITCHER_REVIEW_LIST_ID,
        "parent_object": "companies",
        "status_new": new_count,
        "total_entries": total,
    }


@router.get("/status/ac-bridge")
def status_ac_bridge(authorization: str | None = Header(None)):
    """Real state of the AC <-> Attio tag sync.

    Replaces the Ops Center's hardcoded "Down", which probed
    peaceful-generosity-production-312b.up.railway.app -- a host that 404s and
    that the bridge never ran on. The bridge is a route in *this* service, so
    it was reporting Down while working fine.

    Two independent facts, deliberately not collapsed into one green light:

    `route_registered` -- is the receiver actually mounted? Answered by asking
    this app's own router, not by an HTTP call to ourselves. A route dropped
    from main.py shows up here as False while /health still says 200.

    `events` -- has any traffic arrived, and when? This is the part a
    reachability check cannot answer: a correctly mounted route with a
    misconfigured AC automation pointed at it looks perfectly healthy.

    Note what this deliberately does *not* claim. Quiet is not the same as
    broken -- no events just means nobody's tag changed, which is the normal
    state most days. The caller gets the timestamp and decides; this endpoint
    does not invent an up/down verdict out of silence.
    """
    _check_auth(authorization)

    # Local import: main.py imports this module, so this cannot happen at
    # module scope. `main` is already in sys.modules by the time any request
    # is served, so this is a dict lookup, not a re-import.
    #
    # Falls back to activecampaign_routes' own router if `main` isn't
    # importable under some entrypoint. A slightly weaker answer beats a 500 --
    # this endpoint exists to report status, so it has no business being the
    # thing that breaks.
    try:
        from main import app as _app
        routes, scope = _app.routes, "app"
    except Exception as e:
        import activecampaign_routes
        routes, scope = activecampaign_routes.router.routes, f"router-only ({e})"

    registered = any(
        getattr(r, "path", None) == AC_WEBHOOK_PATH
        and "POST" in (getattr(r, "methods", None) or set())
        for r in routes
    )

    return {
        "service": "attio-automation-hub",
        "webhook_path": AC_WEBHOOK_PATH,
        "route_registered": registered,
        "checked_against": scope,
        "expected_series_id": os.environ.get("ACTIVECAMPAIGN_SERIES_ID", "15"),
        "events": webhook_log.recent_summary("activecampaign"),
    }


# ---------------------------------------------------------------------------
# Automation flags
# ---------------------------------------------------------------------------

@router.get("/config/flags")
def get_config_flags(authorization: str | None = Header(None)):
    """Current value of every toggleable flag, plus where the value came from.

    `source` matters operationally: a flag showing `default` means neither the
    toggle nor the env var is set, which is also what a MotherDuck outage looks
    like -- so the Ops Center can show that rather than implying the stored
    value was read back successfully.
    """
    _check_auth(authorization)

    stored = {r["key"]: r for r in automation_config.list_flags()}
    out = []
    for key, meta in TOGGLEABLE_FLAGS.items():
        value = automation_config.get_flag(key, default=meta["default"], env_var=meta["env_var"])
        row = stored.get(key)
        if row is not None:
            source = "motherduck"
        elif os.environ.get(meta["env_var"]) is not None:
            source = "env"
        else:
            source = "default"
        out.append({
            "key": key,
            "value": value,
            "source": source,
            "label": meta["label"],
            "description": meta["description"],
            # Shown by the Ops Center when the flag reads False. Lives here so
            # adding a flag doesn't also mean editing the Streamlit page.
            "live_warning": meta.get("live_warning"),
            "updated_at": (row or {}).get("updated_at"),
            "updated_by": (row or {}).get("updated_by"),
        })
    return out


@router.patch("/config/flags/{key}")
def set_config_flag(
    key: str,
    value: bool,
    updated_by: str = "ops-center",
    authorization: str | None = Header(None),
):
    """Toggle a flag. Takes effect on the next scheduled run -- no redeploy."""
    _check_auth(authorization)
    if key not in TOGGLEABLE_FLAGS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown flag {key!r}; known flags: {sorted(TOGGLEABLE_FLAGS)}",
        )
    try:
        automation_config.set_flag(key, value, updated_by=updated_by)
    except Exception as e:
        # set_flag raising means the value was not persisted. Surfacing 503
        # keeps the Ops Center from rendering a toggle the jobs will not obey.
        raise HTTPException(status_code=503, detail=redact_secrets(str(e)))
    return {"key": key, "value": value, "updated_by": updated_by}


@router.get("/status/allo-tag-registry")
def status_allo_tag_registry(authorization: str | None = Header(None)):
    _check_auth(authorization)
    con = md_connection()
    try:
        cur = con.execute(
            f"""SELECT tag_name, action_type, action_params, active, notes,
                       created_at, updated_at
                FROM {MOTHERDUCK_DB}.main.allo_tag_registry
                ORDER BY active DESC, tag_name"""
        )
        rows = rows_to_dicts(cur)
    finally:
        con.close()

    # action_params is JSON and created_at/updated_at are timestamps --
    # stringify so FastAPI's default encoder does not choke.
    for r in rows:
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                r[k] = v.isoformat()
            elif not isinstance(v, (str, int, float, bool, type(None))):
                r[k] = str(v)
    return rows
