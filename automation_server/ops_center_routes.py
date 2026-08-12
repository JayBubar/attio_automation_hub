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
from pathlib import Path

import automation_config
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
def get_rep_tasks(rep: str, authorization: str | None = Header(None)):
    """Open Attio tasks assigned to this rep, left-joined against the
    AI-drafted email bodies in MotherDuck. Call tasks have no draft row by
    design and come back with draft: null."""
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
    task_ids = [t["id"]["task_id"] for t in mine]

    drafts = {}
    if task_ids:
        con = md_connection()
        try:
            placeholders = ",".join("?" for _ in task_ids)
            cur = con.execute(
                f"""SELECT task_id, subject, body, sequence_position
                    FROM {MOTHERDUCK_DB}.main.outreach_email_drafts
                    WHERE task_id IN ({placeholders})""",
                task_ids,
            )
            drafts = {r["task_id"]: r for r in rows_to_dicts(cur)}
        finally:
            con.close()

    def company_from_content(content: str):
        """Cadence tasks are titled "Email 1/3 · {company}: {contact}".
        Anything else is a hand-written task with no company to parse --
        return None rather than echoing the whole title back as a company."""
        if "·" not in content:
            return None
        return content.split("·", 1)[1].split(":", 1)[0].strip() or None

    out = []
    for t in mine:
        task_id = t["id"]["task_id"]
        linked = t.get("linked_records") or []
        d = drafts.get(task_id)
        content = t.get("content_plaintext") or t.get("content") or ""
        out.append({
            "task_id": task_id,
            "content": content,
            "deadline_at": t.get("deadline_at"),
            "company_name": company_from_content(content),
            "linked_record_id": linked[0].get("target_record_id") if linked else None,
            "draft": (
                {"subject": d["subject"], "body": d["body"],
                 "sequence_position": d["sequence_position"]} if d else None
            ),
        })

    out.sort(key=lambda t: (t["deadline_at"] or "9999"))
    return out


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
