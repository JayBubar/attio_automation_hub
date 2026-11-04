"""
ac_engagement_routes.py

ActiveCampaign clicks and guide downloads -> Attio notes and follow-up tasks.

    POST /webhooks/ac/engagement?token=...   AC native webhooks (click,
                                             contact_tag_added) land here
    POST /ac-engagement/sweep                judge + write now (OPS token);
                                             ?dry_run=true previews only
    GET  /status/ac-engagement               counts, last event, last sweep

## Flow

1. The webhook parses the form payload, drops AC plumbing (open pixel,
   unsubscribe links, tags other than the guide tag) and buffers the rest.
   A background thread flushes the buffer to MotherDuck every few seconds
   (`ac_engagement_events`, status `pending`), so a scanner burst of 40 clicks
   is one insert, not 40 connections.
2. Every AC_ENGAGEMENT_SWEEP_SECONDS the sweep claims pending events at least
   AC_ENGAGEMENT_HOLD_SECONDS old and runs the scanner filter over them plus
   their already-judged neighbours (ac_engagement_rules.py). The hold exists
   because a burst can only be recognised once it is over.
3. Real events are matched to an Attio person (email first, AC's Attio Record
   ID as the cross-check and fallback), grouped by person + kind
   (Download / AC Activity), and written:
     * no open thread -> note "AC Download — <campaign>" + task with the same
       prefix, assigned to the person's Contact Owner (Jay if blank or not a
       workspace member), due 5pm CT next business day;
     * open thread (task not completed) -> lines appended to that note, no new
       task;
     * either way, last_activity_date moves forward.
   The person + kind -> note/task mapping is `ac_engagement_threads`.
4. Every event ends in a status the 990 tool's dashboard reads directly:
   real / dropped (with the rule) / exception (no Attio match) / error.

## Off by default

AC_ENGAGEMENT_ENABLED=false collects and stores events but writes nothing to
Attio and runs no automatic sweep. Use `POST /ac-engagement/sweep?dry_run=true`
to see what it would do, then turn it on. Pending events are kept, so nothing
received while it was off is lost.

## Single process

The sweep claims events with an UPDATE before working on them, so two copies
of the service (a deploy overlap) cannot both write the same event. The
buffer is in memory: events received in the last few seconds before a crash
are lost. AC does not retry a 200, so that window is kept short (flush every
AC_ENGAGEMENT_FLUSH_SECONDS, and on shutdown).
"""

from __future__ import annotations

import json
import os
import secrets
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import duckdb
import requests
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool

import ac_engagement_rules as rules
import http_client
from activecampaign_routes import attio_find_person_by_email, attio_headers
from ops_center_routes import REP_MEMBER_IDS, _check_auth

router = APIRouter()

WEBHOOK_PATH = "/webhooks/ac/engagement"
ATTIO_BASE = "https://api.attio.com/v2"
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")
EVENTS = "ac_engagement_events"
THREADS = "ac_engagement_threads"


def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_list(name, default):
    raw = os.environ.get(name)
    if not raw:
        return tuple(default)
    return tuple(p.strip() for p in raw.split(",") if p.strip())


AC_WEBHOOK_TOKEN = os.environ.get("AC_WEBHOOK_TOKEN", "")
ENABLED = _env_bool("AC_ENGAGEMENT_ENABLED", False)
SWEEP_SECONDS = int(os.environ.get("AC_ENGAGEMENT_SWEEP_SECONDS", "600"))
HOLD_SECONDS = int(os.environ.get("AC_ENGAGEMENT_HOLD_SECONDS", "300"))
FLUSH_SECONDS = int(os.environ.get("AC_ENGAGEMENT_FLUSH_SECONDS", "10"))
MAX_ATTEMPTS = 5
GUIDE_LINK_PATTERNS = _env_list("AC_GUIDE_LINK_PATTERNS", rules.DEFAULT_GUIDE_LINK_PATTERNS)
GUIDE_TAGS = _env_list("AC_GUIDE_TAGS", rules.DEFAULT_GUIDE_TAGS)
ATTIO_FIELD_KEYS = _env_list("AC_ATTIO_FIELD_KEYS", rules.DEFAULT_ATTIO_FIELD_KEYS)

AC_API_URL = os.environ.get("ACTIVECAMPAIGN_API_URL", "").rstrip("/")
AC_API_KEY = os.environ.get("ACTIVECAMPAIGN_API_KEY") or os.environ.get("AC_API_KEY", "")

# Contact Owner (a select of names on People) -> workspace member. Edna Stone
# is an option but not a workspace member, so her contacts fall back to Jay.
OWNER_MEMBER_IDS = {
    "jay bubar": REP_MEMBER_IDS["jay"],
    "kurt haas": REP_MEMBER_IDS["kurt"],
    "joel weinbach": REP_MEMBER_IDS["joel"],
    "kevin hoyle": "d9bc01a6-73e6-4fec-b70d-ff6644a332d3",
}
FALLBACK_ASSIGNEE = os.environ.get("AC_ENGAGEMENT_FALLBACK_ASSIGNEE", REP_MEMBER_IDS["jay"])

_buffer: list[dict] = []
_buffer_lock = threading.Lock()
_sweep_lock = threading.Lock()
_BUFFER_CAP = 5000
_last_sweep: dict = {}
_worker_started = False


# ---------------------------------------------------------------------------
# MotherDuck
# ---------------------------------------------------------------------------

def md_connection():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")


def ensure_tables(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {EVENTS} (
            event_key       VARCHAR PRIMARY KEY,
            received_at     TIMESTAMP,   -- UTC
            event_at        TIMESTAMP,   -- UTC, AC's own timestamp
            event_type      VARCHAR,     -- click / contact_tag_added
            ac_contact_id   VARCHAR,
            email           VARCHAR,
            record_id_hint  VARCHAR,     -- AC's Attio Record ID field, bare uuid
            contact_name    VARCHAR,
            campaign_id     VARCHAR,
            campaign_name   VARCHAR,
            link_id         VARCHAR,
            link_url        VARCHAR,
            tag             VARCHAR,
            payload         VARCHAR,     -- raw form, JSON text
            status          VARCHAR,     -- pending/processing/real/dropped/exception/error
            reason          VARCHAR,     -- scanner rule, exception or error detail
            kind            VARCHAR,     -- Download / AC Activity
            attio_record_id VARCHAR,
            matched_by      VARCHAR,
            attio_action    VARCHAR,     -- created / appended
            note_id         VARCHAR,
            task_id         VARCHAR,
            assignee_id     VARCHAR,
            attempts        INTEGER DEFAULT 0,
            claimed_by      VARCHAR,
            claimed_at      TIMESTAMP,
            processed_at    TIMESTAMP
        )
    """)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {THREADS} (
            attio_record_id VARCHAR,
            kind            VARCHAR,
            note_id         VARCHAR,
            task_id         VARCHAR,
            title           VARCHAR,
            assignee_id     VARCHAR,
            opened_at       TIMESTAMP,
            last_event_at   TIMESTAMP,
            event_count     INTEGER,
            PRIMARY KEY (attio_record_id, kind)
        )
    """)


def _utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _aware(dt) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def flush_buffer() -> int:
    """Write buffered events. On failure they stay buffered for the next try."""
    with _buffer_lock:
        batch = list(_buffer)
    if not batch or not os.environ.get("MOTHERDUCK_TOKEN"):
        return 0
    rows = [[
        e["event_key"], _utc_naive(e["received_at"]), _utc_naive(e["event_at"]),
        e["event_type"], e["ac_contact_id"], e["email"], e["record_id_hint"],
        e["contact_name"], e["campaign_id"], e["campaign_name"], e["link_id"],
        e["link_url"], e["tag"], e["payload"],
    ] for e in batch]
    try:
        con = md_connection()
        try:
            ensure_tables(con)
            con.executemany(f"""
                INSERT OR IGNORE INTO {EVENTS}
                  (event_key, received_at, event_at, event_type, ac_contact_id, email,
                   record_id_hint, contact_name, campaign_id, campaign_name, link_id,
                   link_url, tag, payload, status, attempts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0)
            """, rows)
        finally:
            con.close()
    except Exception as e:
        print(f"ac_engagement: buffer flush failed, {len(batch)} kept ({e})")
        return 0
    with _buffer_lock:
        flushed = {e["event_key"] for e in batch}
        _buffer[:] = [e for e in _buffer if e["event_key"] not in flushed]
    return len(batch)


# ---------------------------------------------------------------------------
# Webhook
# ---------------------------------------------------------------------------

def _check_token(token: str | None):
    # Fail closed: AC native webhooks are unsigned, and this route ends in
    # Attio tasks. Without a token anyone who learns the URL could create them.
    if not AC_WEBHOOK_TOKEN:
        raise HTTPException(status_code=503, detail="AC_WEBHOOK_TOKEN not configured")
    if not token or not secrets.compare_digest(token, AC_WEBHOOK_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.post(WEBHOOK_PATH)
async def ac_engagement_webhook(request: Request, token: str | None = None):
    _check_token(token)
    form = await request.form()
    payload = {k: str(v) for k, v in form.items()}
    received_at = datetime.now(timezone.utc)
    event = rules.parse_payload(payload, received_at, ATTIO_FIELD_KEYS)
    skip = rules.ignore_reason(event, GUIDE_TAGS)
    if skip:
        return {"status": "ignored", "reason": skip}
    event["received_at"] = received_at
    event["payload"] = json.dumps(payload)[:20000]
    with _buffer_lock:
        if len(_buffer) >= _BUFFER_CAP:
            # MotherDuck has been unreachable for a long time. Refusing makes AC
            # retry later instead of the hub silently dropping events.
            raise HTTPException(status_code=503, detail="engagement buffer full")
        _buffer.append(event)
    return {"status": "queued", "event_key": event["event_key"]}


# ---------------------------------------------------------------------------
# Attio
# ---------------------------------------------------------------------------

class AttioError(Exception):
    pass


def _ok(resp, what):
    if not resp.ok:
        raise AttioError(f"{what} {resp.status_code}: {resp.text[:400]}")
    return resp.json() if resp.content else {}


def attio_person(record_id):
    """(name, owner_title, last_activity_date) or None if the record is gone."""
    resp = http_client.get(f"{ATTIO_BASE}/objects/people/records/{record_id}",
                           headers=attio_headers())
    if resp.status_code in (400, 404):
        return None
    values = _ok(resp, "person get")["data"].get("values", {})

    def first(slug, key):
        for v in values.get(slug) or []:
            if key == "option":
                opt = v.get("option") or {}
                if opt.get("title"):
                    return opt["title"]
            elif v.get(key):
                return v[key]
        return None

    return {
        "name": first("name", "full_name"),
        "owner": first("contact_owner", "option"),
        "last_activity_date": first("last_activity_date", "value"),
    }


def attio_create_note(record_id, title, content):
    resp = http_client.post(f"{ATTIO_BASE}/notes", headers=attio_headers(), json={"data": {
        "parent_object": "people", "parent_record_id": record_id,
        "title": title, "format": "plaintext", "content": content,
    }})
    return _ok(resp, "note create")["data"]["id"]["note_id"]


def attio_note_content(note_id):
    resp = http_client.get(f"{ATTIO_BASE}/notes/{note_id}", headers=attio_headers())
    if resp.status_code == 404:
        return None
    return _ok(resp, "note get")["data"].get("content_plaintext") or ""


def attio_set_note_content(note_id, content):
    # PATCH replaces the whole body; the caller re-reads it first so a rep's
    # own edits to the note are kept.
    resp = http_client.patch(f"{ATTIO_BASE}/notes/{note_id}", headers=attio_headers(),
                             json={"data": {"format": "plaintext", "content": content}})
    _ok(resp, "note patch")


def attio_task_open(task_id) -> bool:
    if not task_id:
        return False
    resp = http_client.get(f"{ATTIO_BASE}/tasks/{task_id}", headers=attio_headers())
    if resp.status_code == 404:
        return False
    return not _ok(resp, "task get")["data"].get("is_completed", False)


def attio_create_task(record_id, content, deadline, assignee_id):
    resp = http_client.post(f"{ATTIO_BASE}/tasks", headers=attio_headers(), json={"data": {
        "content": content,
        "format": "plaintext",
        "deadline_at": deadline.isoformat(),
        "is_completed": False,
        "linked_records": [{"target_object": "people", "target_record_id": record_id}],
        "assignees": [{"referenced_actor_type": "workspace-member",
                       "referenced_actor_id": assignee_id}],
    }})
    return _ok(resp, "task create")["data"]["id"]["task_id"]


def attio_set_last_activity(record_id, day_iso):
    resp = http_client.patch(f"{ATTIO_BASE}/objects/people/records/{record_id}",
                             headers=attio_headers(),
                             json={"data": {"values": {"last_activity_date": [{"value": day_iso}]}}})
    _ok(resp, "last_activity_date patch")


def resolve_person(event, cache):
    """(record_id, matched_by). Email first (Attio's dedupe key), then the AC field."""
    email, hint = event.get("email") or "", event.get("record_id_hint") or ""
    key = (email, hint)
    if key in cache:
        return cache[key]
    found = attio_find_person_by_email(email) if email else None
    if found:
        result = (found, "email" if not hint or hint == found else "email_id_mismatch")
    elif hint and attio_person(hint) is not None:
        result = (hint, "attio_record_id")
    else:
        result = (None, "no_email" if not email and not hint else "no_attio_match")
    cache[key] = result
    return result


# ---------------------------------------------------------------------------
# Sweep
# ---------------------------------------------------------------------------

def _campaign_send_times(campaign_ids):
    """campaign_id -> when AC last finished sending it (for early_click)."""
    out = {}
    if not (AC_API_URL and AC_API_KEY):
        return out
    for cid in {c for c in campaign_ids if c}:
        try:
            resp = http_client.get(f"{AC_API_URL}/api/3/campaigns/{cid}",
                                   headers={"Api-Token": AC_API_KEY})
            if resp.ok:
                ldate = (resp.json().get("campaign") or {}).get("ldate")
                if ldate:
                    out[cid] = rules.parse_ac_time(ldate, datetime.now(timezone.utc))
        except requests.RequestException as e:
            print(f"ac_engagement: campaign {cid} lookup failed ({e})")
    return out


_COLS = ("event_key", "received_at", "event_at", "event_type", "ac_contact_id", "email",
         "record_id_hint", "contact_name", "campaign_id", "campaign_name", "link_id",
         "link_url", "tag", "status", "reason", "attempts")


def _rows(cur):
    out = []
    for r in cur.fetchall():
        e = dict(zip(_COLS, r))
        e["event_at"] = _aware(e["event_at"])
        out.append(e)
    return out


def _load(con, dry_run, claim_id):
    cols = ", ".join(_COLS)
    now = datetime.now(timezone.utc)
    cutoff = _utc_naive(now - timedelta(seconds=HOLD_SECONDS))
    if dry_run:
        mine = _rows(con.execute(
            f"SELECT {cols} FROM {EVENTS} WHERE status = 'pending' AND received_at < ? "
            "ORDER BY event_at LIMIT 1000", [cutoff]))
    else:
        # A sweep that died mid-run leaves rows 'processing'; give them back.
        con.execute(f"UPDATE {EVENTS} SET status = 'pending', claimed_by = NULL "
                    "WHERE status = 'processing' AND claimed_at < ?",
                    [_utc_naive(now - timedelta(minutes=30))])
        con.execute(f"""
            UPDATE {EVENTS} SET status = 'processing', claimed_by = ?, claimed_at = ?
            WHERE event_key IN (SELECT event_key FROM {EVENTS}
                                WHERE status = 'pending' AND received_at < ?
                                ORDER BY event_at LIMIT 1000)
        """, [claim_id, _utc_naive(now), cutoff])
        mine = _rows(con.execute(
            f"SELECT {cols} FROM {EVENTS} WHERE claimed_by = ? AND status = 'processing'",
            [claim_id]))
    if not mine:
        return [], []
    lo = min(e["event_at"] for e in mine) - timedelta(minutes=15)
    hi = max(e["event_at"] for e in mine) + timedelta(minutes=15)
    context = _rows(con.execute(
        f"SELECT {cols} FROM {EVENTS} WHERE event_at BETWEEN ? AND ?",
        [_utc_naive(lo), _utc_naive(hi)]))
    return mine, context


def _write_group(con, record_id, kind, events, now):
    """One person + kind: create the note and task, or append to the open one."""
    events = sorted(events, key=lambda e: e["event_at"])
    lines = "\n".join(rules.note_line(e) for e in events)
    thread = con.execute(
        f"SELECT note_id, task_id, title, assignee_id FROM {THREADS} "
        "WHERE attio_record_id = ? AND kind = ?", [record_id, kind]).fetchone()

    person = attio_person(record_id) or {}
    action = None
    if thread and attio_task_open(thread[1]):
        note_id, task_id, title, assignee_id = thread
        current = attio_note_content(note_id) if note_id else None
        if current is None:  # note deleted; start a new one, keep the task
            note_id = attio_create_note(record_id, title, lines)
        else:
            attio_set_note_content(note_id, (current.rstrip() + "\n" + lines).strip())
        action = "appended"
    else:
        title = rules.note_title(kind, events[0])
        owner = (person.get("owner") or "").strip().lower()
        assignee_id = OWNER_MEMBER_IDS.get(owner, FALLBACK_ASSIGNEE)
        note_id = attio_create_note(record_id, title, lines)
        who = person.get("name") or events[0].get("contact_name") or events[0].get("email")
        task_id = attio_create_task(record_id, f"{title}: {who}",
                                    rules.task_deadline(now), assignee_id)
        action = "created"

    last_day = events[-1]["event_at"].astimezone(rules.LOCAL_TZ).date().isoformat()
    if (person.get("last_activity_date") or "") < last_day:
        attio_set_last_activity(record_id, last_day)

    return {"note_id": note_id, "task_id": task_id, "title": title,
            "assignee_id": assignee_id, "action": action,
            "last_event_at": events[-1]["event_at"], "count": len(events)}


def _finish(con, keys, status, reason=None, **extra):
    if not keys:
        return
    sets = ["status = ?", "reason = ?", "processed_at = ?", "claimed_by = NULL"]
    params = [status, reason, _utc_naive(datetime.now(timezone.utc))]
    for col, val in extra.items():
        sets.append(f"{col} = ?")
        params.append(val)
    placeholders = ", ".join("?" for _ in keys)
    con.execute(f"UPDATE {EVENTS} SET {', '.join(sets)} WHERE event_key IN ({placeholders})",
                params + list(keys))


def sweep(dry_run=False):
    """Judge every pending event past the hold and write the real ones to Attio."""
    if not _sweep_lock.acquire(blocking=False):
        return {"status": "busy"}
    started = datetime.now(timezone.utc)
    claim_id = uuid.uuid4().hex
    summary = {"dry_run": dry_run, "started_at": started.isoformat(), "claimed": 0,
               "dropped": defaultdict(int), "exceptions": defaultdict(int),
               "created": 0, "appended": 0, "errors": 0, "preview": []}
    try:
        flush_buffer()
        con = md_connection()
        try:
            ensure_tables(con)
            mine, context = _load(con, dry_run, claim_id)
            summary["claimed"] = len(mine)
            if not mine:
                return _summarise(summary)

            send_times = _campaign_send_times(
                e["campaign_id"] for e in mine if e["event_type"] == "click")
            dropped = rules.scanner_verdicts(context, send_times)

            by_reason = defaultdict(list)
            groups = defaultdict(list)
            exceptions = defaultdict(list)
            retry = []
            cache = {}
            for e in mine:
                rule = dropped.get(e["event_key"])
                if rule:
                    by_reason[rule].append(e["event_key"])
                    summary["dropped"][rule] += 1
                    continue
                kind = rules.classify(e, GUIDE_LINK_PATTERNS, GUIDE_TAGS)
                try:
                    record_id, matched_by = resolve_person(e, cache)
                except (AttioError, requests.RequestException) as err:
                    # Attio lookup failed, not "no match": try again next sweep.
                    retry.append((e, str(err)[:500]))
                    continue
                if not record_id:
                    exceptions[matched_by].append(e["event_key"])
                    summary["exceptions"][matched_by] += 1
                    continue
                e["kind"], e["matched_by"] = kind, matched_by
                groups[(record_id, kind)].append(e)

            if dry_run:
                for (record_id, kind), evs in groups.items():
                    summary["preview"].append({
                        "attio_record_id": record_id, "kind": kind,
                        "email": evs[0]["email"], "events": len(evs),
                        "title": rules.note_title(kind, evs[0]),
                        "lines": [rules.note_line(e) for e in evs]})
                return _summarise(summary)

            for rule, keys in by_reason.items():
                _finish(con, keys, "dropped", rule)
            for why, keys in exceptions.items():
                _finish(con, keys, "exception", why)
            for e, err in retry:
                summary["errors"] += 1
                attempts = (e["attempts"] or 0) + 1
                _finish(con, [e["event_key"]], "error" if attempts >= MAX_ATTEMPTS else "pending",
                        err, attempts=attempts)

            now = datetime.now(timezone.utc)
            for (record_id, kind), evs in groups.items():
                keys = [e["event_key"] for e in evs]
                try:
                    out = _write_group(con, record_id, kind, evs, now)
                except (AttioError, requests.RequestException) as err:
                    summary["errors"] += 1
                    print(f"ac_engagement: {record_id}/{kind} failed ({err})")
                    attempts = max(e["attempts"] or 0 for e in evs) + 1
                    status = "error" if attempts >= MAX_ATTEMPTS else "pending"
                    _finish(con, keys, status, str(err)[:500], attempts=attempts,
                            attio_record_id=record_id, kind=kind)
                    continue
                summary[out["action"]] += 1
                if out["action"] == "created":
                    # A new thread replaces the closed one for this person + kind.
                    con.execute(f"""
                        INSERT OR REPLACE INTO {THREADS} (attio_record_id, kind, note_id,
                            task_id, title, assignee_id, opened_at, last_event_at, event_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [record_id, kind, out["note_id"], out["task_id"], out["title"],
                          out["assignee_id"], _utc_naive(now),
                          _utc_naive(out["last_event_at"]), out["count"]])
                else:
                    con.execute(f"""
                        UPDATE {THREADS} SET note_id = ?, last_event_at = ?,
                            event_count = event_count + ?
                        WHERE attio_record_id = ? AND kind = ?
                    """, [out["note_id"], _utc_naive(out["last_event_at"]), out["count"],
                          record_id, kind])
                for e in evs:
                    _finish(con, [e["event_key"]], "real", None, kind=kind,
                            attio_record_id=record_id, matched_by=e["matched_by"],
                            attio_action=out["action"], note_id=out["note_id"],
                            task_id=out["task_id"], assignee_id=out["assignee_id"])
        finally:
            con.close()
        return _summarise(summary)
    except Exception as e:
        print(f"ac_engagement: sweep failed ({e})")
        summary["failed"] = str(e)[:500]
        return _summarise(summary)
    finally:
        if not dry_run:
            _last_sweep.clear()
            _last_sweep.update(_summarise(summary))
        _sweep_lock.release()


def _summarise(s):
    out = dict(s)
    out["dropped"] = dict(s["dropped"])
    out["exceptions"] = dict(s["exceptions"])
    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    if not out["dry_run"]:
        out.pop("preview", None)
    return out


# ---------------------------------------------------------------------------
# Background worker
# ---------------------------------------------------------------------------

def _worker():
    last_sweep = 0.0
    while True:
        time.sleep(FLUSH_SECONDS)
        try:
            flush_buffer()
            if ENABLED and SWEEP_SECONDS > 0 and time.time() - last_sweep >= SWEEP_SECONDS:
                last_sweep = time.time()
                sweep()
        except Exception as e:  # the loop must outlive any one failure
            print(f"ac_engagement: worker error ({e})")


def start_worker():
    global _worker_started
    if _worker_started:
        return
    _worker_started = True
    threading.Thread(target=_worker, name="ac-engagement", daemon=True).start()


def stop_worker():
    flush_buffer()


# ---------------------------------------------------------------------------
# Ops routes
# ---------------------------------------------------------------------------

@router.post("/ac-engagement/sweep")
async def trigger_sweep(dry_run: bool = True, authorization: str | None = Header(None)):
    """Run a sweep now. Defaults to a dry run; pass dry_run=false to write."""
    _check_auth(authorization)
    if not dry_run and not ENABLED:
        raise HTTPException(status_code=409,
                            detail="AC_ENGAGEMENT_ENABLED is false; only dry runs are allowed")
    return await run_in_threadpool(sweep, dry_run)


@router.get("/status/ac-engagement")
def status_ac_engagement(authorization: str | None = Header(None)):
    _check_auth(authorization)
    with _buffer_lock:
        buffered = len(_buffer)
    out = {"service": "attio-automation-hub", "webhook_path": WEBHOOK_PATH,
           "enabled": ENABLED, "token_configured": bool(AC_WEBHOOK_TOKEN),
           "sweep_seconds": SWEEP_SECONDS, "hold_seconds": HOLD_SECONDS,
           "buffered": buffered, "last_sweep": dict(_last_sweep) or None}
    try:
        con = md_connection()
        try:
            ensure_tables(con)
            out["last_event_at"] = con.execute(f"SELECT max(received_at) FROM {EVENTS}").fetchone()[0]
            out["status_7d"] = dict(con.execute(
                f"SELECT status, count(*) FROM {EVENTS} "
                "WHERE received_at > now() - INTERVAL 7 DAY GROUP BY 1").fetchall())
        finally:
            con.close()
        if out["last_event_at"] is not None:
            out["last_event_at"] = out["last_event_at"].isoformat()
        out["available"] = True
    except Exception as e:
        out.update(available=False, reason=str(e)[:300])
    return out
