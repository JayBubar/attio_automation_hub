"""
call_outcomes.py

Single source of truth for "this call outcome means this Attio change",
shared by the Allo webhook and the Task Runner's outcome selector.

## Why MotherDuck rather than a shared Python function

The obvious move is a function both callers import. It doesn't work here: the
Allo receiver is a *separate Railway service in a separate repo*
(`allo-webhook/`, deployed by `railway up`, no shared package), so there is
nothing to import across.

`allo_tag_registry` in MotherDuck already is the shared definition, and the Allo
receiver already reads it. So Task Runner reads the same table rather than
carrying a second copy of the mapping. Changing what "Send Information" does is
a row edit that both callers pick up on their next request -- no redeploy of
either service, and no possibility of the two drifting.

That is also what makes the tag lists reconcilable: Allo's tag vocabulary,
Attio's `call_outcome` options, and this table have to agree, and one of them
has to be authoritative. This is it.

## Two fields, not one filtered field

Prospecting and maintenance outcomes are different metrics for different
purposes -- one triggers automation, the other is tracking that justifies
future CS headcount -- so they live in two separate Attio select fields and are
never mixed:

  `call_outcome`         prospecting. Written by Allo's AI tagging during
                         PowerDialer calls, or by Task Runner for the same set.
                         Mapped through `allo_tag_registry`; may move Prospect
                         Path.
  `direct_call_outcome`  maintenance. Manual only, for contacts already on
                         Prospect Path = Client. Never touched by the Allo
                         webhook, never influences Prospect Path.

The registry therefore holds *only* Allo's tag vocabulary -- one row per tag
Allo can send. Maintenance outcomes are not Allo tags and deliberately have no
rows: their option list is read from the Attio field itself, which is where a
manual-only list belongs. Adding one is a select option in Attio, not a row
here and not a code change.

Neither list has a "No Answer". An unconnected call arrives from Allo as
`call.completed` with no tag at all, on purpose; `Allo Calls Count` on the
person record is what tracks attempt volume.
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import duckdb
import requests

import http_client

ATTIO_BASE = "https://api.attio.com/v2"
REGISTRY_TABLE = "allo_tag_registry"

CALL_OUTCOME_SLUG = "call_outcome"
DIRECT_OUTCOME_SLUG = "direct_call_outcome"

# Prospect Path value that routes a contact to the maintenance list. A constant
# because three separate decisions have to agree on the spelling.
CLIENT_PATH = "Client"

# The maintenance list is read from Attio rather than stored here, so it can
# never be a second copy that drifts. Cached briefly because Task Runner asks
# once per card and the option list changes about never.
_DIRECT_CACHE = {"options": None, "fetched_at": 0.0}
_DIRECT_CACHE_TTL = 300


def _db():
    return duckdb.connect(
        f"md:{os.environ.get('MOTHERDUCK_DATABASE', 'hubspot_email_archive')}"
        f"?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}"
    )


def attio_headers():
    return {
        "Authorization": f"Bearer {os.environ['ATTIO_API_KEY']}",
        "Content-Type": "application/json",
    }


def is_client(prospect_path):
    return (prospect_path or "").strip().lower() == CLIENT_PATH.lower()


def load_registry(conn=None):
    """{lowercased tag_name: row}. Matching is case-insensitive because the
    three systems that feed this table disagree about capitalisation."""
    own = conn is None
    conn = conn or _db()
    try:
        rows = conn.execute(
            f"SELECT tag_name, action_type, action_params, active FROM {REGISTRY_TABLE}"
        ).fetchall()
    finally:
        if own:
            conn.close()
    return {
        r[0].strip().lower(): {
            "tag_name": r[0], "action_type": r[1],
            "action_params": r[2], "active": r[3],
        }
        for r in rows
    }


def direct_outcome_options(force=False):
    """The `Direct Call Outcome` select options, straight off the Attio field.

    Read rather than hardcoded: this list is manual-only and lives entirely in
    Attio, so a copy here would be a second definition with nothing keeping it
    honest. On a read failure this returns [] and the caller says the list
    could not be loaded -- better than offering a stale hardcoded list that may
    no longer match the field and would be rejected on write.
    """
    fresh = time.time() - _DIRECT_CACHE["fetched_at"] < _DIRECT_CACHE_TTL
    if not force and _DIRECT_CACHE["options"] is not None and fresh:
        return list(_DIRECT_CACHE["options"])
    try:
        resp = http_client.get(
            f"{ATTIO_BASE}/objects/people/attributes/{DIRECT_OUTCOME_SLUG}/options",
            headers=attio_headers(),
        )
        resp.raise_for_status()
        options = [o["title"] for o in resp.json().get("data", [])
                   if not o.get("is_archived")]
    except (requests.RequestException, KeyError, ValueError) as e:
        print(f"call_outcomes: could not read {DIRECT_OUTCOME_SLUG} options ({e})")
        return []
    _DIRECT_CACHE["options"] = options
    _DIRECT_CACHE["fetched_at"] = time.time()
    return list(options)


def outcome_options(prospect_path=None):
    """Which outcomes to offer a contact on this path, and where they land.

    Returns the field slug alongside the list, because the two are one
    decision: a Client gets the maintenance options *and* the maintenance
    field, and nothing further down gets to reopen that.
    """
    if is_client(prospect_path):
        return {
            "audience": "maintenance",
            "field": DIRECT_OUTCOME_SLUG,
            "options": direct_outcome_options(),
            "moves_path": False,
        }
    return {
        "audience": "prospecting",
        "field": CALL_OUTCOME_SLUG,
        "options": sorted(r["tag_name"] for r in load_registry().values() if r["active"]),
        "moves_path": True,
    }


def _params(row):
    raw = row.get("action_params")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw or {}


def apply_outcome(record_id, outcome, note=None, source="task-runner",
                  prospect_path=None, assignee_id=None):
    """Apply one call outcome to an Attio person. Returns a result dict.

    `prospect_path` is the contact's path as the card was loaded, and picks the
    branch: Client goes to the maintenance field with no mapping and no path
    change, everyone else through the registry.

    Never raises on an unknown outcome -- it returns a status the caller can
    surface. A rep picking an option the registry hasn't caught up with should
    see "not mapped", not a 500.
    """
    if is_client(prospect_path):
        return _apply_direct_outcome(record_id, outcome, note, source)

    registry = load_registry()
    row = registry.get((outcome or "").strip().lower())
    if row is None:
        return {"ok": False, "status": "unknown_outcome", "outcome": outcome,
                "known": sorted(r["tag_name"] for r in registry.values())}
    if not row["active"]:
        return {"ok": False, "status": "inactive_outcome", "outcome": outcome}

    params = _params(row)
    action_type = row["action_type"]

    # Dispatch on action_type, exactly as the Allo receiver does. Writing the
    # tag name to `call_outcome` regardless of action_type would make the two
    # callers disagree about the one thing this module exists to hold in one
    # place: "Follow-up Later" is a *task*, and treating it as a field write
    # both mislabels the outcome and silently loses the callback.
    if action_type == "booking_outcome":
        return _apply_booking_outcome(record_id, row, params, note, source)
    if action_type == "task":
        return _apply_task_outcome(record_id, row, params, note, source, assignee_id)
    return {"ok": False, "status": "unsupported_action_type",
            "action_type": action_type, "outcome": row["tag_name"],
            "detail": f"{action_type} outcomes are handled by the Allo receiver only."}


def _apply_booking_outcome(record_id, row, params, note, source):
    # The registry's own `attio_call_outcome` is the option title to write --
    # not the tag name. They match for every row today and are allowed not to:
    # the tag name is Allo's vocabulary, the option title is Attio's.
    option = params.get("attio_call_outcome") or row["tag_name"]
    values = {CALL_OUTCOME_SLUG: [{"option": option}]}

    # Absent prospect_path == log only, the same rule the receiver follows.
    path = params.get("prospect_path")
    if path:
        values["prospect_path"] = [{"status": path}]
        values["last_path_change_date"] = [
            {"value": datetime.now(timezone.utc).date().isoformat()}
        ]

    ok, detail = _patch_person(record_id, values)
    if not ok:
        return {"ok": False, "status": "attio_rejected", "detail": detail, "sent": values}

    return {"ok": True, "status": "applied", "outcome": option,
            "field": CALL_OUTCOME_SLUG, "prospect_path": path,
            "path_changed": bool(path),
            "note_id": _maybe_note(record_id, option, note, source)}


def _apply_task_outcome(record_id, row, params, note, source, assignee_id):
    """Follow-up Later, and anything else the registry models as a callback."""
    if not assignee_id:
        return {"ok": False, "status": "no_assignee", "outcome": row["tag_name"],
                "detail": ("This outcome creates a follow-up task, which needs an "
                           "assignee. Pass the rep so it can be assigned.")}
    hours = params.get("deadline_offset_hours", 48)
    deadline = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    try:
        resp = http_client.post(
            f"{ATTIO_BASE}/tasks", headers=attio_headers(),
            json={"data": {
                "content": params.get("task_title", row["tag_name"]),
                "format": "plaintext",
                "deadline_at": deadline,
                "is_completed": False,
                "linked_records": [
                    {"target_object": "people", "target_record_id": record_id}],
                "assignees": [{"referenced_actor_type": "workspace-member",
                               "referenced_actor_id": assignee_id}],
            }},
        )
    except http_client.VENDOR_UNAVAILABLE_EXCEPTIONS as e:
        return {"ok": False, "status": "attio_unavailable", "outcome": row["tag_name"],
                "detail": f"Attio temporarily unreachable: {e}"}
    if not resp.ok:
        print(f"call_outcomes: task create failed {resp.status_code}: {resp.text[:500]}")
        return {"ok": False, "status": "attio_rejected", "detail": resp.text[:500]}

    return {"ok": True, "status": "task_created", "outcome": row["tag_name"],
            "field": None, "path_changed": False,
            "task_deadline_hours": hours,
            "note_id": _maybe_note(record_id, row["tag_name"], note, source)}


def _apply_direct_outcome(record_id, outcome, note, source):
    """Maintenance call on a Client: log it, move nothing.

    No registry lookup and no path branch by construction rather than by a flag
    that could be passed wrong -- there is nothing on this path that *can*
    write Prospect Path.
    """
    valid = direct_outcome_options()
    if not valid:
        return {"ok": False, "status": "direct_options_unavailable",
                "detail": (f"Could not read the {DIRECT_OUTCOME_SLUG} options from "
                           "Attio, so the selection could not be validated.")}
    match = next(
        (v for v in valid if v.strip().lower() == (outcome or "").strip().lower()), None)
    if match is None:
        return {"ok": False, "status": "unknown_outcome", "outcome": outcome,
                "known": valid}

    ok, detail = _patch_person(record_id, {DIRECT_OUTCOME_SLUG: [{"option": match}]})
    if not ok:
        return {"ok": False, "status": "attio_rejected", "detail": detail}

    return {"ok": True, "status": "applied", "outcome": match,
            "field": DIRECT_OUTCOME_SLUG, "prospect_path": None,
            "path_changed": False,
            "note_id": _maybe_note(record_id, match, note, source)}


def _patch_person(record_id, values):
    try:
        resp = http_client.patch(
            f"{ATTIO_BASE}/objects/people/records/{record_id}",
            headers=attio_headers(), json={"data": {"values": values}},
        )
    except http_client.VENDOR_UNAVAILABLE_EXCEPTIONS as e:
        return False, f"Attio temporarily unreachable: {e}"
    if resp.ok:
        return True, None
    print(f"call_outcomes: PATCH {record_id} failed {resp.status_code}: {resp.text[:800]}")
    print(f"call_outcomes: rejected payload {values}")
    return False, resp.text[:500]


def _maybe_note(record_id, outcome, note, source):
    if note and note.strip():
        return create_note(record_id, f"Call outcome: {outcome}", note.strip(), source)
    return None


def create_note(record_id, title, body, source="task-runner"):
    """Best-effort. A failed note must not undo an applied outcome -- the
    outcome is the record of truth, the note is commentary on it."""
    try:
        resp = http_client.post(
            f"{ATTIO_BASE}/notes", headers=attio_headers(),
            json={"data": {
                "parent_object": "people",
                "parent_record_id": record_id,
                "title": title,
                "format": "plaintext",
                "content": body,
            }},
        )
        if resp.ok:
            return resp.json()["data"]["id"]["note_id"]
        print(f"call_outcomes: note create failed {resp.status_code}: {resp.text[:300]}")
    except requests.RequestException as e:
        print(f"call_outcomes: note create errored ({e})")
    return None
