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
carrying a second copy of the mapping. Changing what "Left a VM" does is a row
edit that both callers pick up on their next request -- no redeploy of either
service, and no possibility of the two drifting.

That is also what makes the tag lists reconcilable: Allo's tag vocabulary,
Attio's `call_outcome` options, and this table have to agree, and one of them
has to be authoritative. This is it.

## Prospecting vs maintenance

Only prospecting outcomes move Prospect Path. A maintenance call on an existing
Client shouldn't move them anywhere -- it is logged (the `call_outcome` field
plus a note) and nothing else. Registry rows carry that distinction in
`action_params.prospect_path`: present means move, absent means log only. So
"maintenance" is not a separate code path, just a row with no path in it.
"""

import json
import os
from datetime import datetime, timezone

import duckdb
import requests

ATTIO_BASE = "https://api.attio.com/v2"
REGISTRY_TABLE = "allo_tag_registry"


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


def outcome_options(prospect_path=None):
    """Outcomes offered for a contact on this path.

    Client records get the maintenance subset, everyone else the prospecting
    subset. The split lives in the registry (`action_params.audience`) rather
    than in the UI, so adding an outcome never means editing Streamlit.
    """
    out = []
    for row in load_registry().values():
        if not row["active"]:
            continue
        params = _params(row)
        audience = params.get("audience", "prospecting")
        wanted = "maintenance" if prospect_path == "Client" else "prospecting"
        if audience in (wanted, "both"):
            out.append(row["tag_name"])
    return sorted(out)


def _params(row):
    raw = row.get("action_params")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return raw or {}


def apply_outcome(record_id, outcome, note=None, source="task-runner"):
    """Apply one call outcome to an Attio person. Returns a result dict.

    Never raises on an unknown outcome -- it returns a status the caller can
    surface. A rep picking an option the registry hasn't caught up with should
    see "not mapped", not a 500.
    """
    registry = load_registry()
    row = registry.get((outcome or "").strip().lower())
    if row is None:
        return {"ok": False, "status": "unknown_outcome", "outcome": outcome,
                "known": sorted(r["tag_name"] for r in registry.values())}
    if not row["active"]:
        return {"ok": False, "status": "inactive_outcome", "outcome": outcome}

    params = _params(row)
    values = {"call_outcome": [{"option": row["tag_name"]}]}

    # Absent prospect_path == log only. That is how maintenance outcomes on a
    # Client stay put while prospecting outcomes move the funnel.
    path = params.get("prospect_path")
    if path:
        values["prospect_path"] = [{"status": path}]
        values["last_path_change_date"] = [
            {"value": datetime.now(timezone.utc).date().isoformat()}
        ]

    resp = requests.patch(
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(), json={"data": {"values": values}}, timeout=30,
    )
    if not resp.ok:
        print(f"call_outcomes: PATCH {record_id} failed {resp.status_code}: {resp.text[:800]}")
        print(f"call_outcomes: rejected payload {values}")
        return {"ok": False, "status": "attio_rejected",
                "detail": resp.text[:500], "sent": values}

    note_id = None
    if note and note.strip():
        note_id = _create_note(record_id, outcome, note.strip(), source)

    return {"ok": True, "status": "applied", "outcome": row["tag_name"],
            "prospect_path": path, "path_changed": bool(path), "note_id": note_id}


def _create_note(record_id, outcome, body, source):
    """Best-effort. A failed note must not undo an applied outcome -- the
    outcome is the record of truth, the note is commentary on it."""
    try:
        resp = requests.post(
            f"{ATTIO_BASE}/notes", headers=attio_headers(),
            json={"data": {
                "parent_object": "people",
                "parent_record_id": record_id,
                "title": f"Call outcome: {outcome}",
                "format": "plaintext",
                "content": body,
            }}, timeout=30,
        )
        if resp.ok:
            return resp.json()["data"]["id"]["note_id"]
        print(f"call_outcomes: note create failed {resp.status_code}: {resp.text[:300]}")
    except requests.RequestException as e:
        print(f"call_outcomes: note create errored ({e})")
    return None
