"""
setup_call_outcomes.py

Reconciles the three lists that have to agree about call outcomes:

  1. Attio `call_outcome` select options   (what a rep can pick in Task Runner)
  2. MotherDuck `allo_tag_registry` rows   (what each outcome *does*)
  3. Allo's own tag list                   (manual -- see the note at the end)

Idempotent. Run locally:

    set ATTIO_API_KEY=...
    set MOTHERDUCK_TOKEN=...
    py setup_call_outcomes.py

## Select options CAN be created over the API

The redesign spec said there is "no API/tool path for creating select options,
only for writing values to options that already exist." That is not the case --
`POST /v2/objects/people/attributes/{slug}/options` creates them, and it is the
same call `allo_webhook.py` used to seed this very field. No UI work needed.

## Why the registry carries the audience

Task Runner shows prospecting outcomes to normal contacts and maintenance
outcomes to Clients, off one single-select field. Which subset an outcome
belongs to is `action_params.audience`, so adding an outcome later is a row
insert plus an option -- never a code change.

## Maintenance outcomes deliberately have no prospect_path

A maintenance call on an existing Client should be logged, not re-pathed.
`apply_outcome()` treats an absent `prospect_path` as "log only", so these rows
simply omit it rather than needing their own branch.
"""

import json
import os
import time

import duckdb
import requests

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
BASE_URL = "https://api.attio.com/v2"
HEADERS = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
SLUG = "call_outcome"

# (tag_name, action_type, action_params)
# prospect_path present -> moves the funnel. Absent -> logged only.
OUTCOMES = [
    # --- Prospecting -------------------------------------------------------
    ("Wrong Contact",     "booking_outcome", {"audience": "prospecting"}),
    ("To Call Back",      "booking_outcome", {"audience": "prospecting"}),
    ("Follow Up Later",   "booking_outcome", {"audience": "prospecting"}),
    ("Booked Discovery",  "booking_outcome", {"audience": "prospecting", "prospect_path": "Opportunity"}),
    ("Booked Demo",       "booking_outcome", {"audience": "prospecting", "prospect_path": "Opportunity"}),
    ("Not Interested",    "booking_outcome", {"audience": "prospecting", "prospect_path": "Not Interested"}),
    ("No Answer",         "booking_outcome", {"audience": "prospecting"}),
    # --- Maintenance (Prospect Path = Client) ------------------------------
    ("Connected - Client",       "booking_outcome", {"audience": "maintenance"}),
    ("Left Message - Client",    "booking_outcome", {"audience": "maintenance"}),
    ("Rescheduled",              "booking_outcome", {"audience": "maintenance"}),
    ("Needs Internal Follow-Up", "booking_outcome", {"audience": "maintenance"}),
    ("Escalate/Issue",           "booking_outcome", {"audience": "maintenance"}),
]

# Rows the old speculative design left behind. Deactivated rather than deleted:
# a contact may already carry one, and the row is the only record of what it
# meant. Deactivating stops it being offered without rewriting history.
RETIRE = ["Booked Discovery Call", "Call Me Later",
          "Send Info: Case Study", "Send Info: Overview", "Send Info: Pricing"]


def req(method, url, **kw):
    delay = 1
    resp = None
    for _ in range(4):
        resp = requests.request(method, url, headers=HEADERS, timeout=30, **kw)
        if resp.status_code < 500:
            return resp
        print(f"  transient {resp.status_code}, retry in {delay}s")
        time.sleep(delay)
        delay *= 2
    return resp


def ensure_options():
    resp = req("GET", f"{BASE_URL}/objects/people/attributes/{SLUG}/options")
    resp.raise_for_status()
    current = {o["title"] for o in resp.json()["data"]}
    print(f"Attio '{SLUG}' currently has {len(current)} options.")

    for title, _, _ in OUTCOMES:
        if title in current:
            print(f"  ok       {title}")
            continue
        r = req("POST", f"{BASE_URL}/objects/people/attributes/{SLUG}/options",
                json={"data": {"title": title}})
        print(f"  {'created ' if r.status_code < 300 else 'FAILED  '}{title}"
              + ("" if r.status_code < 300 else f" ({r.status_code} {r.text[:200]})"))


def sync_registry():
    conn = duckdb.connect(
        f"md:{os.environ.get('MOTHERDUCK_DATABASE', 'hubspot_email_archive')}"
        f"?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}"
    )
    try:
        for tag, action_type, params in OUTCOMES:
            conn.execute("DELETE FROM allo_tag_registry WHERE lower(tag_name) = lower(?)", [tag])
            conn.execute(
                """INSERT INTO allo_tag_registry
                   (tag_name, action_type, action_params, active, notes, created_at, updated_at)
                   VALUES (?, ?, ?, TRUE, ?, now(), now())""",
                [tag, action_type, json.dumps(params),
                 f"{params.get('audience', 'prospecting')} outcome; "
                 + (f"moves path to {params['prospect_path']}"
                    if params.get("prospect_path") else "logged only, no path change")],
            )
            print(f"  registry <- {tag}")

        for tag in RETIRE:
            n = conn.execute(
                "SELECT count(*) FROM allo_tag_registry WHERE lower(tag_name) = lower(?)", [tag]
            ).fetchone()[0]
            if n:
                conn.execute(
                    "UPDATE allo_tag_registry SET active = FALSE, updated_at = now(), "
                    "notes = 'Retired by setup_call_outcomes.py; superseded by the "
                    "reconciled list' WHERE lower(tag_name) = lower(?)", [tag])
                print(f"  registry -- retired {tag}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("== Attio call_outcome options ==")
    ensure_options()
    print("\n== allo_tag_registry ==")
    sync_registry()
    print("""
Done. Remaining manual step, in Allo's own settings:

  Allo's tag list is not reachable from any API here, so it stays hand-kept.
  Make its tags match these names exactly, or a call tagged directly in Allo
  (bypassing Task Runner) lands on a tag the registry does not know and logs
  as skipped_unknown_tag:

    Wrong Contact, To Call Back, Follow Up Later, Booked Discovery,
    Booked Demo, Not Interested, No Answer

  Known mismatches today: Allo sends "Follow-up later" (registry now has
  "Follow Up Later"), "Meeting Booked" (-> "Booked Discovery"), and "Demo"
  (-> "Booked Demo"). Renaming them in Allo is the fix.
""")
