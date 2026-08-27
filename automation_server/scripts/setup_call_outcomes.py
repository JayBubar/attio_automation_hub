"""
setup_call_outcomes.py

Reconciles the lists that have to agree about call outcomes, and reports where
they don't. Read-only unless you pass --apply.

    py setup_call_outcomes.py            # report drift, change nothing
    py setup_call_outcomes.py --apply    # additionally create missing options

Needs ATTIO_API_KEY and MOTHERDUCK_TOKEN in the environment.

## The two fields are not interchangeable

  `call_outcome`         prospecting, 7 options. Written by Allo's AI tagging
                         and by Task Runner. Each one has an `allo_tag_registry`
                         row saying what it does; some move Prospect Path.
  `direct_call_outcome`  maintenance, 8 options. Manual only, for Clients.
                         No registry rows on purpose -- nothing in Allo emits
                         these, and nothing maps them to a path change.

So there are two reconciliations here, not one: the prospecting field has to
match the registry (and Allo's tag names), and the maintenance field just has
to exist with its options.

## Why this no longer writes the registry

An earlier version of this script owned the outcome list and rewrote
`allo_tag_registry` from a Python constant every run. That inverted the whole
point of the table: the registry is the authority both the Allo receiver and
Task Runner read at request time, and a script that overwrites it from a hardcoded
list makes the table a cache of this file. It also carried the pre-reversal
list -- "No Answer", "Booked Discovery", a maintenance subset sharing the
prospecting field -- so running it would have quietly undone the live mapping
and reintroduced an outcome that is deliberately absent.

Registry changes are row edits now, made against the table directly. This
script's job is to *notice* when the three lists have drifted apart, which is
the thing that actually goes wrong.

## No "No Answer", in either field

An unconnected call arrives from Allo as `call.completed` with no tag at all.
That is intentional: `Allo Calls Count` on the person record is how attempt
volume gets tracked, so a "No Answer" option would be a second, worse count of
the same thing. If you see one in either field, it is drift -- delete it.
"""

import argparse
import json
import os
import sys
import time

import duckdb
import requests

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
BASE_URL = "https://api.attio.com/v2"
HEADERS = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}

CALL_OUTCOME_SLUG = "call_outcome"
DIRECT_OUTCOME_SLUG = "direct_call_outcome"

# The maintenance list, and the only list this script still holds literally.
# It has no registry rows to be checked against, so drift here has nothing else
# to be caught by.
DIRECT_OUTCOMES = [
    "Check-In",
    "Onboarding/Training",
    "Support Issue - Resolved",
    "Support Issue - Escalated",
    "Billing/Collections",
    "Renew/Upsell Discussion",
    "Left Message",
    "Follow-Up Scheduled",
]

# Outcomes that must never exist in either field. See the module docstring.
FORBIDDEN = {"no answer"}


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


def field_options(slug):
    resp = req("GET", f"{BASE_URL}/objects/people/attributes/{slug}/options")
    resp.raise_for_status()
    return [o["title"] for o in resp.json()["data"] if not o.get("is_archived")]


def create_option(slug, title):
    r = req("POST", f"{BASE_URL}/objects/people/attributes/{slug}/options",
            json={"data": {"title": title}})
    ok = r.status_code < 300
    print(f"  {'created ' if ok else 'FAILED  '}{title}"
          + ("" if ok else f" ({r.status_code} {r.text[:200]})"))
    return ok


def registry_rows():
    conn = duckdb.connect(
        f"md:{os.environ.get('MOTHERDUCK_DATABASE', 'hubspot_email_archive')}"
        f"?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}"
    )
    try:
        rows = conn.execute(
            "SELECT tag_name, action_type, action_params, active "
            "FROM allo_tag_registry ORDER BY tag_name"
        ).fetchall()
    finally:
        conn.close()
    out = []
    for tag, action_type, params, active in rows:
        parsed = json.loads(params) if isinstance(params, str) else (params or {})
        out.append({"tag_name": tag, "action_type": action_type,
                    "params": parsed, "active": active})
    return out


def check_prospecting(apply_changes):
    """Registry is the authority; the field has to be able to hold what it maps."""
    print(f"== {CALL_OUTCOME_SLUG} (prospecting) ==")
    rows = registry_rows()
    options = field_options(CALL_OUTCOME_SLUG)
    print(f"  registry: {len(rows)} rows ({sum(1 for r in rows if r['active'])} active)")
    print(f"  field:    {len(options)} options")

    problems = []

    # Only `booking_outcome` rows write the field. A `task` row (Follow-up
    # Later) creates a callback instead, so it needs no option -- flagging one
    # as missing would send you creating an option nothing ever writes.
    needed = {}
    for row in rows:
        if not row["active"] or row["action_type"] != "booking_outcome":
            continue
        needed[row["params"].get("attio_call_outcome") or row["tag_name"]] = row["tag_name"]

    for title, tag in sorted(needed.items()):
        if title in options:
            print(f"  ok       {title}")
        else:
            print(f"  MISSING  {title}  (registry row {tag!r} writes it)")
            if apply_changes:
                create_option(CALL_OUTCOME_SLUG, title)
            else:
                problems.append(f"{CALL_OUTCOME_SLUG} has no option {title!r}")

    for title in options:
        if title in needed:
            continue
        if title.lower() in FORBIDDEN:
            print(f"  FORBIDDEN {title}  — delete this option; see the docstring")
            problems.append(f"{CALL_OUTCOME_SLUG} carries a forbidden option {title!r}")
        else:
            # Not an error on its own: an option can outlive the row that wrote
            # it, and contacts may still carry it. Worth seeing, not fixing.
            print(f"  extra    {title}  (no active registry row writes this)")

    for row in rows:
        if row["action_type"] == "booking_outcome" and not row["params"].get("prospect_path"):
            print(f"  note     {row['tag_name']}: logged only, no path change")
        if row["action_type"] == "task":
            hours = row["params"].get("deadline_offset_hours", 48)
            print(f"  note     {row['tag_name']}: creates a task, {hours}h deadline")
        if row["tag_name"].lower() in FORBIDDEN:
            problems.append(f"allo_tag_registry carries a forbidden row {row['tag_name']!r}")

    return problems


def check_maintenance(apply_changes):
    print(f"\n== {DIRECT_OUTCOME_SLUG} (maintenance) ==")
    options = field_options(DIRECT_OUTCOME_SLUG)
    problems = []

    for title in DIRECT_OUTCOMES:
        if title in options:
            print(f"  ok       {title}")
        elif apply_changes:
            create_option(DIRECT_OUTCOME_SLUG, title)
        else:
            print(f"  MISSING  {title}")
            problems.append(f"{DIRECT_OUTCOME_SLUG} has no option {title!r}")

    for title in options:
        if title in DIRECT_OUTCOMES:
            continue
        if title.lower() in FORBIDDEN:
            print(f"  FORBIDDEN {title}  — delete this option; see the docstring")
            problems.append(f"{DIRECT_OUTCOME_SLUG} carries a forbidden option {title!r}")
        else:
            # Task Runner reads this field's options live, so an addition made
            # in Attio is already in use -- it just isn't in this file yet.
            print(f"  extra    {title}  (live in Task Runner; add it here to keep "
                  f"this check honest)")

    return problems


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="create missing select options (never deletes, never "
                         "writes allo_tag_registry)")
    args = ap.parse_args()

    if not args.apply:
        print("Reporting only. Re-run with --apply to create missing options.\n")

    problems = check_prospecting(args.apply) + check_maintenance(args.apply)

    print("""
Remaining manual step, in Allo's own settings:

  Allo's tag list is not reachable from any API here, so it stays hand-kept.
  Its tags must match the `allo_tag_registry` tag names exactly, or a call
  tagged in Allo lands on a tag the registry does not know and logs as
  skipped_unknown_tag.
""")

    if problems:
        print(f"{len(problems)} problem(s):")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)
    print("No drift.")


if __name__ == "__main__":
    main()
