"""
campaign_routes.py

Campaign tracking for the Ops Center's Campaign Detail page. Registered in
main.py via app.include_router(campaign_router).

## Why the campaign -> list link is a text field

Attio record-reference attributes can only point at other *objects*, never at
Lists. So there is no native way to say "this Campaign's targets are that
List". The Campaigns object carries a `target_list_slug` text field holding the
List's api_slug (e.g. `bbcon_2026_targets`), and this module resolves through
it. Matching by name string was the alternative and it breaks the first time
someone renames a list.

## Why activity is joined at query time

`contact_activity_log` has no campaign column and deliberately gets none. A
contact can sit on several campaign lists at once -- that is what Lists are
for -- so stamping one campaign onto an activity row at write time forces a
choice that isn't the row's to make. Campaign-scoped activity is therefore a
*view*: take the list's current members, join their emails against the log.
Re-running it next month against a changed list gives the answer for the list
as it is then, which is the correct behaviour for a membership-based question.

## Deal value: `value` is not the field to sum

The Deals object has three currency attributes, and the obvious-looking one is
the wrong one:

    value                       "Deal value"                  EMPTY in practice
    deal_value_arr              "Deal Value - ARR"            populated
    deal_value_implementation   "Deal Value - Implementation" populated

Every deal in the workspace as of 2026-08-13 leaves `value` unset and fills the
other two. Summing `value` returns 0.00 for every campaign while looking
exactly like a working ROI panel, so the default basis here is ARR +
Implementation (first-year contract value, which is what a campaign budget is
actually being compared against). `basis` on the detail route switches it, and
the per-deal breakdown is always returned so the total can be audited rather
than trusted.
"""

import os

import requests
from fastapi import APIRouter, Header, HTTPException

# Auth and the MotherDuck connection are defined once, in ops_center_routes,
# and imported rather than re-implemented -- a second copy of the auth check is
# a second thing to forget to fix. main.py imports both modules and
# ops_center_routes does not import this one, so there is no cycle.
from ops_center_routes import _check_auth, md_connection, MOTHERDUCK_DB

router = APIRouter()

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"
HTTP_TIMEOUT = 30

WON_STAGE = "Won"

# Attribute slugs, read off the live workspace rather than guessed. Several do
# not match their titles -- Attio appends a suffix when a slug collides with
# one that existed before -- so none of these are safe to infer.
C_NAME = "name_1"
C_BUDGET = "budget"
C_START = "start_date"
C_END = "end_date_6"
C_STATUS = "status_6"
C_TYPE = "type"
C_EVENT_DETAILS = "event_name_details"
C_TARGET_LIST = "target_list_slug"

D_CAMPAIGN = "campaign_4"
D_STAGE = "stage"
D_VALUE = "value"
D_ARR = "deal_value_arr"
D_IMPL = "deal_value_implementation"

VALUE_BASES = {
    "arr_plus_implementation": (D_ARR, D_IMPL),
    "arr_only": (D_ARR,),
    "deal_value_field": (D_VALUE,),
}
DEFAULT_BASIS = "arr_plus_implementation"


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# Attio value unwrapping
#
# Attio wraps every attribute in a list of dicts whose inner key depends on the
# attribute type. One tolerant reader beats a per-type branch at each call site.
# ---------------------------------------------------------------------------

def _one(values, slug):
    entries = (values or {}).get(slug) or []
    return entries[0] if entries else None


def _scalar(values, slug):
    d = _one(values, slug)
    if not d:
        return None
    # full_name covers the personal-name type, which People's `name` uses and
    # which carries no plain `value` key at all.
    for key in ("value", "currency_value", "email_address", "full_name",
                "target_record_id"):
        if d.get(key) is not None:
            return d[key]
    for key in ("status", "option"):
        if isinstance(d.get(key), dict):
            return d[key].get("title")
    return None


def _titles(values, slug):
    """Every title for a multiselect, rather than just the first."""
    out = []
    for d in (values or {}).get(slug) or []:
        for key in ("status", "option"):
            if isinstance(d.get(key), dict) and d[key].get("title"):
                out.append(d[key]["title"])
        if d.get("value") is not None and not out:
            out.append(d["value"])
    return out


def _money(values, slug):
    d = _one(values, slug)
    if not d:
        return None
    v = d.get("currency_value")
    return float(v) if v is not None else None


# ---------------------------------------------------------------------------
# Attio fetches
# ---------------------------------------------------------------------------

def _get(url):
    resp = requests.get(url, headers=attio_headers(), timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()["data"]


def _query_all(url, body=None, limit=500):
    """Page a POST-query endpoint to exhaustion."""
    out = []
    offset = 0
    while True:
        resp = requests.post(
            url, headers=attio_headers(),
            json={**(body or {}), "limit": limit, "offset": offset},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        page = resp.json().get("data", [])
        out.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return out


def fetch_campaigns():
    return _query_all(f"{ATTIO_BASE}/objects/campaigns/records/query")


def fetch_campaign(record_id):
    return _get(f"{ATTIO_BASE}/objects/campaigns/records/{record_id}")


def fetch_list_entries(list_slug):
    """List entries by api_slug. Attio accepts a slug wherever it accepts a
    list id, which is the whole reason target_list_slug stores one."""
    return _query_all(f"{ATTIO_BASE}/lists/{list_slug}/entries/query")


def fetch_people(record_ids):
    """Person records for a set of ids, one GET each.

    Attio has no documented bulk-by-id read for records, and list entries carry
    only the entry's own attributes -- not the parent's email, which is the
    join key for the activity log. So this is N requests for N members.

    Fine at conference-target-list scale (tens to low hundreds, behind a button
    the user pressed). If a campaign list ever runs to thousands this becomes
    the slow part of the page and wants a cached email crosswalk instead.
    """
    people = {}
    for rid in record_ids:
        try:
            people[rid] = _get(f"{ATTIO_BASE}/objects/people/records/{rid}")
        except requests.RequestException as e:
            # One unreadable member must not blank the whole page.
            print(f"campaign: could not read person {rid} ({e})")
    return people


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/campaigns")
def list_campaigns(authorization: str | None = Header(None)):
    """Every campaign, shaped for a selector."""
    _check_auth(authorization)
    out = []
    for rec in fetch_campaigns():
        v = rec.get("values", {})
        out.append({
            "record_id": rec["id"]["record_id"],
            "name": _scalar(v, C_NAME),
            "status": _scalar(v, C_STATUS),
            "target_list_slug": _scalar(v, C_TARGET_LIST),
            "budget": _money(v, C_BUDGET),
        })
    out.sort(key=lambda c: (c["name"] or "").lower())
    return out


@router.get("/campaigns/{record_id}/detail")
def campaign_detail(
    record_id: str,
    basis: str = DEFAULT_BASIS,
    authorization: str | None = Header(None),
):
    """Everything the Campaign Detail page renders, in one call.

    One endpoint rather than four because every panel needs the same list
    membership, and resolving it once server-side (where the Attio and
    MotherDuck credentials live) beats the Streamlit page making four
    round-trips that can disagree with each other mid-render.
    """
    _check_auth(authorization)
    if basis not in VALUE_BASES:
        raise HTTPException(
            status_code=400,
            detail=f"basis must be one of {sorted(VALUE_BASES)}",
        )

    try:
        campaign = fetch_campaign(record_id)
    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else 502
        raise HTTPException(status_code=404 if code == 404 else 502,
                            detail=f"campaign {record_id} could not be read")
    cv = campaign.get("values", {})

    budget = _money(cv, C_BUDGET)
    out = {
        "campaign": {
            "record_id": record_id,
            "name": _scalar(cv, C_NAME),
            "budget": budget,
            "start_date": _scalar(cv, C_START),
            "end_date": _scalar(cv, C_END),
            "status": _scalar(cv, C_STATUS),
            "type": _titles(cv, C_TYPE),
            "event_name_details": _scalar(cv, C_EVENT_DETAILS),
            "target_list_slug": _scalar(cv, C_TARGET_LIST),
        }
    }

    # --- Targets -----------------------------------------------------------
    slug = (_scalar(cv, C_TARGET_LIST) or "").strip()
    members, list_error = [], None
    if not slug:
        list_error = (
            "This campaign has no Target List Slug set, so it has no targets to "
            "report on. Set it on the Campaign record in Attio to the list's "
            "api_slug (e.g. bbcon_2026_targets)."
        )
    else:
        try:
            entries = fetch_list_entries(slug)
        except requests.HTTPError:
            entries = []
            list_error = (
                f"No Attio list with api_slug {slug!r}. The Target List Slug on "
                "the campaign does not match a real list — check it for a typo "
                "or a renamed list."
            )
        if not list_error:
            parent_ids = [e.get("parent_record_id") for e in entries if e.get("parent_record_id")]
            people = fetch_people(parent_ids)
            for e in entries:
                pid = e.get("parent_record_id")
                ev = e.get("entry_values", {})
                pv = (people.get(pid) or {}).get("values", {})
                members.append({
                    "record_id": pid,
                    "name": _scalar(pv, "name"),
                    "email": _scalar(pv, "email_addresses"),
                    "attended_event": bool(_scalar(ev, "attended_event")),
                    "in_person_meeting_scheduled": bool(_scalar(ev, "in_person_meeting_scheduled")),
                    "follow_up_status": _scalar(ev, "follow_up_status"),
                    "source": _titles(ev, "source"),
                    "notes": _scalar(ev, "notes"),
                })

    out["targets"] = {"list_slug": slug or None, "error": list_error,
                      "count": len(members), "members": members}

    # --- Funnel ------------------------------------------------------------
    follow_up = {}
    sources = {}
    for m in members:
        follow_up[m["follow_up_status"] or "Not set"] = \
            follow_up.get(m["follow_up_status"] or "Not set", 0) + 1
        for s in m["source"] or ["Not set"]:
            sources[s] = sources.get(s, 0) + 1

    out["funnel"] = {
        "targeted": len(members),
        "attended": sum(1 for m in members if m["attended_event"]),
        "meeting_scheduled": sum(1 for m in members if m["in_person_meeting_scheduled"]),
        "follow_up_status": follow_up,
        "source": sources,
    }

    # --- Activity ----------------------------------------------------------
    emails = sorted({(m["email"] or "").strip().lower() for m in members if m["email"]})
    out["activity"] = campaign_activity(emails)

    # --- Deals and ROI -----------------------------------------------------
    out["deals"] = campaign_deals(record_id, basis)
    won = out["deals"]["won_value"]
    out["roi"] = {
        "budget": budget,
        "won_value": won,
        # None, not 0: "no budget recorded" and "budget of zero" are different
        # statements, and dividing by the second one is undefined anyway.
        "net": (won - budget) if budget is not None else None,
        "roi_pct": ((won - budget) / budget * 100) if budget else None,
        "basis": basis,
    }
    return out


def campaign_activity(emails):
    """contact_activity_log rows for these emails.

    Returns `available: False` on a lookup failure so the page can say "could
    not read" rather than rendering an empty feed, which would be
    indistinguishable from "nothing has happened".
    """
    if not emails:
        return {"available": True, "rows": [], "matched_emails": 0,
                "note": "No targets with email addresses, so nothing to join against."}

    con = md_connection()
    try:
        placeholders = ",".join("?" for _ in emails)
        cur = con.execute(
            f"""SELECT source, event_type, contact_email, timestamp, details
                FROM {MOTHERDUCK_DB}.main.contact_activity_log
                WHERE lower(contact_email) IN ({placeholders})
                ORDER BY timestamp DESC""",
            emails,
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        return {"available": False, "rows": [], "reason": str(e)}
    finally:
        con.close()

    for r in rows:
        if hasattr(r.get("timestamp"), "isoformat"):
            r["timestamp"] = r["timestamp"].isoformat()

    return {
        "available": True,
        "rows": rows,
        "matched_emails": len({r["contact_email"] for r in rows}),
        "queried_emails": len(emails),
    }


def campaign_deals(campaign_record_id, basis):
    """Deals linked to this campaign, and the Won total on the chosen basis.

    Every deal is paged and filtered here rather than server-side: the filter
    syntax for a record-reference attribute is the fragile part, and the deal
    count is small. If Deals grows past a few thousand this should become an
    Attio-side filter on `campaign_4`.
    """
    fields = VALUE_BASES[basis]
    linked = []
    for rec in _query_all(f"{ATTIO_BASE}/objects/deals/records/query"):
        v = rec.get("values", {})
        if _scalar(v, D_CAMPAIGN) != campaign_record_id:
            continue
        amounts = {f: _money(v, f) for f in (D_VALUE, D_ARR, D_IMPL)}
        linked.append({
            "record_id": rec["id"]["record_id"],
            "name": _scalar(v, "name"),
            "stage": _scalar(v, D_STAGE),
            "is_won": _scalar(v, D_STAGE) == WON_STAGE,
            "amounts": amounts,
            "basis_value": sum(amounts[f] or 0.0 for f in fields),
        })

    won = [d for d in linked if d["is_won"]]
    return {
        "count": len(linked),
        "won_count": len(won),
        "won_value": sum(d["basis_value"] for d in won),
        "open_value": sum(d["basis_value"] for d in linked if not d["is_won"]),
        "basis": basis,
        "basis_fields": list(fields),
        # Surfaced so the page can warn rather than silently reporting 0.00:
        # `value` is unset on every deal in the workspace, so a run on the
        # deal_value_field basis is a total that means nothing.
        "basis_all_empty": bool(linked) and all(
            (d["basis_value"] or 0) == 0 for d in linked
        ),
        "deals": sorted(linked, key=lambda d: -d["basis_value"]),
    }
