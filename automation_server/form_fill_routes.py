"""
form_fill_routes.py

ActiveCampaign form-fill receiver. Five AC Automations -- one per tracked
form -- POST here, and this does exactly two things:

  1. appends a row to MotherDuck contact_activity_log
  2. appends the matching option to the Attio person's "Form Filled" multiselect

One direction only, AC -> Attio. Nothing here starts an Attio workflow or
sequence, and nothing is written back to AC, so there is no loop to prevent.
Filled-form contacts keep getting AC's existing marketing email exactly as
they do today; this route only makes that history visible on the Attio side.

The AC automation name is NOT the Attio option title
----------------------------------------------------
The Attio field predates this pipeline and its options are named for the
offer, not for the automation that fires. TRACKED_FORMS below is the whole
translation layer -- it is deliberately explicit rather than derived, because
three of the five pairs don't share a word.

The field also carries "ROI Calculator" and "Request a Demo", which belong to
other pipelines (ROI Calculator flows through the separate Lovable/Supabase
path). This route never writes them and never clears them -- PATCH only adds.

Prerequisite, manual -- no API creates them:
  - Attio: the "Form Filled" options "Get the Guide" and "RX Contact Us" are
    still being added by hand. Until they exist, a fill on either of those two
    forms logs its MotherDuck row and comes back with attio_error rather than
    silently doing nothing. The other three work today.
  - ActiveCampaign: contact custom field "Form Filled" (not read by this
    route -- AC-side bookkeeping only)

Form name comes from the URL, not the payload
---------------------------------------------
AC's native Webhook action posts contact fields; it has no built-in "which
form was this" value. Each of the five automations gets its own webhook
block, so the reliable place to put the form name is the URL that block
points at:

    https://<hub>/webhooks/ac-form-fill?form=Free%20Trial%20Sign%20Up

A form / form_name / form[name] field in the body is accepted as a fallback,
but the query string is what to configure.

Auth
----
Set AC_WEBHOOK_TOKEN on this service and append &token=<value> to each of the
five webhook URLs. If the variable is unset the route serves unauthenticated
-- the same posture /webhooks/activecampaign has today -- but this one writes
to MotherDuck and mutates Attio on every call, so it is worth setting.
"""

import json
import os
import secrets
from datetime import datetime, timezone

import duckdb
import requests
from fastapi import APIRouter, HTTPException, Request

from activecampaign_routes import attio_find_person_by_email, attio_headers

router = APIRouter()

ATTIO_BASE = "https://api.attio.com/v2"
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")
AC_WEBHOOK_TOKEN = os.environ.get("AC_WEBHOOK_TOKEN", "")

# Multiselect attribute on People. Verified against the live workspace:
# title "Form Filled", attribute_id 021dbd7a-70e3-43e2-9d94-b7f7ea84e793.
FILLED_FORM_SLUG = "form_filled"

# AC automation name -> Attio option title. Both sides are exact strings:
# Attio resolves select options by title and 400s on anything that doesn't
# match one, so neither column can drift without breaking a write.
TRACKED_FORMS = {
    "Free Trial Sign Up":   "Free Trial",
    "Newsletter Signup":    "Newsletter",
    "Get the Guide":        "Get the Guide",
    "Send Us a Message":    "Contact Us",
    "RX Send Us a Message": "RX Contact Us",
}

# Accept either side of the mapping in ?form=. The webhook URLs should carry
# the AC automation name, but a URL built from the Attio label instead still
# resolves rather than logging an "unknown form" row. AC names are applied
# last so they win any overlap.
_FORMS_BY_KEY = {opt.casefold(): name for name, opt in TRACKED_FORMS.items()}
_FORMS_BY_KEY.update({name.casefold(): name for name in TRACKED_FORMS})

ACTIVITY_SOURCE = "activecampaign"
ACTIVITY_EVENT_TYPE = "form_fill"


def _check_token(token: str | None):
    if not AC_WEBHOOK_TOKEN:
        return  # unset == open, deliberately; see module docstring
    if not token or not secrets.compare_digest(token, AC_WEBHOOK_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


def md_connection():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")


def ensure_activity_log_table(con):
    """`details` is a JSON text blob rather than typed columns on purpose:
    Social and Conference follow-ups are meant to land in this same table
    under a different `source`, and whatever they need to carry goes in
    there instead of a migration."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MOTHERDUCK_DB}.main.contact_activity_log (
            source VARCHAR,
            event_type VARCHAR,
            contact_email VARCHAR,
            "timestamp" TIMESTAMP,
            details VARCHAR
        )
    """)


def log_activity(source, event_type, contact_email, details):
    con = md_connection()
    try:
        ensure_activity_log_table(con)
        con.execute(
            f"""INSERT INTO {MOTHERDUCK_DB}.main.contact_activity_log
                (source, event_type, contact_email, "timestamp", details)
                VALUES (?, ?, ?, ?, ?)""",
            [source, event_type, contact_email, datetime.now(timezone.utc), details],
        )
    finally:
        con.close()


def attio_append_filled_form(record_id: str, option_title: str):
    """PATCH, not PUT, is what makes this an append: Attio's PATCH record
    endpoint adds the supplied multiselect values to whatever is already on
    the record, where PUT would replace them. That is also what keeps this
    route's hands off "ROI Calculator" / "Request a Demo" -- options it
    never sends are never touched.

    `option_title` is the Attio side of TRACKED_FORMS, not the AC automation
    name. Passing the automation name here would 400.
    """
    resp = requests.patch(
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": {FILLED_FORM_SLUG: [{"option": option_title}]}}},
        timeout=30,
    )
    resp.raise_for_status()


def _resolve_form(query_form: str | None, payload: dict) -> str | None:
    """Raw form name from the URL if present, else from the body."""
    return (
        query_form
        or payload.get("form")
        or payload.get("form_name")
        or payload.get("form[name]")
        or payload.get("contact[form]")
    )


def _resolve_email(payload: dict) -> str | None:
    return payload.get("contact[email]") or payload.get("email")


@router.post("/webhooks/ac-form-fill")
async def ac_form_fill_webhook(
    request: Request,
    form: str | None = None,
    token: str | None = None,
):
    _check_token(token)

    # AC's Webhook action posts form-encoded. Accept JSON too so a curl test
    # doesn't need to imitate that.
    try:
        payload = dict(await request.form())
    except Exception:
        payload = {}
    if not payload:
        try:
            body = await request.json()
            payload = body if isinstance(body, dict) else {}
        except Exception:
            payload = {}

    print(f"AC form-fill payload: form={form!r} {payload}")

    raw_form = _resolve_form(form, payload)
    email = _resolve_email(payload)
    canonical_form = _FORMS_BY_KEY.get((raw_form or "").strip().casefold())
    attio_option = TRACKED_FORMS.get(canonical_form) if canonical_form else None

    result = {
        "status": "ok",
        "form": canonical_form or raw_form,
        "attio_option": attio_option,
        "form_recognized": canonical_form is not None,
        "contact_email": email,
        "logged": False,
        "attio_updated": False,
    }

    # Only bother looking the person up for a form we can actually write.
    # An unrecognized name would 400 at Attio ("no such select option"), and
    # the lookup would be a wasted round trip.
    record_id = None
    if canonical_form and email:
        try:
            record_id = attio_find_person_by_email(email)
        except requests.exceptions.RequestException as e:
            result["attio_lookup_error"] = str(e)

    details = json.dumps(
        {
            "form_name": canonical_form or raw_form,
            "attio_option": attio_option,
            "form_recognized": canonical_form is not None,
            "attio_record_id": record_id,
            "raw_payload": payload,
        },
        default=str,
    )

    # The log write and the Attio patch are independent and both matter, so
    # neither failure is allowed to skip the other. A row with no Attio match
    # is the useful signal that this person isn't in the CRM yet.
    try:
        log_activity(ACTIVITY_SOURCE, ACTIVITY_EVENT_TYPE, email, details)
        result["logged"] = True
    except Exception as e:
        result["status"] = "partial"
        result["log_error"] = str(e)

    if record_id:
        try:
            attio_append_filled_form(record_id, attio_option)
            result["attio_updated"] = True
            result["attio_record_id"] = record_id
        except requests.exceptions.RequestException as e:
            result["status"] = "partial"
            detail = e.response.text[:300] if e.response is not None else str(e)
            result["attio_error"] = detail

    if not raw_form:
        result["status"] = "partial"
        result["reason"] = (
            "no form name in the query string or the payload -- add "
            "?form=<Form Name> to this automation's webhook URL"
        )
    elif not canonical_form:
        result["status"] = "partial"
        result["reason"] = (
            f"'{raw_form}' is not one of the five tracked forms; logged, but "
            "no Attio update. Fix the ?form= value or add it to TRACKED_FORMS. "
            f"Known: {', '.join(TRACKED_FORMS)}"
        )
    elif not email:
        result["status"] = "partial"
        result["reason"] = "no email in payload -- check the AC webhook field mapping"
    elif not record_id and "attio_lookup_error" not in result:
        result["status"] = "partial"
        result["reason"] = "no matching Attio person for this email"

    return result
