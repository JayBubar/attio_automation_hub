"""
smartlead_routes.py

Smartlead webhook route for the nonprofit-crm automation server. Handles
EMAIL_SENT, SEQUENCE_COMPLETED, EMAIL_REPLIED, EMAIL_BOUNCED,
LEAD_UNSUBSCRIBED, and LEAD_CATEGORY_UPDATED events and keeps the matching
Attio People record's touch count, Prospect Path, and Cold Outreach Contact
flag in sync.

Registered in main.py via app.include_router(smartlead_router).

Requires ATTIO_API_KEY (and optionally SMARTLEAD_WEBHOOK_SECRET) set as
Railway variables on this service -- see README.md.

Smartlead side: Campaign > Settings > Webhooks > add
  https://<this-service's-railway-domain>/webhooks/smartlead
Two webhook entries land on this same route/URL:
  1. EMAIL_SENT, SEQUENCE_COMPLETED, EMAIL_REPLIED, EMAIL_BOUNCED,
     LEAD_UNSUBSCRIBED (the original wiring)
  2. LEAD_CATEGORY_UPDATED (added for the AI reply-categorization feature --
     see the category sets below for the category -> Path mapping and the
     reasoning per category)

LEAD_CATEGORY_UPDATED payload shape (per Smartlead's webhook reference):
  top-level "category" (string, e.g. "Interested") plus a redundant
  lead_data.category.name. This code reads the top-level field and falls
  back to the nested one so a Smartlead payload-format change doesn't
  silently break category routing.
"""

import os
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"
SMARTLEAD_WEBHOOK_SECRET = os.environ.get("SMARTLEAD_WEBHOOK_SECRET")  # optional

# ---------------------------------------------------------------------------
# LEAD_CATEGORY_UPDATED routing
#
# These are the 9 "Active AI Categories" configured in Smartlead as of
# 2026-08-10. Deliberately conservative: only "Interested" / "Meeting
# Request" / "Information Request" (clear positive signal) and "Not
# Interested" / "Do Not Contact" (clear negative signal) move Prospect Path
# automatically. "Wrong Person" and "Uncategorizable by Ai" are data-quality
# problems, not funnel signals -- they get flagged via exclude_reason for a
# human to look at rather than silently moving Path in either direction (the
# Review Queue list this should really feed doesn't exist in Attio yet --
# see Contact/Company Review List Workflow in the tracker). "Out Of Office"
# is a transient auto-reply, not a lead signal -- no-op. "Sender Originated
# Bounce" indicates a problem with our sending infra, not the lead, so it's
# logged for visibility rather than acted on against the lead's record.
# ---------------------------------------------------------------------------

ENGAGED_CATEGORIES = {"Interested", "Meeting Request", "Information Request"}
NOT_INTERESTED_CATEGORIES = {"Not Interested"}
DO_NOT_CONTACT_CATEGORIES = {"Do Not Contact"}
REVIEW_FLAG_CATEGORIES = {"Wrong Person", "Uncategorizable by Ai"}
NO_OP_CATEGORIES = {"Out Of Office", "Sender Originated Bounce"}


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_get_person(record_id):
    resp = requests.get(f"{ATTIO_BASE}/objects/people/records/{record_id}", headers=attio_headers())
    resp.raise_for_status()
    return resp.json()["data"]


def attio_patch_person(record_id, values):
    resp = requests.patch(
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": values}},
    )
    if not resp.ok:
        # Attio names the offending attribute in the body; raise_for_status()
        # throws that away and leaves a bare "400 Client Error". Log it first.
        print(f"Smartlead webhook: Attio PATCH {record_id} failed "
              f"{resp.status_code}: {resp.text[:1000]}")
        print(f"Smartlead webhook: rejected payload was {values}")
    resp.raise_for_status()


def increment_touch_count(record_id, field_name):
    current = attio_get_person(record_id)
    existing = current["values"].get(field_name, [{}])
    count = (existing[0].get("value") or 0) if existing else 0
    attio_patch_person(record_id, {field_name: [{"value": count + 1}]})


def extract_record_id(payload):
    """attio_record_id can show up in a few different places depending on
    event type -- EMAIL_SENT/REPLY-style events nest it under "lead", while
    LEAD_CATEGORY_UPDATED nests it under "lead_data". Check all of them."""
    for container_key in ("lead", "lead_data"):
        custom_fields = payload.get(container_key, {}).get("custom_fields", {})
        if custom_fields.get("attio_record_id"):
            return custom_fields["attio_record_id"]
    return payload.get("custom_fields", {}).get("attio_record_id")


def extract_category(payload):
    """Top-level "category" is the documented field; lead_data.category.name
    is a redundant nested copy. Prefer top-level, fall back to nested."""
    if payload.get("category"):
        return payload["category"]
    return (payload.get("lead_data", {}).get("category") or {}).get("name")


@router.post("/webhooks/smartlead")
async def smartlead_webhook(request: Request):
    if SMARTLEAD_WEBHOOK_SECRET:
        sig = request.headers.get("x-smartlead-signature")
        if sig != SMARTLEAD_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="invalid signature")

    payload = await request.json()
    event_type = payload.get("event_type")
    record_id = extract_record_id(payload)

    if not record_id:
        print(f"Smartlead webhook with no attio_record_id: {event_type}")
        return {"status": "ignored"}

    today = datetime.now(timezone.utc).date().isoformat()

    if event_type == "EMAIL_SENT":
        increment_touch_count(record_id, "smartlead_touch_count")

    elif event_type == "SEQUENCE_COMPLETED":
        attio_patch_person(record_id, {
            "active_cold_outreach_contact": [{"value": False}],
            "prospect_path": [{"status": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "EMAIL_REPLIED":
        attio_patch_person(record_id, {
            "prospect_path": [{"status": "Engaged"}],
            "active_cold_outreach_contact": [{"value": False}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "EMAIL_BOUNCED":
        # !! BROKEN, PENDING A DECISION -- do not assume this works. !!
        # Neither `do_not_migrate` nor `exclude_reason` exists on People in the
        # live workspace (checked against all 59 attributes, archived included,
        # on 2026-08-13). Attio rejects a PATCH naming an unknown attribute
        # *entirely*, so this call writes nothing at all -- the suppression flag
        # and the two valid fields beside it are all lost together. Same failure
        # mode already documented for `ac_contact_id` in outreach_rotation.py.
        # Either create the two attributes in Attio or move suppression onto an
        # existing field; see README "Suppression fields that don't exist".
        attio_patch_person(record_id, {
            "active_cold_outreach_contact": [{"value": False}],
            "do_not_migrate": [{"value": True}],
            "exclude_reason": [{"value": "Smartlead hard bounce"}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "LEAD_UNSUBSCRIBED":
        attio_patch_person(record_id, {
            "active_cold_outreach_contact": [{"value": False}],
            "prospect_path": [{"status": "Not Interested"}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "LEAD_CATEGORY_UPDATED":
        category = extract_category(payload)

        if category in ENGAGED_CATEGORIES:
            attio_patch_person(record_id, {
                "prospect_path": [{"status": "Engaged"}],
                "active_cold_outreach_contact": [{"value": False}],
                "last_path_change_date": [{"value": today}],
            })

        elif category in NOT_INTERESTED_CATEGORIES:
            attio_patch_person(record_id, {
                "prospect_path": [{"status": "Not Interested"}],
                "active_cold_outreach_contact": [{"value": False}],
                "last_path_change_date": [{"value": today}],
            })

        elif category in DO_NOT_CONTACT_CATEGORIES:
            attio_patch_person(record_id, {
                "prospect_path": [{"status": "Not Interested"}],
                "active_cold_outreach_contact": [{"value": False}],
                "do_not_migrate": [{"value": True}],
                "exclude_reason": [{"value": "Smartlead: Do Not Contact"}],
                "last_path_change_date": [{"value": today}],
            })

        elif category in REVIEW_FLAG_CATEGORIES:
            # Doesn't move Path in either direction -- this is a
            # data-quality problem (bad match / AI couldn't tell), not a
            # funnel signal. Flags for a human via exclude_reason since the
            # dedicated Review Queue list doesn't exist in Attio yet.
            attio_patch_person(record_id, {
                "exclude_reason": [{"value": f"Smartlead category: {category} -- needs review"}],
            })

        elif category in NO_OP_CATEGORIES:
            print(f"Smartlead category '{category}' for record {record_id} -- no action (transient/infra, not a lead signal)")

        else:
            # New/renamed category in Smartlead that isn't in any bucket
            # above -- don't guess, just log so it gets noticed and mapped.
            print(f"Unmapped Smartlead category '{category}' for record {record_id}")

    else:
        print(f"Unhandled Smartlead event type: {event_type}")

    return {"status": "ok"}