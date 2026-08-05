"""
smartlead_routes.py

Smartlead webhook route for the nonprofit-crm automation server. Handles
EMAIL_SENT, SEQUENCE_COMPLETED, EMAIL_REPLIED, EMAIL_BOUNCED, and
LEAD_UNSUBSCRIBED events and keeps the matching Attio People record's touch
count, Prospect Path, and Cold Outreach Contact flag in sync.

Registered in main.py via app.include_router(smartlead_router).

Requires ATTIO_API_KEY (and optionally SMARTLEAD_WEBHOOK_SECRET) set as
Railway variables on this service -- see README.md.

Smartlead side: Campaign > Settings > Webhooks > add
  https://<this-service's-railway-domain>/webhooks/smartlead
subscribed to EMAIL_SENT, SEQUENCE_COMPLETED, EMAIL_REPLIED, EMAIL_BOUNCED,
LEAD_UNSUBSCRIBED.
"""

import os
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"
SMARTLEAD_WEBHOOK_SECRET = os.environ.get("SMARTLEAD_WEBHOOK_SECRET")  # optional


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
    resp.raise_for_status()


def increment_touch_count(record_id, field_name):
    current = attio_get_person(record_id)
    existing = current["values"].get(field_name, [{}])
    count = (existing[0].get("value") or 0) if existing else 0
    attio_patch_person(record_id, {field_name: [{"value": count + 1}]})


@router.post("/webhooks/smartlead")
async def smartlead_webhook(request: Request):
    if SMARTLEAD_WEBHOOK_SECRET:
        sig = request.headers.get("x-smartlead-signature")
        if sig != SMARTLEAD_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="invalid signature")

    payload = await request.json()
    event_type = payload.get("event_type")
    custom_fields = payload.get("lead", {}).get("custom_fields", {}) or payload.get("custom_fields", {})
    record_id = custom_fields.get("attio_record_id")

    if not record_id:
        print(f"Smartlead webhook with no attio_record_id: {event_type}")
        return {"status": "ignored"}

    today = datetime.now(timezone.utc).date().isoformat()

    if event_type == "EMAIL_SENT":
        increment_touch_count(record_id, "smartlead_touch_count")

    elif event_type == "SEQUENCE_COMPLETED":
        attio_patch_person(record_id, {
            "cold_outreach_contact": [{"value": False}],
            "prospect_path": [{"value": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "EMAIL_REPLIED":
        attio_patch_person(record_id, {
            "prospect_path": [{"value": "Engaged"}],
            "cold_outreach_contact": [{"value": False}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "EMAIL_BOUNCED":
        attio_patch_person(record_id, {
            "cold_outreach_contact": [{"value": False}],
            "do_not_migrate": [{"value": True}],
            "exclude_reason": [{"value": "Smartlead hard bounce"}],
            "last_path_change_date": [{"value": today}],
        })

    elif event_type == "LEAD_UNSUBSCRIBED":
        attio_patch_person(record_id, {
            "cold_outreach_contact": [{"value": False}],
            "prospect_path": [{"value": "Not Interested"}],
            "last_path_change_date": [{"value": today}],
        })

    else:
        print(f"Unhandled Smartlead event type: {event_type}")

    return {"status": "ok"}
