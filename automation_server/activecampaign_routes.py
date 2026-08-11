"""
activecampaign_routes.py

ActiveCampaign webhook route. Two AC Automations (tag "Attio Marketing
Contact" added / removed) POST here, and this keeps the matching Attio
People record's Marketing Contact flag, Prospect Path, and Active Campaign
Target List membership in sync.

ASSUMPTION TO VERIFY: AC's native Automation "Webhook" action sends
form-encoded data, and the exact field names depend on what you map in the
automation's webhook step. This assumes contact[email] and a `type` field
(contact_tag_added / contact_tag_removed) -- confirm both against a real
test payload once the AC Automations are built, and adjust the field names
below if they don't match. Log the raw payload on first run to check.

No AC contact -> Attio record ID mapping exists yet (contacts_crosswalk
was empty at last check), so this looks the person up by email instead,
per the established fallback pattern.
"""

import os
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, Request

router = APIRouter()

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"
AC_LIST_ID = os.environ.get("ACTIVECAMPAIGN_LIST_ID", "")  # Active Campaign Target List


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_find_person_by_email(email):
    resp = requests.post(
        f"{ATTIO_BASE}/objects/people/records/query",
        headers=attio_headers(),
        json={"filter": {"email_addresses": {"email_address": {"$eq": email}}}, "limit": 1},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return data[0]["id"]["record_id"] if data else None


def attio_patch_person(record_id, values):
    resp = requests.patch(
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": values}},
    )
    resp.raise_for_status()


def attio_add_to_list(record_id):
    if not AC_LIST_ID:
        return
    requests.post(
        f"{ATTIO_BASE}/lists/{AC_LIST_ID}/entries",
        headers=attio_headers(),
        json={"data": {"parent_record_id": record_id, "parent_object": "people"}},
    )


@router.post("/webhooks/activecampaign")
async def activecampaign_webhook(request: Request):
    form = await request.form()
    payload = dict(form)
    print(f"AC webhook payload: {payload}")  # first-run: confirm real field names here

    event_type = payload.get("type", "")
    email = payload.get("contact[email]") or payload.get("email")

    if not email:
        return {"status": "ignored", "reason": "no email in payload"}

    record_id = attio_find_person_by_email(email)
    if not record_id:
        return {"status": "ignored", "reason": "no matching Attio record"}

    today = datetime.now(timezone.utc).date().isoformat()

    if event_type == "contact_tag_added":
        attio_patch_person(record_id, {
            "marketing_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": today}],
        })
        attio_add_to_list(record_id)

    elif event_type == "contact_tag_removed":
        attio_patch_person(record_id, {
            "marketing_contact": [{"value": False}],
            "prospect_path": [{"value": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": today}],
        })
        # List removal is handled by the Attio-native workflow (Marketing
        # Contact = false -> remove from list), not duplicated here.

    else:
        print(f"Unhandled AC event type: {event_type}")

    return {"status": "ok"}
