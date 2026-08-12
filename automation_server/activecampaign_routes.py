"""
activecampaign_routes.py

ActiveCampaign webhook route. Automation 15 ("Attio Marketing Contact" tag
added *and* removed -- both triggers now point at this one URL) POSTs here,
and this keeps the matching Attio People record's Active Marketing Contact
flag, Prospect Path, and "Current Marketing Prospects in ActiveCampaign"
list membership in sync.

## Telling add from remove

Both triggers live in the same automation, so `seriesid` is 15 on either
one and cannot distinguish them. The signal is `contact[tags]`, which lists
only the tags *currently applied* to the contact: the marketing tag present
means added, absent means removed.

That makes an absent `contact[tags]` key genuinely ambiguous -- "no tags" and
"field not mapped in the webhook step" would otherwise look identical, and
guessing wrong un-flags a contact who is still in outreach. So a payload with
no `contact[tags]` key at all is logged and ignored, while a payload carrying
an empty one is a real removal. If every event starts coming back as
`no_tags_field`, the tags field is not mapped on the AC webhook action.

`seriesid` also gates the route: a payload from any other automation is
logged and ignored rather than acted on, so pointing a sixth automation at
this URL by mistake shows up in the logs instead of writing to Attio.

## Attio lookup

`contact[fields][attio_record_id]` is the primary key -- AC's own custom
field, so it survives a contact changing their email address, which the old
email-only lookup did not. Email match is the fallback for contacts that
predate the field or have it blank. A record id that no longer resolves
(deleted or merged record) also falls back to email rather than failing.

## Attribute slugs

Verified against the live workspace, not guessed:
  active_marketing_contact   checkbox   (NOT `marketing_contact` -- no such
                                         attribute exists; the old slug meant
                                         every PATCH here was 400ing)
  prospect_path              status
  last_path_change_date      date
"""

import os
from datetime import datetime, timezone

import requests
from fastapi import APIRouter, Request

import webhook_log

router = APIRouter()

WEBHOOK_SOURCE = "activecampaign"

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"

# Attio list "Current Marketing Prospects in ActiveCampaign"
# (api_slug active_campaign_target_list). This is an Attio list UUID, which is
# a different identifier from ACTIVECAMPAIGN_LIST_ID -- that one is AC's own
# numeric list id, used by scripts/outreach_rotation.py against AC's API.
ATTIO_AC_LIST_ID = os.environ.get(
    "ATTIO_AC_TARGET_LIST_ID", "e65a24d3-711f-410c-94aa-83a4738aaad6"
)

# Automation 15. Set to empty to accept any automation (not recommended).
AC_SERIES_ID = os.environ.get("ACTIVECAMPAIGN_SERIES_ID", "15")
AC_MARKETING_TAG = os.environ.get("ACTIVECAMPAIGN_MARKETING_TAG", "Attio Marketing Contact")

HTTP_TIMEOUT = 30


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_find_person_by_email(email):
    resp = requests.post(
        f"{ATTIO_BASE}/objects/people/records/query",
        headers=attio_headers(),
        json={"filter": {"email_addresses": {"email_address": {"$eq": email}}}, "limit": 1},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return data[0]["id"]["record_id"] if data else None


def attio_person_exists(record_id):
    """Confirm a record id from AC still resolves. Returns False on 404 so the
    caller can fall back to email -- a stale id in AC (record since deleted or
    merged away) must not stop the sync."""
    try:
        resp = requests.get(
            f"{ATTIO_BASE}/objects/people/records/{record_id}",
            headers=attio_headers(),
            timeout=HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        print(f"AC webhook: Attio lookup for record {record_id} failed ({e})")
        return False
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


def attio_patch_person(record_id, values):
    resp = requests.patch(
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": values}},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()


def attio_add_to_list(record_id):
    """Add the person to the AC target list.

    A contact re-tagged after a previous removal is the normal case, not an
    error, so an already-present entry (409) is treated as success. Anything
    else raises -- a silently dropped list add was invisible before.
    """
    if not ATTIO_AC_LIST_ID:
        print("AC webhook: ATTIO_AC_TARGET_LIST_ID unset -- skipping list add")
        return False
    resp = requests.post(
        f"{ATTIO_BASE}/lists/{ATTIO_AC_LIST_ID}/entries",
        headers=attio_headers(),
        json={"data": {"parent_record_id": record_id, "parent_object": "people"}},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 409:
        return True
    resp.raise_for_status()
    return True


def tags_from_payload(payload):
    """Return (tags, field_was_present).

    AC sends `contact[tags]` as a comma-separated string; some webhook
    configurations send indexed keys instead. Both are handled. The second
    element distinguishes "no tags" from "field not mapped" -- see module
    docstring for why that distinction decides whether we write at all.
    """
    raw = payload.get("contact[tags]")
    if raw is not None:
        return [t.strip() for t in str(raw).split(",") if t.strip()], True

    indexed = [v for k, v in payload.items() if k.startswith("contact[tags][")]
    if indexed:
        return [str(v).strip() for v in indexed if str(v).strip()], True

    return [], False


def resolve_attio_record(payload):
    """(record_id, how_it_was_found). Custom field first, email as fallback."""
    record_id = (payload.get("contact[fields][attio_record_id]") or "").strip()
    if record_id and attio_person_exists(record_id):
        return record_id, "attio_record_id"

    email = payload.get("contact[email]") or payload.get("email")
    if not email:
        return None, "no_email"

    found = attio_find_person_by_email(email.strip())
    if not found:
        return None, "no_match"
    return found, "email_fallback_stale_id" if record_id else "email"


@router.post("/webhooks/activecampaign")
async def activecampaign_webhook(request: Request):
    form = await request.form()
    payload = dict(form)
    print(f"AC webhook payload: {payload}")

    series_id = str(payload.get("seriesid", "")).strip()
    if AC_SERIES_ID and series_id != AC_SERIES_ID:
        print(f"AC webhook: ignoring seriesid {series_id!r} (expected {AC_SERIES_ID!r})")
        webhook_log.log_event(
            WEBHOOK_SOURCE, "ignored", "unexpected_seriesid",
            detail=f"seriesid={series_id!r}, expected {AC_SERIES_ID!r}",
        )
        return {"status": "ignored", "reason": "unexpected seriesid", "seriesid": series_id}

    tags, tags_present = tags_from_payload(payload)
    if not tags_present:
        # Not a removal -- an unmappable payload. See module docstring.
        print("AC webhook: payload carries no contact[tags] field; cannot infer add vs remove")
        webhook_log.log_event(
            WEBHOOK_SOURCE, "ignored", "no_tags_field",
            detail="contact[tags] not mapped on the AC webhook action",
        )
        return {"status": "ignored", "reason": "no_tags_field"}

    tag_applied = any(t.lower() == AC_MARKETING_TAG.lower() for t in tags)

    record_id, matched_by = resolve_attio_record(payload)
    if not record_id:
        webhook_log.log_event(
            WEBHOOK_SOURCE, "ignored", matched_by,
            detail=f"tag_applied={tag_applied}",
        )
        return {"status": "ignored", "reason": matched_by}

    today = datetime.now(timezone.utc).date().isoformat()

    if tag_applied:
        attio_patch_person(record_id, {
            "active_marketing_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": today}],
        })
        listed = attio_add_to_list(record_id)
        action = "added"
    else:
        attio_patch_person(record_id, {
            "active_marketing_contact": [{"value": False}],
            "prospect_path": [{"value": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": today}],
        })
        # List removal stays with the Attio-native workflow (Active Marketing
        # Contact = false -> remove from list), not duplicated here.
        listed = False
        action = "removed"

    # After the Attio write, not before: a logged 'ok' has to mean the PATCH
    # actually landed. attio_patch_person raises on failure, which propagates
    # to a 500 -- so a failed sync leaves no 'ok' row rather than a false one.
    webhook_log.log_event(
        WEBHOOK_SOURCE, "ok", action,
        record_id=record_id, matched_by=matched_by,
        detail=f"list_updated={listed}",
    )

    return {
        "status": "ok",
        "action": action,
        "record_id": record_id,
        "matched_by": matched_by,
        "list_updated": listed,
    }
