"""
outreach_rotation.py

Daily job that rotates contacts through ActiveCampaign and Smartlead outreach,
keeping Attio as the source of truth. Run once per day (Windows Task Scheduler
or manually via `py outreach_rotation.py`).

What it does, in order:
  1. Reads current capacity for AC (5,000 contact cap) and Smartlead
     (per-campaign max_leads_per_day, read live from the Smartlead API).
  2. Evicts stale contacts from each channel to free up room:
       - Smartlead: sequence exhausted with no reply after N days
       - AC: no engagement after N days on the tag
     Evicted contacts get pulled off the channel, flagged off, and moved to
     Prospect Path "Cold/Retry Pending" in Attio.
  3. Fills the freed + any open headroom with new contacts pulled from Attio's
     "Never Contacted" pool.
  4. Pushes the new batch to the channel and immediately updates Attio state:
     Prospect Path -> "In Outreach", channel flag -> True, list membership,
     Last Path Change Date -> today.

Idempotency: every run logs pushed record IDs + channel + date to a MotherDuck
checkpoint table (`outreach_rotation_log`) before touching any API, so a
crashed/re-run script does not double-push. All bulk Attio operations use the
two-phase pattern: collect IDs first with a stable query, then process.

Environment variables required (set with `set VAR=value`, no quotes):
  ATTIO_API_KEY
  ACTIVECAMPAIGN_API_URL       e.g. https://raisetell.api-us1.com
  ACTIVECAMPAIGN_API_KEY
  ACTIVECAMPAIGN_LIST_ID       "Active Campaign Target List" numeric list id
  ACTIVECAMPAIGN_TAG_ID        "Attio Marketing Contact" tag id (3)
  ACTIVECAMPAIGN_MAX_CONTACTS  e.g. 5000
  SMARTLEAD_API_KEY
  SMARTLEAD_CAMPAIGN_ID        the campaign your 15 warmed inboxes send from
  MOTHERDUCK_TOKEN
  MOTHERDUCK_DATABASE          e.g. hubspot_email_archive
  DRY_RUN                      "true" to log actions without calling write APIs
"""

import os
import sys
import time
import json
import random
from datetime import datetime, timedelta, timezone

import requests
import duckdb

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"

AC_BASE = os.environ["ACTIVECAMPAIGN_API_URL"].rstrip("/")
AC_API_KEY = os.environ["ACTIVECAMPAIGN_API_KEY"]
AC_LIST_ID = os.environ["ACTIVECAMPAIGN_LIST_ID"]
AC_TAG_ID = os.environ["ACTIVECAMPAIGN_TAG_ID"]
AC_MAX_CONTACTS = int(os.environ.get("ACTIVECAMPAIGN_MAX_CONTACTS", "5000"))

SMARTLEAD_API_KEY = os.environ["SMARTLEAD_API_KEY"]
SMARTLEAD_BASE = "https://server.smartlead.ai/api/v1"
SMARTLEAD_CAMPAIGN_ID = os.environ["SMARTLEAD_CAMPAIGN_ID"]

MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
MOTHERDUCK_DATABASE = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")

DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"

# AC is PAUSED from this script for now -- AC is working the ~4,500 contacts
# already loaded via its own automations. Leave AC_* functions in place below
# so resuming later is a one-line change in main(), but nothing calls them.
AC_PAUSED = True

# Smartlead no longer uses a day-based staleness guess. SEQUENCE_COMPLETED,
# EMAIL_REPLIED, EMAIL_BOUNCED, and LEAD_UNSUBSCRIBED webhooks (see
# webhook_receiver_smartlead_additions.py) evict/flag contacts the instant
# the event happens, so this script only needs to top up open capacity.
# Target standing pool size = max_leads_per_day x sequence length, so the
# campaign always has roughly one full sequence's worth of leads loaded.
SMARTLEAD_SEQUENCE_LENGTH_DAYS = int(os.environ.get("SMARTLEAD_SEQUENCE_LENGTH_DAYS", "10"))

CHECKPOINT_TABLE = "outreach_rotation_log"


# ---------------------------------------------------------------------------
# Retry wrapper (matches your existing exponential backoff pattern)
# ---------------------------------------------------------------------------

def with_backoff(fn, *args, max_attempts=4, **kwargs):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = fn(*args, **kwargs)
            if resp.status_code in (429, 500, 502, 503):
                raise requests.HTTPError(f"transient {resp.status_code}")
            resp.raise_for_status()
            return resp
        except (requests.HTTPError, requests.ConnectionError) as e:
            if attempt == max_attempts:
                raise
            sleep_s = (2 ** attempt) + random.uniform(0, 1)
            print(f"  retry {attempt}/{max_attempts} after error ({e}), sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)


# ---------------------------------------------------------------------------
# MotherDuck helpers (fetchdf() not available -- use description + fetchall)
# ---------------------------------------------------------------------------

def get_duck_conn():
    return duckdb.connect(f"md:{MOTHERDUCK_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")


def fetch_dicts(cursor):
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def ensure_checkpoint_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CHECKPOINT_TABLE} (
            attio_record_id VARCHAR,
            channel VARCHAR,          -- 'activecampaign' or 'smartlead'
            action VARCHAR,           -- 'push' or 'evict'
            run_date DATE,
            created_at TIMESTAMP DEFAULT now()
        )
    """)


def already_processed_today(conn, attio_record_id, channel, action):
    cur = conn.execute(f"""
        SELECT 1 FROM {CHECKPOINT_TABLE}
        WHERE attio_record_id = ? AND channel = ? AND action = ? AND run_date = CURRENT_DATE
    """, [attio_record_id, channel, action])
    return cur.fetchone() is not None


def log_checkpoint(conn, rows):
    """rows: list of (attio_record_id, channel, action) tuples"""
    if DRY_RUN or not rows:
        return
    conn.executemany(f"""
        INSERT INTO {CHECKPOINT_TABLE} (attio_record_id, channel, action, run_date)
        VALUES (?, ?, ?, CURRENT_DATE)
    """, [(r[0], r[1], r[2]) for r in rows])


# ---------------------------------------------------------------------------
# Attio helpers
# ---------------------------------------------------------------------------

def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_query_people(filter_body, limit=500):
    """Two-phase collect: page through with a stable filter, return all IDs + key fields."""
    results = []
    offset = 0
    while True:
        body = {**filter_body, "limit": limit, "offset": offset}
        resp = with_backoff(
            requests.post,
            f"{ATTIO_BASE}/objects/people/records/query",
            headers=attio_headers(),
            json=body,
        )
        data = resp.json().get("data", [])
        if not data:
            break
        results.extend(data)
        if len(data) < limit:
            break
        offset += limit
    return results


def attio_update_person(record_id, values, dry_run_label=""):
    if DRY_RUN:
        print(f"  [DRY RUN] would update {record_id}: {json.dumps(values)} {dry_run_label}")
        return
    with_backoff(
        requests.patch,
        f"{ATTIO_BASE}/objects/people/records/{record_id}",
        headers=attio_headers(),
        json={"data": {"values": values}},
    )


def attio_add_to_list(list_id, record_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would add {record_id} to list {list_id}")
        return
    with_backoff(
        requests.post,
        f"{ATTIO_BASE}/lists/{list_id}/entries",
        headers=attio_headers(),
        json={"data": {"parent_record_id": record_id, "parent_object": "people"}},
    )


def attio_remove_from_list(list_id, entry_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove entry {entry_id} from list {list_id}")
        return
    with_backoff(
        requests.delete,
        f"{ATTIO_BASE}/lists/{list_id}/entries/{entry_id}",
        headers=attio_headers(),
    )


# ---------------------------------------------------------------------------
# ActiveCampaign helpers
# ---------------------------------------------------------------------------

def ac_headers():
    return {"Api-Token": AC_API_KEY, "Content-Type": "application/json"}


def ac_current_tag_count():
    resp = with_backoff(
        requests.get,
        f"{AC_BASE}/api/3/contacts",
        headers=ac_headers(),
        params={"tagid": AC_TAG_ID, "limit": 1},
    )
    return int(resp.json()["meta"]["total"])


def ac_add_contact(email, first_name, last_name):
    if DRY_RUN:
        print(f"  [DRY RUN] would upsert AC contact {email}")
        return "dry-run-id"
    resp = with_backoff(
        requests.post,
        f"{AC_BASE}/api/3/contact/sync",
        headers=ac_headers(),
        json={"contact": {"email": email, "firstName": first_name, "lastName": last_name}},
    )
    return resp.json()["contact"]["id"]

def ac_tag_and_list(contact_id):
    if DRY_RUN:
        return
    with_backoff(requests.post, f"{AC_BASE}/api/3/contactTags", headers=ac_headers(),
                 json={"contactTag": {"contact": contact_id, "tag": AC_TAG_ID}})
    with_backoff(requests.post, f"{AC_BASE}/api/3/contactLists", headers=ac_headers(),
                 json={"contactList": {"list": AC_LIST_ID, "contact": contact_id, "status": 1}})


def ac_remove_tag(contact_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove AC tag from contact {contact_id}")
        return
    with_backoff(requests.delete, f"{AC_BASE}/api/3/contactTags/{contact_id}", headers=ac_headers())


# ---------------------------------------------------------------------------
# Smartlead helpers
# ---------------------------------------------------------------------------

def smartlead_campaign_settings():
    resp = with_backoff(
        requests.get,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}",
        params={"api_key": SMARTLEAD_API_KEY},
    )
    return resp.json()


def smartlead_current_lead_count():
    resp = with_backoff(
        requests.get,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads",
        params={"api_key": SMARTLEAD_API_KEY, "limit": 1},
    )
    return int(resp.json().get("total_leads", 0))


def smartlead_add_leads(leads):
    """leads: list of dicts with email/first_name/last_name/company_name.
    Max 400 per request per Smartlead's API limit."""
    if DRY_RUN:
        print(f"  [DRY RUN] would push {len(leads)} leads to Smartlead campaign {SMARTLEAD_CAMPAIGN_ID}")
        return
    for i in range(0, len(leads), 400):
        chunk = leads[i:i + 400]
        with_backoff(
            requests.post,
            f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads",
            params={"api_key": SMARTLEAD_API_KEY},
            json={"lead_list": chunk},
        )


def smartlead_remove_lead(lead_id):
    if DRY_RUN:
        print(f"  [DRY RUN] would remove Smartlead lead {lead_id} from campaign")
        return
    with_backoff(
        requests.post,
        f"{SMARTLEAD_BASE}/campaigns/{SMARTLEAD_CAMPAIGN_ID}/leads/{lead_id}/remove",
        params={"api_key": SMARTLEAD_API_KEY},
    )


# ---------------------------------------------------------------------------
# Step 2 + 3: Eviction
# ---------------------------------------------------------------------------

def evict_stale_ac(conn):
    """Evict AC contacts whose last touch is older than AC_STALE_DAYS.
    Relies on `AC Touch Count` + `last_interaction` NOT being used for this --
    per your own rule, use MotherDuck activity_date, not Attio's native
    interaction field, for date-based staleness."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=AC_STALE_DAYS)).isoformat()
    filter_body = {
        "filter": {
            "$and": [
                {"marketing_contact": {"$eq": True}},
                {"prospect_path": {"$eq": "In Outreach"}},
                {"last_path_change_date": {"$lt": cutoff}},
            ]
        }
    }
    candidates = attio_query_people(filter_body)
    print(f"AC eviction candidates: {len(candidates)}")

    checkpoint_rows = []
    for rec in candidates:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "activecampaign", "evict"):
            continue
        ac_contact_id = rec["values"].get("ac_contact_id", [{}])[0].get("value")
        if ac_contact_id:
            ac_remove_tag(ac_contact_id)
        attio_update_person(record_id, {
            "marketing_contact": [{"value": False}],
            "prospect_path": [{"value": "Cold/Retry Pending"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }, dry_run_label="(AC evict)")
        checkpoint_rows.append((record_id, "activecampaign", "evict"))

    log_checkpoint(conn, checkpoint_rows)
    return len(checkpoint_rows)


# Eviction is now handled entirely by the SEQUENCE_COMPLETED / EMAIL_REPLIED /
# EMAIL_BOUNCED / LEAD_UNSUBSCRIBED webhook handlers in
# webhook_receiver_smartlead_additions.py -- no day-based sweep needed here.
# This script's only Smartlead job is topping up the standing pool below.


# ---------------------------------------------------------------------------
# Step 4 + 5 + 6: Fill and push
# ---------------------------------------------------------------------------

def never_contacted_batch(size, exclude_flag_field):
    """Pull `size` records from the Never Contacted pool, excluding anyone
    already flagged for this channel. Uses a stable created_at sort so
    pagination/offset doesn't skip records if the pool shrinks mid-run."""
    filter_body = {
        "filter": {
            "$and": [
                {"prospect_path": {"$eq": "Never Contacted"}},
                {exclude_flag_field: {"$eq": False}},
            ]
        },
        "sorts": [{"attribute": "created_at", "direction": "asc"}],
    }
    all_ids = attio_query_people(filter_body, limit=min(size, 500))
    return all_ids[:size]


def push_ac_batch(conn, headroom):
    if headroom <= 0:
        print("AC at capacity, nothing to push.")
        return
    batch = never_contacted_batch(headroom, "marketing_contact")
    print(f"Pushing {len(batch)} contacts to AC (headroom {headroom}).")

    checkpoint_rows = []
    for rec in batch:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "activecampaign", "push"):
            continue
        email = rec["values"].get("email_addresses", [{}])[0].get("email_address")
        first = rec["values"].get("first_name", [{}])[0].get("value", "")
        last = rec["values"].get("last_name", [{}])[0].get("value", "")
        if not email:
            continue

        ac_contact_id = ac_add_contact(email, first, last)
        ac_tag_and_list(ac_contact_id)

        attio_update_person(record_id, {
            "marketing_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
            "ac_contact_id": [{"value": str(ac_contact_id)}],
        }, dry_run_label="(AC push)")
        attio_add_to_list(AC_LIST_ID, record_id)

        checkpoint_rows.append((record_id, "activecampaign", "push"))

    log_checkpoint(conn, checkpoint_rows)
    print(f"AC push complete: {len(checkpoint_rows)} pushed.")


def push_smartlead_batch(conn, headroom):
    if headroom <= 0:
        print("Smartlead at capacity, nothing to push.")
        return
    batch = never_contacted_batch(headroom, "cold_outreach_contact")
    print(f"Pushing {len(batch)} contacts to Smartlead (headroom {headroom}).")

    leads_payload = []
    record_map = {}
    for rec in batch:
        record_id = rec["id"]["record_id"]
        if already_processed_today(conn, record_id, "smartlead", "push"):
            continue
        email = rec["values"].get("email_addresses", [{}])[0].get("email_address")
        if not email:
            continue
        leads_payload.append({
            "email": email,
            "first_name": rec["values"].get("first_name", [{}])[0].get("value", ""),
            "last_name": rec["values"].get("last_name", [{}])[0].get("value", ""),
            "custom_fields": {"attio_record_id": record_id},
        })
        record_map[email] = record_id

    if not leads_payload:
        print("Nothing new to push to Smartlead.")
        return

    smartlead_add_leads(leads_payload)

    checkpoint_rows = []
    for email, record_id in record_map.items():
        attio_update_person(record_id, {
            "cold_outreach_contact": [{"value": True}],
            "prospect_path": [{"value": "In Outreach"}],
            "last_path_change_date": [{"value": datetime.now(timezone.utc).date().isoformat()}],
        }, dry_run_label="(Smartlead push)")
        checkpoint_rows.append((record_id, "smartlead", "push"))

    log_checkpoint(conn, checkpoint_rows)
    print(f"Smartlead push complete: {len(checkpoint_rows)} pushed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"=== Outreach rotation run: {datetime.now(timezone.utc).isoformat()} (DRY_RUN={DRY_RUN}) ===")

    conn = get_duck_conn()
    ensure_checkpoint_table(conn)

    # --- AC: paused. AC is working the ~4,500 already loaded via its own
    # automations; the existing tag<->list webhook sync stays live, this
    # script just doesn't push new contacts or evict for AC right now. ---
    if not AC_PAUSED:
        ac_count = ac_current_tag_count()
        print(f"AC current active marketing contacts: {ac_count} / {AC_MAX_CONTACTS}")
        ac_headroom = AC_MAX_CONTACTS - ac_count
        push_ac_batch(conn, ac_headroom)
    else:
        print("AC push/evict is paused -- skipping.")

    # --- Smartlead: event-driven eviction (see webhook handler), this
    # script only tops up the standing pool to keep inboxes fed. ---
    settings = smartlead_campaign_settings()
    max_per_day = int(settings.get("max_leads_per_day", 0))
    if max_per_day == 0:
        print("WARNING: could not read max_leads_per_day from Smartlead; skipping push.")
    else:
        target_pool = max_per_day * SMARTLEAD_SEQUENCE_LENGTH_DAYS
        current_active = smartlead_current_lead_count()
        sl_headroom = max(0, target_pool - current_active)
        print(f"Smartlead max/day: {max_per_day}, target pool: {target_pool}, "
              f"current active: {current_active}, pushing: {sl_headroom}")
        push_smartlead_batch(conn, sl_headroom)

    print("=== Run complete ===")


if __name__ == "__main__":
    main()
