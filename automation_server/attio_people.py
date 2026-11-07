"""
attio_people.py

Attio People lookups shared by the AC engagement and form-fill webhooks.
"""

import os

import http_client

ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
ATTIO_BASE = "https://api.attio.com/v2"


def attio_headers():
    return {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}


def attio_find_person_by_email(email):
    resp = http_client.post(
        f"{ATTIO_BASE}/objects/people/records/query",
        headers=attio_headers(),
        json={"filter": {"email_addresses": {"email_address": {"$eq": email}}}, "limit": 1},
    )
    resp.raise_for_status()
    data = resp.json().get("data", [])
    return data[0]["id"]["record_id"] if data else None
