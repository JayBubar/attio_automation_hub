"""
ac_engagement_rules.py

The pure half of the AC engagement -> Attio bridge: parsing AC's native
webhook payloads, the mail-scanner filter, Download vs AC Activity, and the
next-business-day deadline. No environment, no network, no MotherDuck -- so it
can be tested on its own (tests/test_ac_engagement_rules.py) and tuned without
touching the code that writes to Attio. The routes live in
ac_engagement_routes.py.

## Why a filter at all

Mail-security gateways (Defender, Mimecast, Proofpoint...) open every link in
an inbound email to check it. AC records those as clicks. On 10/29 the
Waterfall Email 1 export had ~17 of 28 "clickers" opening between 13:53 and
13:55, two to six minutes after the send, with click counts in multiples of 4.
That is software, not people, and turning it into rep tasks would bury the
real ones.

## The rules (each one logged by name so it can be tuned)

  multi_link     one contact clicks >= 3 different links in one email within
                 60s. A scanner opens every link; a person opens one or two.
  repeat_link    one contact clicks the same link >= 3 times within 60s.
  domain_burst   >= 3 different contacts at the same organisation domain click
                 the same email within 120s -- one gateway scanning a whole
                 org's copies. Free-mail domains are exempt.
  early_click    a click within 5 minutes of AC finishing the send (needs the
                 campaign's send time, so only applies when the hub can read
                 AC's API).
  tag_bulk       >= 20 contacts get the same tag within 120s -- an import or a
                 bulk edit, not 20 people downloading at once.

A burst can only be seen once it is over, which is why the receiver stores
events and a sweep judges them a few minutes later instead of acting on each
webhook as it lands.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Chicago")
except Exception:  # no tz database on this machine; CT without DST is close enough
    LOCAL_TZ = timezone(timedelta(hours=-6), "CT")

DOWNLOAD = "Download"
ACTIVITY = "AC Activity"

# AC custom field 8 is "Attio Record ID" (perstag ATTIO_RECORD_ID). Native
# webhooks key custom fields by id; automation webhook steps key them by
# perstag, so both are read.
DEFAULT_ATTIO_FIELD_KEYS = ("contact[fields][8]", "contact[fields][attio_record_id]")

DEFAULT_GUIDE_LINK_PATTERNS = ("plug-the-leaks-guide",)
DEFAULT_GUIDE_TAGS = ("Guide Download",)

# Links that are AC plumbing, not interest: the open pixel, unsubscribe and
# preference pages.
NOISE_LINK_MARKERS = ("act=unsub", "unsubscribe", "act=update", "updatepreferences")

FREE_MAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "live.com", "msn.com", "aol.com", "icloud.com", "me.com", "mac.com",
    "comcast.net", "att.net", "verizon.net", "sbcglobal.net", "protonmail.com",
    "proton.me", "ymail.com", "bellsouth.net", "charter.net", "cox.net",
})

RULES = {
    "multi_link":   {"window_s": 60,  "min_links": 3},
    "repeat_link":  {"window_s": 60,  "min_clicks": 3},
    "domain_burst": {"window_s": 120, "min_contacts": 3},
    "early_click":  {"after_send_s": 300},
    "tag_bulk":     {"window_s": 120, "min_contacts": 20},
}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_ac_time(raw, fallback: datetime) -> datetime:
    """AC timestamps -> aware UTC. Naive values are AC account time (Central)."""
    text = (raw or "").strip()
    if text:
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=LOCAL_TZ)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    return fallback.astimezone(timezone.utc)


def bare_record_id(raw) -> str:
    """`person:<uuid>` -> `<uuid>` (see activecampaign_routes.bare_record_id)."""
    return (raw or "").strip().split(":", 1)[-1].strip()


def _tag_from(payload: dict) -> str:
    for key in ("tag", "tag[name]", "contact[tag]", "tag_name"):
        if payload.get(key):
            return str(payload[key]).strip()
    return ""


def parse_payload(payload: dict, received_at: datetime,
                  field_keys=DEFAULT_ATTIO_FIELD_KEYS) -> dict:
    """Flatten one AC form-encoded webhook into the row the hub stores."""
    etype = (payload.get("type") or "").strip().lower()
    email = (payload.get("contact[email]") or payload.get("email") or "").strip().lower()
    hint = ""
    for key in field_keys:
        if payload.get(key):
            hint = bare_record_id(payload[key])
            break
    first = (payload.get("contact[first_name]") or "").strip()
    last = (payload.get("contact[last_name]") or "").strip()
    raw_time = (payload.get("date_time") or "").strip()
    event = {
        "event_type": etype,
        "event_at": parse_ac_time(raw_time, received_at),
        "raw_time": raw_time,
        "ac_contact_id": str(payload.get("contact[id]") or "").strip(),
        "email": email,
        "record_id_hint": hint,
        "contact_name": " ".join(p for p in (first, last) if p),
        "campaign_id": str(payload.get("campaign[id]") or "").strip(),
        "campaign_name": (payload.get("campaign[name]") or "").strip(),
        "link_id": str(payload.get("link[id]") or "").strip(),
        "link_url": (payload.get("link[url]") or "").strip(),
        "tag": _tag_from(payload),
    }
    event["event_key"] = event_key(event, received_at)
    return event


def event_key(event: dict, received_at: datetime) -> str:
    """Idempotency key: contact + campaign + link (or tag) + timestamp.

    AC resends a failed webhook with the same date_time, so the key is the
    exact AC timestamp, not a coarse bucket -- a scanner clicking one link four
    times in a minute is four events, and repeat_link needs to see all four.
    Without a date_time, the minute the hub received it stands in.
    """
    stamp = event.get("raw_time") or received_at.strftime("%Y-%m-%dT%H:%M")
    who = event.get("ac_contact_id") or event.get("email") or ""
    what = event.get("link_url") or event.get("tag") or ""
    raw = "|".join([event.get("event_type", ""), who, event.get("campaign_id", ""),
                    what, stamp])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def email_domain(email: str) -> str:
    return email.rsplit("@", 1)[-1].lower() if "@" in (email or "") else ""


# ---------------------------------------------------------------------------
# What to keep
# ---------------------------------------------------------------------------

def ignore_reason(event: dict, guide_tags=DEFAULT_GUIDE_TAGS) -> str | None:
    """Events never worth storing. None means keep it."""
    etype = event.get("event_type")
    if etype == "click":
        url = (event.get("link_url") or "").strip().lower()
        if not url or url == "open":
            return "open_pixel"
        if any(m in url for m in NOISE_LINK_MARKERS):
            return "unsubscribe_link"
        return None
    if etype == "contact_tag_added":
        wanted = {t.casefold() for t in guide_tags}
        return None if (event.get("tag") or "").casefold() in wanted else "untracked_tag"
    return "unsupported_type"


def classify(event: dict, guide_patterns=DEFAULT_GUIDE_LINK_PATTERNS,
             guide_tags=DEFAULT_GUIDE_TAGS) -> str:
    if event.get("event_type") == "contact_tag_added":
        wanted = {t.casefold() for t in guide_tags}
        if (event.get("tag") or "").casefold() in wanted:
            return DOWNLOAD
    url = (event.get("link_url") or "").lower()
    if any(p.lower() in url for p in guide_patterns if p):
        return DOWNLOAD
    return ACTIVITY


def _who(e: dict) -> str:
    return e.get("ac_contact_id") or e.get("email") or e.get("event_key")


def _windows(items, window_s):
    """Yield each maximal run of items (sorted by event_at) within window_s of its first."""
    items = sorted(items, key=lambda e: e["event_at"])
    for i, first in enumerate(items):
        run = [first]
        for later in items[i + 1:]:
            if (later["event_at"] - first["event_at"]).total_seconds() > window_s:
                break
            run.append(later)
        yield run


def scanner_verdicts(events, send_times: dict | None = None, rules=RULES) -> dict:
    """{event_key: rule_name} for every event the filter drops.

    `events` should include neighbours already judged in earlier sweeps, so a
    burst split across two sweeps is still seen whole. `send_times` maps
    campaign_id -> aware datetime AC finished sending it.
    """
    dropped: dict[str, str] = {}

    def mark(run, rule):
        for e in run:
            dropped.setdefault(e["event_key"], rule)

    clicks = [e for e in events if e.get("event_type") == "click"]
    tags = [e for e in events if e.get("event_type") == "contact_tag_added"]

    by_contact = defaultdict(list)
    by_link = defaultdict(list)
    by_domain = defaultdict(list)
    for e in clicks:
        by_contact[(_who(e), e.get("campaign_id"))].append(e)
        by_link[(_who(e), e.get("campaign_id"), e.get("link_url"))].append(e)
        dom = email_domain(e.get("email", ""))
        if dom and dom not in FREE_MAIL_DOMAINS:
            by_domain[(e.get("campaign_id"), dom)].append(e)

    r = rules["multi_link"]
    for group in by_contact.values():
        for run in _windows(group, r["window_s"]):
            if len({e.get("link_url") for e in run}) >= r["min_links"]:
                mark(run, "multi_link")

    r = rules["repeat_link"]
    for group in by_link.values():
        for run in _windows(group, r["window_s"]):
            if len(run) >= r["min_clicks"]:
                mark(run, "repeat_link")

    r = rules["domain_burst"]
    for group in by_domain.values():
        for run in _windows(group, r["window_s"]):
            if len({_who(e) for e in run}) >= r["min_contacts"]:
                mark(run, "domain_burst")

    r = rules["early_click"]
    for e in clicks:
        sent = (send_times or {}).get(e.get("campaign_id"))
        if sent is not None:
            gap = (e["event_at"] - sent).total_seconds()
            if 0 <= gap <= r["after_send_s"]:
                mark([e], "early_click")

    r = rules["tag_bulk"]
    by_tag = defaultdict(list)
    for e in tags:
        by_tag[(e.get("tag") or "").casefold()].append(e)
    for group in by_tag.values():
        for run in _windows(group, r["window_s"]):
            if len({_who(e) for e in run}) >= r["min_contacts"]:
                mark(run, "tag_bulk")

    return dropped


# ---------------------------------------------------------------------------
# What gets written to Attio
# ---------------------------------------------------------------------------

def next_business_day(d: date) -> date:
    d = d + timedelta(days=1)
    while d.weekday() >= 5:  # Sat, Sun
        d += timedelta(days=1)
    return d


def task_deadline(now: datetime, hour: int = 17) -> datetime:
    """5pm Central on the next business day after `now`, as aware UTC."""
    local_day = now.astimezone(LOCAL_TZ).date()
    due = datetime.combine(next_business_day(local_day), time(hour, 0), LOCAL_TZ)
    return due.astimezone(timezone.utc)


def label(event: dict) -> str:
    return event.get("campaign_name") or event.get("tag") or "ActiveCampaign"


def note_title(kind: str, event: dict) -> str:
    prefix = "AC Download" if kind == DOWNLOAD else "AC Activity"
    return f"{prefix} — {label(event)}"


def note_line(event: dict) -> str:
    when = event["event_at"].astimezone(LOCAL_TZ).strftime("%Y-%m-%d %I:%M %p CT")
    if event.get("event_type") == "contact_tag_added":
        what = f"Tagged “{event.get('tag')}” in ActiveCampaign"
    else:
        what = f"Clicked {event.get('link_url')}"
    campaign = event.get("campaign_name")
    return f"{when} · {what}" + (f" · {campaign}" if campaign else "")
