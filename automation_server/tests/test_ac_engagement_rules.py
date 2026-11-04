"""Tests for the pure engagement rules. Run from automation_server/: pytest -q tests"""

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ac_engagement_rules as r  # noqa: E402

T0 = datetime(2026, 10, 29, 18, 53, 0, tzinfo=timezone.utc)  # 13:53 CT
GUIDE = "https://raisetell.com/plug-the-leaks-guide"
SOCIAL = ["https://www.instagram.com/goraisetell/", "https://www.facebook.com/RaiseTell",
          "https://www.youtube.com/@RaiseTellNow", "https://www.linkedin.com/company/raisetell/"]


def click(contact, url, seconds, email=None, campaign="84"):
    e = {"event_type": "click", "ac_contact_id": str(contact),
         "email": email or f"person{contact}@org{contact}.org",
         "campaign_id": campaign, "campaign_name": "Leaky Bucket Guide Email 1",
         "link_url": url, "tag": "", "event_at": T0 + timedelta(seconds=seconds)}
    e["event_key"] = f"{contact}|{url}|{seconds}"
    return e


def tag(contact, seconds, name="Guide Download"):
    return {"event_type": "contact_tag_added", "ac_contact_id": str(contact),
            "email": f"p{contact}@x{contact}.org", "campaign_id": "", "link_url": "",
            "tag": name, "event_at": T0 + timedelta(seconds=seconds),
            "event_key": f"tag|{contact}|{seconds}"}


def test_parse_native_click_payload():
    received = datetime(2026, 10, 31, 20, 0, tzinfo=timezone.utc)
    e = r.parse_payload({
        "type": "click", "date_time": "2026-10-31T14:12:05-05:00",
        "contact[id]": "991", "contact[email]": "Laura@Example.org",
        "contact[first_name]": "Laura", "contact[last_name]": "Halad",
        "contact[fields][8]": "person:1234-abcd",
        "campaign[id]": "86", "campaign[name]": "Leaky Bucket Guide Email 2",
        "link[id]": "320", "link[url]": GUIDE,
    }, received)
    assert e["email"] == "laura@example.org"
    assert e["record_id_hint"] == "1234-abcd"
    assert e["contact_name"] == "Laura Halad"
    assert e["event_at"] == datetime(2026, 10, 31, 19, 12, 5, tzinfo=timezone.utc)
    # same payload again (an AC retry) -> same key
    again = r.parse_payload({"type": "click", "date_time": "2026-10-31T14:12:05-05:00",
                             "contact[id]": "991", "campaign[id]": "86",
                             "link[url]": GUIDE}, received)
    assert again["event_key"] == e["event_key"]


def test_naive_ac_time_is_central():
    got = r.parse_ac_time("2026-10-29 13:53:00", T0)
    assert got == T0


def test_ignore_open_pixel_unsubscribe_and_other_tags():
    assert r.ignore_reason({"event_type": "click", "link_url": "open"}) == "open_pixel"
    assert r.ignore_reason({"event_type": "click",
                            "link_url": "https://x.activehosted.com/proc.php?act=unsub"}) == "unsubscribe_link"
    assert r.ignore_reason({"event_type": "click", "link_url": GUIDE}) is None
    assert r.ignore_reason({"event_type": "contact_tag_added", "tag": "Import Date: 07/18/2026"}) == "untracked_tag"
    assert r.ignore_reason({"event_type": "contact_tag_added", "tag": "guide download"}) is None


def test_classify():
    assert r.classify({"event_type": "click", "link_url": GUIDE}) == r.DOWNLOAD
    assert r.classify({"event_type": "click", "link_url": SOCIAL[0]}) == r.ACTIVITY
    assert r.classify({"event_type": "contact_tag_added", "tag": "Guide Download"}) == r.DOWNLOAD


def test_scanner_clicking_every_link_is_dropped():
    # One gateway opens all five links within ~10 seconds, twice over.
    events = [click(1, u, i * 2) for i, u in enumerate([GUIDE] + SOCIAL)]
    events += [click(1, u, 20 + i * 2) for i, u in enumerate([GUIDE] + SOCIAL)]
    dropped = r.scanner_verdicts(events)
    assert set(dropped) == {e["event_key"] for e in events}


def test_person_clicking_guide_once_or_twice_is_kept():
    events = [click(2, GUIDE, 0), click(2, GUIDE, 40)]
    assert r.scanner_verdicts(events) == {}


def test_same_link_hammered_is_dropped():
    events = [click(3, GUIDE, s) for s in (0, 1, 2, 3)]
    assert set(r.scanner_verdicts(events).values()) == {"repeat_link"}


def test_domain_burst_drops_org_wide_scan_but_not_free_mail():
    org = [click(10 + i, GUIDE, i * 15, email=f"u{i}@museum.org") for i in range(3)]
    gmail = [click(20 + i, GUIDE, i * 15, email=f"u{i}@gmail.com") for i in range(3)]
    dropped = r.scanner_verdicts(org + gmail)
    assert set(dropped) == {e["event_key"] for e in org}
    assert set(dropped.values()) == {"domain_burst"}


def test_colleagues_far_apart_are_kept():
    events = [click(30 + i, GUIDE, i * 3600, email=f"u{i}@museum.org") for i in range(3)]
    assert r.scanner_verdicts(events) == {}


def test_early_click_needs_send_time():
    e = click(40, GUIDE, 0)
    assert r.scanner_verdicts([e]) == {}
    sent = T0 - timedelta(minutes=2)
    assert r.scanner_verdicts([e], {"84": sent}) == {e["event_key"]: "early_click"}
    late = click(41, GUIDE, 3600)
    assert r.scanner_verdicts([late], {"84": sent}) == {}


def test_tag_bulk_import_is_dropped_single_download_kept():
    bulk = [tag(100 + i, i) for i in range(25)]
    assert set(r.scanner_verdicts(bulk).values()) == {"tag_bulk"}
    assert r.scanner_verdicts([tag(1, 0)]) == {}


def test_next_business_day_skips_weekend():
    assert r.next_business_day(date(2026, 11, 4)) == date(2026, 11, 5)   # Wed -> Thu
    assert r.next_business_day(date(2026, 11, 6)) == date(2026, 11, 9)   # Fri -> Mon
    assert r.next_business_day(date(2026, 11, 7)) == date(2026, 11, 9)   # Sat -> Mon


def test_task_deadline_is_5pm_central_next_business_day():
    # Friday 11pm CT is already Saturday in UTC; due Monday either way.
    fri_night = datetime(2026, 11, 7, 5, 0, tzinfo=timezone.utc)
    due = r.task_deadline(fri_night)
    local = due.astimezone(r.LOCAL_TZ)
    assert local.date() == date(2026, 11, 9) and local.hour == 17


def test_note_title_and_line():
    e = click(5, GUIDE, 0)
    assert r.note_title(r.DOWNLOAD, e) == "AC Download — Leaky Bucket Guide Email 1"
    assert r.note_title(r.ACTIVITY, e).startswith("AC Activity — ")
    line = r.note_line(e)
    assert line.startswith("2026-10-29 01:53 PM CT") and GUIDE in line
