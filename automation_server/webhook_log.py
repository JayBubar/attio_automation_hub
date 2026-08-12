"""
webhook_log.py

Append-only record of inbound webhooks, so "is this integration alive?" is a
question with an answer.

The AC <-> Attio bridge spent a while appearing broken for the wrong reason:
the Ops Center showed it Down because it probed a URL the bridge had never
lived at, and the route itself only ever `print`ed, so the real evidence was
in Railway's log tail and expired with it. A route being reachable and a route
actually receiving traffic are different facts, and neither was recorded.

Every received payload is logged -- including the ones the route ignores.
Ignored events are the interesting ones: an `unexpected_seriesid` streak means
another automation is pointed here, and a `no_tags_field` streak means the tags
field is not mapped on the AC webhook action. Log only the successes and both
of those look identical to silence.

**Logging never fails the webhook.** `log_event` swallows everything. A
MotherDuck outage must not make the hub 500 back at ActiveCampaign, which
would make AC retry and eventually disable the webhook -- turning a
reporting-layer problem into an actual outage of the thing being reported on.
The cost of that choice is that a logging failure is itself invisible except
in the process log, which is the right trade for a receiver whose real job is
the Attio write.

Volume here is a handful of events a day (a tag going on or off a contact), so
a fresh MotherDuck connection per event is fine. Anything higher-frequency
should batch instead.
"""

import os
from datetime import datetime, timezone

import duckdb

WEBHOOK_LOG_TABLE = "webhook_event_log"


def _db_name():
    return os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")


def _connect():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        return None
    return duckdb.connect(f"md:{_db_name()}?motherduck_token={token}")


def ensure_log_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {WEBHOOK_LOG_TABLE} (
            source      VARCHAR,   -- 'activecampaign', 'smartlead', ...
            status      VARCHAR,   -- 'ok' or 'ignored'
            action      VARCHAR,   -- 'added' / 'removed', or the ignore reason
            record_id   VARCHAR,   -- Attio record id, when one was resolved
            matched_by  VARCHAR,   -- 'attio_record_id' / 'email' / ...
            detail      VARCHAR,
            received_at TIMESTAMP
        )
    """)


def log_event(source, status, action, record_id=None, matched_by=None, detail=None):
    """Record one inbound webhook. Never raises -- see module docstring."""
    conn = None
    try:
        conn = _connect()
        if conn is None:
            return
        ensure_log_table(conn)
        conn.execute(
            f"""INSERT INTO {WEBHOOK_LOG_TABLE}
                (source, status, action, record_id, matched_by, detail, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [source, status, action, record_id, matched_by, detail,
             datetime.now(timezone.utc)],
        )
    except Exception as e:
        print(f"webhook_log: could not record {source}/{action} event ({e})")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def recent_summary(source, limit=10):
    """Last-seen timestamps and a recent event tail for one source.

    Returns `available: False` rather than raising if MotherDuck is unreachable
    or the table has never been written. The Ops Center has to be able to tell
    "no events yet" from "cannot tell" -- rendering a lookup failure as
    "no traffic" is the same class of lie the hardcoded Down indicator was.
    """
    conn = None
    try:
        conn = _connect()
        if conn is None:
            return {"available": False, "reason": "MOTHERDUCK_TOKEN is not set"}
        ensure_log_table(conn)

        row = conn.execute(
            f"""SELECT
                    max(received_at)                                  AS last_event_at,
                    max(received_at) FILTER (WHERE status = 'ok')     AS last_ok_at,
                    count(*)                                          AS events_total,
                    count(*) FILTER (WHERE received_at > now() - INTERVAL 7 DAY)
                                                                      AS events_7d
                FROM {WEBHOOK_LOG_TABLE}
                WHERE source = ?""",
            [source],
        ).fetchone()

        cur = conn.execute(
            f"""SELECT received_at, status, action, record_id, matched_by, detail
                FROM {WEBHOOK_LOG_TABLE}
                WHERE source = ?
                ORDER BY received_at DESC
                LIMIT ?""",
            [source, limit],
        )
        cols = [d[0] for d in cur.description]
        recent = [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        return {"available": False, "reason": str(e)}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    for r in recent:
        r["received_at"] = iso(r["received_at"])

    return {
        "available": True,
        "last_event_at": iso(row[0]),
        "last_ok_at": iso(row[1]),
        "events_total": row[2],
        "events_7d": row[3],
        "recent": recent,
    }
