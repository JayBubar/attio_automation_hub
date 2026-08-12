"""
automation_config.py

Runtime on/off switches for the automations, so pausing or resuming one is a
config change rather than an edit-commit-deploy cycle. Backs the Ops Center's
toggles (see /config/flags in ops_center_routes.py) and is read by
scripts/outreach_rotation.py at the top of each run.

Resolution order for a flag, first hit wins:

  1. MotherDuck `automation_config` row  -- the Ops Center toggle writes here
  2. environment variable                -- Railway Variables tab
  3. the default passed by the caller

MotherDuck outranks the env var on purpose: the toggle has to take effect on
the next scheduled run without a redeploy, and a Railway variable change
restarts the service. The env var stays useful as the value a fresh
environment starts from, and as the answer when MotherDuck is unreachable.

**Failures resolve to the safe value, not the last-known one.** If MotherDuck
is down, `get_flag` falls through to env/default rather than raising. For a
pause flag that means calling it as `get_flag("ac_paused", default=True)` --
an unintended skip is a day of no outreach, an unintended push emails real
people. Callers pick the direction; this module just never guesses.
"""

import os
from datetime import datetime, timezone

import duckdb

CONFIG_TABLE = "automation_config"

_TRUE = {"true", "1", "yes", "on", "paused"}
_FALSE = {"false", "0", "no", "off", "running"}


def _db_name():
    return os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")


def _connect():
    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        return None
    return duckdb.connect(f"md:{_db_name()}?motherduck_token={token}")


def ensure_config_table(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG_TABLE} (
            key         VARCHAR,
            value       VARCHAR,
            updated_at  TIMESTAMP,
            updated_by  VARCHAR
        )
    """)


def _coerce_bool(raw):
    """Return True/False for a recognized value, None for anything else.

    Unparseable values fall through to the next source rather than being
    read as false -- a typo'd toggle value must not silently un-pause a job.
    """
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return None


def get_flag(key, default, env_var=None):
    """Resolve a boolean flag through MotherDuck -> env var -> default."""
    conn = None
    try:
        conn = _connect()
        if conn is not None:
            ensure_config_table(conn)
            row = conn.execute(
                f"SELECT value FROM {CONFIG_TABLE} WHERE key = ? ORDER BY updated_at DESC LIMIT 1",
                [key],
            ).fetchone()
            value = _coerce_bool(row[0]) if row else None
            if value is not None:
                return value
    except Exception as e:
        # Never let a config lookup take down the caller. The env/default
        # fallthrough below is the whole point of catching this broadly.
        print(f"automation_config: MotherDuck lookup for {key!r} failed ({e}); using env/default")
    finally:
        if conn is not None:
            conn.close()

    value = _coerce_bool(os.environ.get(env_var or key.upper()))
    return default if value is None else value


def set_flag(key, value, updated_by="ops-center"):
    """Write a flag to MotherDuck. Raises if MotherDuck is unavailable --
    a toggle that silently fails to save is worse than one that errors."""
    conn = _connect()
    if conn is None:
        raise RuntimeError("MOTHERDUCK_TOKEN is not set; cannot persist config")
    try:
        ensure_config_table(conn)
        # Delete-then-insert rather than UPDATE: the table is keyed by
        # convention, not constraint, so this collapses any duplicate rows a
        # previous partial write may have left behind.
        conn.execute(f"DELETE FROM {CONFIG_TABLE} WHERE key = ?", [key])
        conn.execute(
            f"INSERT INTO {CONFIG_TABLE} (key, value, updated_at, updated_by) VALUES (?, ?, ?, ?)",
            [key, "true" if value else "false", datetime.now(timezone.utc), updated_by],
        )
    finally:
        conn.close()


def list_flags():
    """All stored flags, for the Ops Center toggle panel."""
    conn = _connect()
    if conn is None:
        return []
    try:
        ensure_config_table(conn)
        cur = conn.execute(
            f"SELECT key, value, updated_at, updated_by FROM {CONFIG_TABLE} ORDER BY key"
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    for r in rows:
        if hasattr(r.get("updated_at"), "isoformat"):
            r["updated_at"] = r["updated_at"].isoformat()
    return rows
