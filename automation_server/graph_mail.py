"""
graph_mail.py

Creates Outlook drafts in each rep's OWN mailbox via Microsoft Graph, so the
draft lands in their Drafts folder and they press Send themselves. Nothing
here sends mail.

Why delegated (not application) permissions
-------------------------------------------
The requirement is "the rep's own mailbox, not a shared/service mailbox".
Application permissions (Mail.ReadWrite as an app) grant tenant-wide mailbox
access, which is both more privilege than this needs and the wrong identity
-- drafts would not be authored by the rep. So each rep signs in once via
the authorization-code flow and we keep their refresh token. `/me` is then
literally that rep.

Nothing writes into Attio's draft area -- there is no API for it, and it is
not needed: Attio's native mail-sync logs the email onto the contact record
once it is actually sent from Outlook.

One-time setup Jay has to do in Entra (cannot be provisioned from code)
-----------------------------------------------------------------------
1. Entra admin center > App registrations > New registration
     - Single tenant
     - Redirect URI (Web): https://<hub-domain>/auth/microsoft/callback
2. API permissions > Microsoft Graph > Delegated:
     Mail.ReadWrite, User.Read, offline_access
   Then "Grant admin consent" as Global Admin. Without tenant consent the
   token exchange returns AADSTS65001 and Graph will 401/403.
3. Certificates & secrets > New client secret.
4. Set on this Railway service:
     AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, HUB_PUBLIC_URL
5. Each rep visits /auth/microsoft/start?rep=<name> once and signs in.

Refresh tokens live in hubspot_email_archive.main.graph_oauth_tokens.
"""

import hashlib
import hmac
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import duckdb
import requests
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()

AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")
HUB_PUBLIC_URL = os.environ.get("HUB_PUBLIC_URL", "").rstrip("/")
OPS_CENTER_TOKEN = os.environ.get("OPS_CENTER_TOKEN", "")
MOTHERDUCK_DB = os.environ.get("MOTHERDUCK_DATABASE", "hubspot_email_archive")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPES = "offline_access User.Read Mail.ReadWrite"

REPS = ("kurt", "jay", "joel")


def _authority():
    return f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0"


def _redirect_uri():
    return f"{HUB_PUBLIC_URL}/auth/microsoft/callback"


def _require_config():
    missing = [
        n for n, v in [
            ("AZURE_TENANT_ID", AZURE_TENANT_ID),
            ("AZURE_CLIENT_ID", AZURE_CLIENT_ID),
            ("AZURE_CLIENT_SECRET", AZURE_CLIENT_SECRET),
            ("HUB_PUBLIC_URL", HUB_PUBLIC_URL),
        ] if not v
    ]
    if missing:
        raise HTTPException(
            status_code=503,
            detail=f"Microsoft Graph not configured -- missing: {', '.join(missing)}",
        )


def _sign_state(rep: str) -> str:
    """Binds the callback to a start request we actually issued. The callback
    is hit by a browser redirect and so cannot carry the bearer token."""
    issued = str(int(time.time()))
    msg = f"{rep}:{issued}"
    sig = hmac.new(OPS_CENTER_TOKEN.encode(), msg.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{msg}:{sig}"


def _verify_state(state: str) -> str:
    try:
        rep, issued, sig = state.split(":")
    except ValueError:
        raise HTTPException(status_code=400, detail="Malformed state")
    expected = hmac.new(
        OPS_CENTER_TOKEN.encode(), f"{rep}:{issued}".encode(), hashlib.sha256
    ).hexdigest()[:32]
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=400, detail="Invalid state signature")
    if time.time() - int(issued) > 900:
        raise HTTPException(status_code=400, detail="Sign-in link expired, start again")
    return rep


def _md():
    return duckdb.connect(f"md:{MOTHERDUCK_DB}?motherduck_token={os.environ['MOTHERDUCK_TOKEN']}")


def _ensure_token_table(con):
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MOTHERDUCK_DB}.main.graph_oauth_tokens (
            rep VARCHAR PRIMARY KEY,
            upn VARCHAR,
            refresh_token VARCHAR,
            updated_at TIMESTAMP
        )
    """)


def _store_refresh_token(rep: str, upn: str, refresh_token: str):
    con = _md()
    try:
        _ensure_token_table(con)
        con.execute(
            f"""INSERT OR REPLACE INTO {MOTHERDUCK_DB}.main.graph_oauth_tokens
                (rep, upn, refresh_token, updated_at) VALUES (?, ?, ?, ?)""",
            [rep, upn, refresh_token, datetime.now(timezone.utc)],
        )
    finally:
        con.close()


def _load_refresh_token(rep: str):
    con = _md()
    try:
        _ensure_token_table(con)
        row = con.execute(
            f"SELECT refresh_token, upn FROM {MOTHERDUCK_DB}.main.graph_oauth_tokens WHERE rep = ?",
            [rep],
        ).fetchone()
    finally:
        con.close()
    return row


def access_token_for(rep: str) -> tuple[str, str]:
    """Exchanges the stored refresh token for a fresh access token.
    Returns (access_token, upn)."""
    _require_config()
    row = _load_refresh_token(rep)
    if not row:
        raise HTTPException(
            status_code=428,
            detail=f"{rep} has not connected Outlook yet -- open "
                   f"/auth/microsoft/start?rep={rep} and sign in once.",
        )
    refresh_token, upn = row

    resp = requests.post(
        f"{_authority()}/token",
        data={
            "client_id": AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": GRAPH_SCOPES,
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Token refresh failed for {rep}: {resp.text[:300]}. "
                   f"They may need to sign in again.",
        )
    payload = resp.json()
    # Entra rotates refresh tokens -- persist the new one or the next call fails.
    if payload.get("refresh_token"):
        _store_refresh_token(rep, upn, payload["refresh_token"])
    return payload["access_token"], upn


def create_outlook_draft(rep: str, to_address: str, subject: str, body: str) -> dict:
    """Creates a draft in the rep's own Drafts folder. Returns the Graph
    message id and webLink so the UI can open it in Outlook.

    POST /me/messages creates a draft; `isDraft` is a read-only property on
    the message resource, so it is not sent in the body.
    """
    token, upn = access_token_for(rep)
    resp = requests.post(
        f"{GRAPH_BASE}/me/messages",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [{"emailAddress": {"address": to_address}}],
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Graph draft creation failed: {resp.text[:400]}",
        )
    msg = resp.json()
    return {
        "ok": True,
        "mailbox": upn,
        "message_id": msg.get("id"),
        "web_link": msg.get("webLink"),
        "to": to_address,
        "subject": subject,
    }


# ---------------------------------------------------------------------------
# One-time per-rep sign-in
# ---------------------------------------------------------------------------

@router.get("/auth/microsoft/start")
def microsoft_auth_start(rep: str, authorization: str | None = Header(None)):
    _require_config()
    if not OPS_CENTER_TOKEN:
        raise HTTPException(status_code=503, detail="OPS_CENTER_TOKEN not configured")
    expected = f"Bearer {OPS_CENTER_TOKEN}"
    if not authorization or not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if rep not in REPS:
        raise HTTPException(status_code=400, detail="rep must be kurt, jay, or joel")

    params = {
        "client_id": AZURE_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": _redirect_uri(),
        "response_mode": "query",
        "scope": GRAPH_SCOPES,
        "state": _sign_state(rep),
        # Force account choice so a rep never silently connects the wrong mailbox.
        "prompt": "select_account",
    }
    return RedirectResponse(f"{_authority()}/authorize?{urlencode(params)}")


@router.get("/auth/microsoft/callback")
def microsoft_auth_callback(
    code: str | None = None, state: str | None = None,
    error: str | None = None, error_description: str | None = None,
):
    if error:
        return HTMLResponse(
            f"<h3>Sign-in failed</h3><p>{error}</p><p>{error_description or ''}</p>",
            status_code=400,
        )
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing code or state")

    _require_config()
    rep = _verify_state(state)

    resp = requests.post(
        f"{_authority()}/token",
        data={
            "client_id": AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": _redirect_uri(),
            "scope": GRAPH_SCOPES,
        },
        timeout=30,
    )
    if resp.status_code >= 400:
        return HTMLResponse(
            f"<h3>Token exchange failed</h3><pre>{resp.text[:600]}</pre>"
            "<p>AADSTS65001 here means admin consent has not been granted for "
            "the delegated Graph permissions.</p>",
            status_code=400,
        )
    payload = resp.json()

    me = requests.get(
        f"{GRAPH_BASE}/me",
        headers={"Authorization": f"Bearer {payload['access_token']}"},
        timeout=30,
    )
    upn = me.json().get("userPrincipalName", "unknown") if me.ok else "unknown"

    _store_refresh_token(rep, upn, payload["refresh_token"])
    return HTMLResponse(
        f"<h3>Outlook connected</h3><p><b>{rep}</b> is now linked to "
        f"<b>{upn}</b>. Drafts will be created in that mailbox. "
        "You can close this tab.</p>"
    )


@router.get("/auth/microsoft/status")
def microsoft_auth_status(authorization: str | None = Header(None)):
    """Which reps have connected a mailbox. Never returns token material."""
    if not OPS_CENTER_TOKEN:
        raise HTTPException(status_code=503, detail="OPS_CENTER_TOKEN not configured")
    expected = f"Bearer {OPS_CENTER_TOKEN}"
    if not authorization or not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")

    con = _md()
    try:
        _ensure_token_table(con)
        rows = con.execute(
            f"SELECT rep, upn, updated_at FROM {MOTHERDUCK_DB}.main.graph_oauth_tokens"
        ).fetchall()
    finally:
        con.close()
    connected = {r[0]: {"upn": r[1], "connected_at": r[2].isoformat() if r[2] else None} for r in rows}
    return {
        "configured": all([AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, HUB_PUBLIC_URL]),
        "reps": {rep: connected.get(rep) for rep in REPS},
    }
