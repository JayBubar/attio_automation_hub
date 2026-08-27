"""
http_client.py

Shared outbound HTTP client for every vendor call in this service (Attio,
Smartlead, ActiveCampaign, Microsoft Graph). FastAPI runs sync `def` routes
in a bounded shared worker pool, so a bare `requests.get(..., timeout=30)`
against a vendor that is slow to connect or unreachable can occupy a worker
for up to 30s -- and unrelated fast routes (e.g. /config/flags, which makes
no outbound call at all) start failing the Ops Center's own 15-30s client
timeout while they wait for a free worker. See the Aug 2026 outbound-call
resilience investigation (Railway logs, service 2ca85ead-...).

`session()` returns a requests.Session with connection-level retries baked in
via urllib3's Retry/HTTPAdapter, so every caller gets the same policy without
copy-pasting a retry loop. It retries only on connection failures (couldn't
connect / DNS / TCP handshake), never on a status code or a read timeout --
a request that reached the vendor and either got a response or started
receiving one is not safe to blindly replay for a PATCH/POST, but a
connection failure means nothing was ever sent.

DEFAULT_TIMEOUT splits connect vs. read on purpose: a flat timeout=30 lets one
attempt eat almost the Ops Center's entire client-side budget before a retry
even happens. At (3, 10) with 2 connect retries and a 0.3 backoff, a fully
unreachable vendor fails in ~6.5s -- comfortably inside the tightest caller
budget (15s) with room for the rest of the route's own work.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# (connect, read) -- not a flat number. A stalled TCP handshake fails fast;
# a vendor that connects but answers slowly still gets a real read window.
DEFAULT_TIMEOUT = (3, 10)

_retry = Retry(
    total=2,
    connect=2,
    read=0,     # already reached the vendor -- not safe to replay blind.
    redirect=0,
    status=0,   # never retry on a status code; that's the vendor having
                # received and answered the request.
    backoff_factor=0.3,
    allowed_methods=None,  # a connection failure means nothing was sent,
                            # regardless of verb, so any method is safe here.
)

_adapter = HTTPAdapter(max_retries=_retry)

_session = requests.Session()
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def session():
    """The shared retrying session. Reuse this instead of a bare `requests`
    call for any outbound vendor request."""
    return _session


def request(method, url, timeout=None, **kwargs):
    """`session().request` with DEFAULT_TIMEOUT unless the caller overrides
    it. Prefer this (or get/post/patch below) over calling session()
    directly, so no call site can drift back to an unbounded timeout."""
    return _session.request(method, url, timeout=timeout or DEFAULT_TIMEOUT, **kwargs)


def get(url, **kwargs):
    return request("GET", url, **kwargs)


def post(url, **kwargs):
    return request("POST", url, **kwargs)


def patch(url, **kwargs):
    return request("PATCH", url, **kwargs)


# Exceptions worth catching at a route boundary and turned into a 502/503
# instead of an unhandled 500: the vendor could not be reached, or reached
# too slowly, even after retries.
VENDOR_UNAVAILABLE_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)
