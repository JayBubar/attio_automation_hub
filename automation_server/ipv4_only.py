"""
ipv4_only.py

Forces DNS resolution to return IPv4 addresses only. Import this before any
HTTP client is constructed:

    import ipv4_only  # noqa: F401  -- must precede anthropic/requests clients

Why
---
This Railway service runs with `ipv6EgressEnabled: false`, so the container
has no outbound route for IPv6. Most of the hosts this codebase talks to are
dual-stack and DNS happily hands back a AAAA record:

    api.anthropic.com          A 160.79.104.10      AAAA 2607:6bc0::10
    api.attio.com              A 172.66.165.206     AAAA 2606:4700:10::6814:1e44
    graph.microsoft.com        A 40.126.23.38       AAAA 2603:1037:1:130::81
    login.microsoftonline.com  A 20.190.135.16      AAAA 2603:1037:1:148::a
    server.smartlead.ai        A 104.20.37.149      AAAA 2606:4700:10::6814:2595
    api.motherduck.com         A 98.88.88.236       (IPv4 only)

Attio calls kept working while Claude calls failed with a bare
"Connection error.", which looks like it is about the host but is not:
requests/urllib3 iterates over every getaddrinfo result and falls back to
the A record when the AAAA connect fails, and the Anthropic SDK's httpx
stack did not. So the same misconfiguration was hitting every dual-stack
host; only one library surfaced it.

Relying on one library's fallback is fragile, and even where it works it
costs a doomed IPv6 connect attempt per request. Resolving A-only removes
both problems, and costs nothing here because IPv6 egress is off anyway --
there is no IPv6-only host we could reach even in principle.

The alternative is flipping `ipv6EgressEnabled: true` on the Railway
service. That is one toggle rather than code, but it widens egress for the
whole service to fix one class of call, and it is a dashboard setting that
does not survive a service recreate. This travels with the code.

Note this only patches the process that imports it. outreach.py runs as a
subprocess and imports it separately -- patching the FastAPI process does
not reach the script, and vice versa.
"""

import socket

_original_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


socket.getaddrinfo = _ipv4_only_getaddrinfo
