from __future__ import annotations
"""Shared networking helpers — residential-proxy routing for blocked sites.

The Voyage / ShoutOut interview networks (and a couple of Cloudflare sites)
block datacenter IPs, so on Railway they previously fell back to the Wayback
Machine, which serves STALE cached sitemaps — the #1 yield drag.

When PROXY_URL is configured we route *only those blocked networks* through the
residential proxy so they fetch LIVE again. Founder/external websites and all
API calls (MillionVerifier, Skrapp, Instantly …) stay direct, so we never burn
paid proxy bandwidth on traffic that isn't blocked.
"""
from config import settings

# Host substrings for the interview networks that block datacenter IPs. Only
# these are worth the paid proxy; everything else fetches directly.
BLOCKED_NETWORK_HINTS = ("voyage", "shoutout", "canvasrebel", "boldjourney")


def should_proxy(url: str) -> bool:
    """True when this URL belongs to a blocked network AND a proxy is configured.

    Wayback URLs are never proxied (web.archive.org isn't blocked, and a Wayback
    URL wraps the original — which may contain a blocked-network substring)."""
    if not (settings.PROXY_URL or "").strip():
        return False
    u = url.lower()
    if "web.archive.org" in u:
        return False
    return any(h in u for h in BLOCKED_NETWORK_HINTS)


def proxy_client_kwargs(url: str) -> dict:
    """httpx.AsyncClient kwargs to route THIS url through the proxy, else {}.

    verify=False mirrors the proxy's quick-test mode (BrightData performs TLS
    interception); we're only scraping public sitemaps/articles, so this is fine.
    """
    if not should_proxy(url):
        return {}
    return {"proxy": settings.PROXY_URL.strip(), "verify": False}
