from __future__ import annotations
"""Hunter.io email finder — Layers 4.5 + 5 in the discovery stack.

TWO modes:
  domain_search(domain) → finds all known emails for a domain, no name needed.
    Use this for directory/company-only leads (Trustpilot, Clutch, GoodFirms …).
    Returns the highest-confidence email directly.

  find_email(first, last, domain) → person-level lookup, fires after Skrapp.

Free tier: 25 searches/month. Starter: $49/mo → 500/mo.
Add HUNTER_API_KEY to Railway env to activate.
"""
import asyncio
import logging
import httpx
from config import settings

log = logging.getLogger("hunter")

_quota_exhausted = False
_calls = 0
_hits = 0
_domain_search_calls = 0
_lock = asyncio.Lock()

# Domains where Hunter won't give a real personal mailbox
SKIP_DOMAINS = {
    "google.com", "apple.com", "microsoft.com", "amazon.com", "meta.com",
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
    "salesforce.com", "oracle.com", "ibm.com", "accenture.com",
    "deloitte.com", "pwc.com", "ey.com", "kpmg.com",
}


async def domain_search(domain: str, limit: int = 10) -> list[dict]:
    """Hunter.io domain-search — returns real emails found for a domain.

    Does NOT need a person name. Perfect for directory/Trustpilot leads where
    we have the company domain but not the founder's full name.

    Returns list of {"email", "type", "confidence", "first_name", "last_name",
    "position"} dicts, sorted by confidence descending. Empty list on failure.
    """
    global _quota_exhausted, _domain_search_calls

    if not settings.HUNTER_API_KEY or not settings.HUNTER_ENABLED:
        return []
    if _quota_exhausted:
        return []
    if not domain or len(domain) > 60 or domain.lower() in SKIP_DOMAINS:
        return []

    async with _lock:
        _domain_search_calls += 1

    try:
        async with httpx.AsyncClient(timeout=15) as cli:
            r = await cli.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain": domain.lower().replace("www.", ""),
                    "limit": limit,
                    "api_key": settings.HUNTER_API_KEY,
                },
            )

        if r.status_code in (401, 403):
            log.warning(f"Hunter domain-search auth/quota HTTP {r.status_code} — disabling")
            _quota_exhausted = True
            return []
        if r.status_code == 429:
            log.info("Hunter domain-search rate-limited — skipping")
            return []
        if r.status_code != 200:
            return []

        data = r.json().get("data") or {}
        emails_raw = data.get("emails") or []

        results = []
        for e in emails_raw:
            email = (e.get("value") or "").strip().lower()
            confidence = int(e.get("confidence") or 0)
            if email and confidence >= 25:
                results.append({
                    "email": email,
                    "confidence": confidence,
                    "type": e.get("type", ""),           # "personal" or "generic"
                    "first_name": (e.get("first_name") or "").strip(),
                    "last_name": (e.get("last_name") or "").strip(),
                    "position": (e.get("position") or "").strip(),
                    "source": "hunter_domain",
                })
        # Sort: personal first, then by confidence
        results.sort(key=lambda x: (0 if x["type"] == "personal" else 1, -x["confidence"]))
        if results:
            log.debug(f"Hunter domain-search {domain}: {len(results)} emails")
        return results

    except Exception as e:
        log.debug(f"Hunter domain-search error {domain}: {e}")
        return []


async def find_email(first_name: str, last_name: str, domain: str) -> dict | None:
    """Hunter.io person email-finder — Layer 5 after Skrapp (L4).

    Returns {"email", "confidence", "source": "hunter"} or None.
    """
    global _quota_exhausted, _calls, _hits

    if not settings.HUNTER_API_KEY or not settings.HUNTER_ENABLED:
        return None
    if _quota_exhausted:
        return None
    if not first_name or not last_name or not domain:
        return None
    if len(domain) > 60 or domain.lower() in SKIP_DOMAINS:
        return None
    if not first_name.strip() or not last_name.strip():
        return None

    async with _lock:
        _calls += 1

    try:
        async with httpx.AsyncClient(timeout=15) as cli:
            r = await cli.get(
                "https://api.hunter.io/v2/email-finder",
                params={
                    "first_name": first_name.strip(),
                    "last_name": last_name.strip(),
                    "domain": domain.lower(),
                    "api_key": settings.HUNTER_API_KEY,
                },
            )

        if r.status_code in (401, 403):
            log.warning(f"Hunter auth/quota issue HTTP {r.status_code} — disabling")
            _quota_exhausted = True
            return None
        if r.status_code == 429:
            return None
        if r.status_code != 200:
            return None

        data = r.json().get("data") or {}
        email = data.get("email")
        confidence = int(data.get("confidence") or 0)

        if not email or confidence < 30:
            return None

        async with _lock:
            _hits += 1

        return {"email": email, "confidence": confidence, "source": "hunter"}

    except Exception as e:
        log.debug(f"Hunter find_email error {first_name} {last_name}@{domain}: {e}")
        return None


def get_state() -> dict:
    return {
        "enabled": bool(settings.HUNTER_API_KEY) and not _quota_exhausted and settings.HUNTER_ENABLED,
        "quota_exhausted": _quota_exhausted,
        "calls": _calls,
        "domain_search_calls": _domain_search_calls,
        "hits": _hits,
        "hit_rate": round(100.0 * _hits / _calls, 1) if _calls else 0.0,
    }
