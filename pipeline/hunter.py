from __future__ import annotations
"""Hunter.io email finder — Layer 5 in the discovery stack.

Fires AFTER Skrapp (L4) when we still have no strong email candidate.
Hunter specialises in B2B pattern detection and has different coverage than Skrapp,
so the two complement each other well.

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
_lock = asyncio.Lock()

# Domains where Hunter won't give a real personal mailbox
SKIP_DOMAINS = {
    "google.com", "apple.com", "microsoft.com", "amazon.com", "meta.com",
    "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
    "salesforce.com", "oracle.com", "ibm.com", "accenture.com",
    "deloitte.com", "pwc.com", "ey.com", "kpmg.com",
}


async def find_email(first_name: str, last_name: str, domain: str) -> dict | None:
    """Hunter.io email-finder API call.

    Returns {"email": ..., "confidence": ..., "source": "hunter"} or None.
    confidence is 0–100; we accept ≥ 50.
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
            log.info("Hunter rate-limited — skipping call")
            return None
        if r.status_code != 200:
            return None

        data = r.json().get("data") or {}
        email = data.get("email")
        confidence = int(data.get("confidence") or 0)

        if not email or confidence < 50:
            return None

        async with _lock:
            _hits += 1

        log.debug(f"Hunter found {email} (confidence={confidence}) for {first_name} {last_name}@{domain}")
        return {"email": email, "confidence": confidence, "source": "hunter"}

    except Exception as e:
        log.debug(f"Hunter error {first_name} {last_name}@{domain}: {e}")
        return None


def get_state() -> dict:
    return {
        "enabled": bool(settings.HUNTER_API_KEY) and not _quota_exhausted and settings.HUNTER_ENABLED,
        "quota_exhausted": _quota_exhausted,
        "calls": _calls,
        "hits": _hits,
        "hit_rate": round(100.0 * _hits / _calls, 1) if _calls else 0.0,
    }
