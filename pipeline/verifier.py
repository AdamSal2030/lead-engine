from __future__ import annotations
"""Reoon email verifier. Tier A only — drops everything else."""
import asyncio
import httpx
import urllib.parse
import logging
from config import settings

log = logging.getLogger("verifier")

REOON_IPS = [s.strip() for s in settings.REOON_IPS.split(",") if s.strip()]
REOON_HOST = "emailverifier.reoon.com"


async def verify_email(email: str, retries: int = 3) -> dict | None:
    """Returns Reoon power response or None."""
    qs = urllib.parse.urlencode({"email": email, "key": settings.REOON_API_KEY, "mode": "power"})
    url = f"{settings.REOON_BASE_URL}/api/v1/verify?{qs}"
    for attempt in range(retries):
        ip = REOON_IPS[attempt % len(REOON_IPS)] if REOON_IPS else None
        try:
            # If we pinned IPs, use httpx transport with explicit address
            transport = None
            if ip:
                transport = httpx.AsyncHTTPTransport(
                    local_address=None,
                    retries=0,
                )
            async with httpx.AsyncClient(timeout=30, transport=transport) as cli:
                r = await cli.get(url)
                if r.status_code == 429:
                    await asyncio.sleep(5)
                    continue
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            log.debug(f"reoon {email} attempt {attempt} fail: {e}")
            await asyncio.sleep(2)
    return None


async def verify_lead(lead: dict) -> dict | None:
    """Try candidates in priority order. Return TIER A lead only, else None."""
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None

    def priority(e):
        local = e.split("@")[0]
        # Personal-looking first
        if local in {"info", "hello", "contact", "support", "team", "admin", "office", "sales"}:
            return 2
        return 1

    sorted_c = sorted(candidates, key=priority)[:8]

    for email in sorted_c:
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        safe = res.get("is_safe_to_send")
        score = res.get("overall_score") or 0
        catch = res.get("is_catch_all")

        # Strict Tier A only: real mailbox confirmed via SMTP, no role accounts, no catch-all
        if status in ("safe", "valid") and not catch and score >= 75:
            return {
                **lead,
                "verified_email": email,
                "verification": {
                    "status": status, "score": score,
                    "is_catch_all": catch, "is_safe_to_send": safe,
                    "tier": "A",
                },
            }
    return None
