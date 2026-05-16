from __future__ import annotations
"""Reoon email verifier. Tier A only — drops everything else."""
import asyncio
import httpx
import urllib.parse
import logging
from sqlalchemy import select, insert, update
from config import settings
from db import SessionLocal, Counter

log = logging.getLogger("verifier")

REOON_IPS = [s.strip() for s in settings.REOON_IPS.split(",") if s.strip()]
REOON_HOST = "emailverifier.reoon.com"

# Persistent counter for Reoon verifier calls.
# Reoon doesn't expose a remaining-credit endpoint, so this tracks our usage over time
# (survives redeploys via the SQLite counters table).
CALLS_MADE = 0
_counter_loaded = False
_counter_lock = asyncio.Lock()


async def _load_counter():
    """Load persisted counter from DB on first use."""
    global CALLS_MADE, _counter_loaded
    if _counter_loaded:
        return
    async with _counter_lock:
        if _counter_loaded:
            return
        async with SessionLocal() as s:
            row = (await s.execute(select(Counter).where(Counter.key == "reoon_calls"))).scalar_one_or_none()
            CALLS_MADE = row.value if row else 0
            _counter_loaded = True


async def _persist_counter():
    """Write current CALLS_MADE to DB. Best-effort, ignores errors."""
    try:
        async with SessionLocal() as s:
            existing = (await s.execute(select(Counter).where(Counter.key == "reoon_calls"))).scalar_one_or_none()
            if existing:
                await s.execute(update(Counter).where(Counter.key == "reoon_calls").values(value=CALLS_MADE))
            else:
                await s.execute(insert(Counter).values(key="reoon_calls", value=CALLS_MADE))
            await s.commit()
    except Exception:
        pass


async def verify_email(email: str, retries: int = 3) -> dict | None:
    """Returns Reoon power response or None."""
    global CALLS_MADE
    await _load_counter()
    CALLS_MADE += 1
    # Persist every 10 calls (avoids excessive DB writes)
    if CALLS_MADE % 10 == 0:
        await _persist_counter()
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
    """Try candidates in priority order (orchestrator already ranked them). Tier-A only."""
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None
    # Trust the orchestrator's ranking; cap at 6 to limit Reoon credit burn per lead
    sorted_c = list(candidates)[:6]

    for email in sorted_c:
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        safe = res.get("is_safe_to_send")
        score = res.get("overall_score") or 0
        catch = res.get("is_catch_all")

        # Tier A: SMTP-confirmed deliverable, NOT catch-all (catch-all bounces in practice)
        if status in ("safe", "valid") and not catch:
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
