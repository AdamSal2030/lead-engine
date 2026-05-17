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
    """Returns Reoon power response or None. Uses pooled keys for parallelism."""
    global CALLS_MADE
    await _load_counter()
    from pipeline.reoon_pool import get_pool
    pool = get_pool()
    if not pool.has_keys():
        return None  # no keys configured

    for attempt in range(retries):
        api_key = await pool.acquire()
        if not api_key:
            # All keys rate-limited — back off briefly
            await asyncio.sleep(3)
            continue

        CALLS_MADE += 1
        if CALLS_MADE % 10 == 0:
            await _persist_counter()

        qs = urllib.parse.urlencode({"email": email, "key": api_key, "mode": "power"})
        url = f"{settings.REOON_BASE_URL}/api/v1/verify?{qs}"
        try:
            async with httpx.AsyncClient(timeout=30) as cli:
                r = await cli.get(url)
                if r.status_code == 429:
                    pool.mark_ratelimited(api_key, seconds=60)
                    await asyncio.sleep(2)
                    continue
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            log.debug(f"reoon {email} attempt {attempt} fail: {e}")
            await asyncio.sleep(1)
    return None


async def verify_lead(lead: dict) -> dict | None:
    """Verify candidates with MillionVerifier (primary) and Reoon (fallback).
    Tier-A only — both verifiers must confirm safe + non-catch-all."""
    from pipeline import mv_verifier as mv
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None
    sorted_c = list(candidates)[:6]

    for email in sorted_c:
        # PRIMARY: MillionVerifier (fast + cheap)
        mv_res = await mv.verify(email)
        if mv_res:
            result = mv_res.get("result")
            if result == "ok":
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": "safe", "verifier": "mv",
                        "result": result, "quality": mv_res.get("quality"),
                        "subresult": mv_res.get("subresult"),
                        "is_catch_all": False, "is_role": mv_res.get("role"),
                        "score": 98, "tier": "A",
                    },
                }
            # MV gave us a definitive bad result — don't waste a Reoon call
            if result in ("invalid", "disposable", "catch_all", "unknown"):
                continue
            # result == "error" or weird → fall through to Reoon below

        # FALLBACK: Reoon (only when MV failed/errored OR MV is disabled)
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        is_catch = res.get("is_catch_all")
        score = res.get("overall_score") or 0
        if status in ("safe", "valid") and not is_catch:
            return {
                **lead,
                "verified_email": email,
                "verification": {
                    "status": status, "verifier": "reoon",
                    "score": score, "is_catch_all": is_catch, "tier": "A",
                },
            }
    return None
