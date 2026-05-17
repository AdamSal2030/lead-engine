from __future__ import annotations
"""MillionVerifier wrapper.
Primary verifier — 1000 RPM, ~$0.0014/email, SMTP-level verification.

Result mapping:
  result="ok"         → Tier-A safe ✓
  result="catch_all"  → reject (catch-alls bounce in our setup)
  result="disposable" → reject
  result="invalid"    → reject
  result="unknown"    → reject (don't risk it)
  result="error"      → return None (caller may fall back to Reoon)
"""
import asyncio
import logging
import urllib.parse
import httpx
from sqlalchemy import select, insert, update
from config import settings
from db import SessionLocal, Counter

log = logging.getLogger("mv_verifier")

CALLS_MADE = 0
HITS = 0  # 'ok' results
_counter_loaded = False
_quota_exhausted = False
_lock = asyncio.Lock()


async def _load_counter():
    global CALLS_MADE, HITS, _counter_loaded
    if _counter_loaded: return
    async with _lock:
        if _counter_loaded: return
        async with SessionLocal() as s:
            c = (await s.execute(select(Counter).where(Counter.key == "mv_calls"))).scalar_one_or_none()
            CALLS_MADE = c.value if c else 0
            h = (await s.execute(select(Counter).where(Counter.key == "mv_hits"))).scalar_one_or_none()
            HITS = h.value if h else 0
        _counter_loaded = True


async def _persist_counter():
    try:
        async with SessionLocal() as s:
            for key, val in [("mv_calls", CALLS_MADE), ("mv_hits", HITS)]:
                row = (await s.execute(select(Counter).where(Counter.key == key))).scalar_one_or_none()
                if row:
                    await s.execute(update(Counter).where(Counter.key == key).values(value=val))
                else:
                    await s.execute(insert(Counter).values(key=key, value=val))
            await s.commit()
    except Exception:
        pass


async def verify(email: str, retries: int = 2) -> dict | None:
    """Returns MV response dict or None on hard failure.
    Caller should check result == 'ok' for Tier-A."""
    global CALLS_MADE, HITS, _quota_exhausted
    if not settings.MV_API_KEY or _quota_exhausted:
        return None
    await _load_counter()

    url = f"https://api.millionverifier.com/api/v3/?api={settings.MV_API_KEY}&email={urllib.parse.quote(email)}&timeout=10"
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=25) as cli:
                r = await cli.get(url)
            if r.status_code == 200:
                data = r.json()
                # MV's success looks like: {"result": "ok", "credits": 52999, ...}
                if "result" in data:
                    CALLS_MADE += 1
                    if data["result"] == "ok":
                        HITS += 1
                    if CALLS_MADE % 10 == 0:
                        await _persist_counter()
                    # Check for credit exhaustion
                    if data.get("credits", 1) == 0:
                        log.warning("MillionVerifier credits exhausted — disabling")
                        _quota_exhausted = True
                    return data
                # No 'result' usually means an error
                if "error" in data:
                    log.debug(f"MV API error for {email}: {data.get('error')}")
                return None
            if r.status_code == 402 or r.status_code == 403:
                log.warning(f"MV quota / auth issue: HTTP {r.status_code}")
                _quota_exhausted = True
                return None
            if r.status_code == 429:
                await asyncio.sleep(2)
                continue
        except Exception as e:
            log.debug(f"mv {email} attempt {attempt} fail: {e}")
            await asyncio.sleep(1)
    return None


def get_state() -> dict:
    return {
        "enabled": bool(settings.MV_API_KEY) and not _quota_exhausted,
        "quota_exhausted": _quota_exhausted,
        "calls": CALLS_MADE,
        "hits": HITS,
        "hit_rate": (100.0 * HITS / CALLS_MADE) if CALLS_MADE else 0.0,
    }
