from __future__ import annotations
"""Email verifier — MillionVerifier primary, Reoon fallback.
Accepts Tier-A leads: confirmed ok, catch-all personal emails, and Skrapp unknowns."""
import asyncio
import re
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


def _is_personal_email(email: str, name: str) -> bool:
    """True if the email local part looks like a real person's address (not generic)."""
    local = email.split("@")[0].lower()
    generic = {"info", "hello", "contact", "support", "team", "admin", "office",
               "sales", "help", "press", "media", "hr", "jobs", "careers",
               "noreply", "no-reply", "donotreply", "mail", "enquiries", "enquiry"}
    if local in generic:
        return False
    # Check if name tokens appear in local part
    if name:
        parts = name.lower().split()
        first = re.sub(r"[^a-z]", "", parts[0]) if parts else ""
        last = re.sub(r"[^a-z]", "", parts[-1]) if len(parts) > 1 else ""
        if (first and first in local) or (last and last in local):
            return True
    # Local part looks like a person (not all-generic)
    return len(local) >= 3 and not any(c.isdigit() for c in local[:3])


async def verify_lead(lead: dict) -> dict | None:
    """Verify email candidates using MillionVerifier (primary) then Reoon (fallback).

    Acceptance tiers:
      Tier A (preferred): MV result="ok" — confirmed safe, deliverable
      Tier A (catch-all): MV result="catch_all" + email looks personal → include with flag
      Tier A (unknown):   MV result="unknown" + email came from Skrapp → include (Skrapp
                          confirmed pattern, MV couldn't SMTP-check — still worth sending)
      Reoon fallback:     MV disabled/errored → use Reoon safe/valid result

    We try up to 12 candidates (was 6) to give more emails a shot.
    """
    from pipeline import mv_verifier as mv
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None

    # Try up to 12 candidates; ranking already puts best ones first
    sorted_c = list(candidates)[:12]
    founder_name = lead.get("name", "")

    for email in sorted_c:
        # PRIMARY: MillionVerifier (fast, cheap)
        mv_res = await mv.verify(email)
        if mv_res:
            result = mv_res.get("result")

            # Best case — confirmed deliverable
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

            # Catch-all domain — include if the email looks personal
            # (small-biz domains often are catch-all but the email is real)
            if result == "catch_all" and _is_personal_email(email, founder_name):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": "catch_all", "verifier": "mv",
                        "result": result, "quality": mv_res.get("quality"),
                        "is_catch_all": True, "score": 70, "tier": "A",
                    },
                }

            # Unknown — MV couldn't reach SMTP. Accept if Skrapp specifically found it.
            if result == "unknown":
                # Check if this email came from Skrapp (first in candidates = Skrapp insert)
                skrapp_email = candidates[0] if candidates else None
                if email == skrapp_email and _is_personal_email(email, founder_name):
                    return {
                        **lead,
                        "verified_email": email,
                        "verification": {
                            "status": "unknown", "verifier": "mv",
                            "result": result, "is_catch_all": None, "score": 55, "tier": "A",
                        },
                    }
                # Fall through to Reoon for unknown
            elif result in ("invalid", "disposable"):
                continue  # hard reject — don't waste a Reoon call

        # FALLBACK: Reoon (when MV failed, errored, or gave unknown)
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        is_catch = res.get("is_catch_all")
        score = res.get("overall_score") or 0

        # Accept safe/valid regardless of catch-all if email is personal
        if status in ("safe", "valid"):
            if not is_catch or _is_personal_email(email, founder_name):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": status, "verifier": "reoon",
                        "score": score, "is_catch_all": is_catch, "tier": "A",
                    },
                }

    return None
