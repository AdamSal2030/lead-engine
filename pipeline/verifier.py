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
    """True if the email local part looks like a real person's address (not generic).

    Priority:
      1. Name match — local contains the founder's first or last name → definitely personal
      2. Generic/business word list → definitely not personal
      3. Fallback heuristic: only accept if local looks like a person's name token
         (short, letters-only, no generic business words)
    """
    local = email.split("@")[0].lower()
    # Broad generic set — any email with these local parts is a role address, not personal
    generic = {
        "info", "hello", "hi", "contact", "contactus", "support", "team", "admin", "office",
        "sales", "help", "press", "media", "hr", "jobs", "careers",
        "noreply", "no-reply", "donotreply", "mail", "enquiries", "enquiry",
        "webmaster", "postmaster", "hostmaster", "abuse", "newsletter",
        "marketing", "billing", "accounting", "legal", "privacy", "security",
        "partnership", "partnerships", "affiliate", "affiliates", "service",
        "services", "general", "reception", "booking", "reservations",
        "customerservice", "customer", "feedback", "inquiry", "inquiries",
        "business", "guestservices", "onlinesupport", "information",
        "servicedesk", "helpdesk", "clientsolutions", "engage", "gradadm",
        "technical", "techsupport", "technicalsupport", "admissions",
        "coaching", "coach", "studio", "management", "operations",
    }
    # Compound generic check — catches "support.ca" → "supportca", "technical-support", etc.
    local_stripped = re.sub(r"[._\-+]", "", local)
    if local in generic or local_stripped in generic:
        return False

    _GENERIC_PREFIXES = ("info", "support", "service", "contact", "sales",
                         "noreply", "donotreply", "billing", "marketing", "booking",
                         "helpdesk", "servicedesk", "customerservice", "technical")
    if any(local_stripped.startswith(pfx) for pfx in _GENERIC_PREFIXES):
        return False

    # Check if name tokens appear in local part (strongest signal)
    if name:
        parts = name.lower().split()
        first = re.sub(r"[^a-z]", "", parts[0]) if parts else ""
        last = re.sub(r"[^a-z]", "", parts[-1]) if len(parts) > 1 else ""
        if (first and len(first) >= 3 and first in local):
            return True
        if (last and len(last) >= 3 and last in local):
            return True

    # Fallback: treat as personal only if local looks like a name token
    # (letters-only or letters with common separators, reasonable length, no leading digits)
    local_alpha = re.sub(r"[._\-]", "", local)
    if any(c.isdigit() for c in local[:2]):
        return False  # starts with digit(s) → account/generated email
    if not local_alpha.isalpha():
        return False  # contains unexpected chars → not a simple name
    if len(local_alpha) < 3 or len(local_alpha) > 25:
        return False  # too short or suspiciously long
    # Final word check — reject common non-name English words
    non_name_words = {
        "news", "blog", "shop", "store", "app", "web", "site", "page",
        "mail", "email", "post", "work", "home", "base", "hub", "central",
        "connect", "link", "network", "group", "club", "pro", "studio",
        "media", "digital", "global", "local", "official", "real", "best",
        "top", "new", "now", "live", "care", "care", "tech", "code",
    }
    if local_alpha in non_name_words:
        return False
    return True


def _is_acceptable_email(email: str, name: str) -> bool:
    """True if this email is worth attempting verification.
    Blocks generic role/dept addresses that go to support desks, not founders.
    """
    return _is_personal_email(email, name)


async def verify_lead(lead: dict) -> dict | None:
    """Verify email candidates. Strict rules to minimise bounces and wasted credits.

    Acceptance tiers (tightest-first):
      Tier A (ok):         MV "ok" — SMTP-confirmed deliverable. Always accept.
      Tier A (catch-all):  MV "catch_all" + email local part contains founder's
                           first OR last name. Catch-all means the domain accepts
                           anything, but we require a name match to avoid sending
                           to made-up addresses like founder@domain.com.
      Reoon safe/valid:    MV unavailable/unknown → Reoon confirms safe/valid.
                           Non-catch-all: always accept.
                           Catch-all: require name match (same rule as MV catch-all).

    REMOVED to cut bounces:
      - Role emails on catch-all (founder@, ceo@) → bounce too often
      - Reoon "risky" acceptance → risky means risky
      - Reoon "unknown" acceptance → too uncertain

    We try up to 8 candidates (down from 15) — most hits come in top 3.
    """
    from pipeline import mv_verifier as mv
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None

    # Only check personal-looking emails — skip generic/role addresses entirely
    personal = [e for e in candidates if _is_personal_email(e, lead.get("name", ""))]
    if not personal:
        return None

    sorted_c = personal[:8]   # top 8 personal candidates, best-ranked first
    founder_name = lead.get("name", "")

    def _name_in_local(email: str) -> bool:
        """True if founder first or last name appears in the email local part."""
        local = email.split("@")[0].lower()
        parts = re.sub(r"[^a-z ]", "", founder_name.lower()).split()
        first = parts[0] if parts else ""
        last = parts[-1] if len(parts) > 1 else ""
        return (
            (first and len(first) >= 3 and first in local) or
            (last and len(last) >= 3 and last in local)
        )

    for email in sorted_c:
        # PRIMARY: MillionVerifier
        mv_res = await mv.verify(email)
        if mv_res:
            result = mv_res.get("result")

            # Best case — SMTP-confirmed deliverable
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

            # Catch-all — ONLY accept if catch-all is enabled AND the founder's
            # name is in the local part. Catch-all domains accept any address at
            # SMTP time, so they're the #1 silent-bounce source. Gated off by
            # default (settings.ACCEPT_CATCH_ALL=False).
            # "jane@acme.com" for Jane Smith → accept (when enabled).
            # "founder@acme.com" or "jsmith@unknownco.com" → skip (too risky).
            if result == "catch_all" and settings.ACCEPT_CATCH_ALL and _name_in_local(email):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": "catch_all", "verifier": "mv",
                        "result": result, "quality": mv_res.get("quality"),
                        "is_catch_all": True, "score": 72, "tier": "A",
                    },
                }

            if result in ("invalid", "disposable", "catch_all"):
                continue  # hard reject or catch-all without name match — skip Reoon

            # "unknown" — fall through to Reoon

        # FALLBACK: Reoon
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        is_catch = res.get("is_catch_all")
        score = res.get("overall_score") or 0

        if status in ("safe", "valid"):
            # Non-catch-all: accept freely.
            # Catch-all: only when enabled AND name in local (else skip — bounce risk).
            if not is_catch or (settings.ACCEPT_CATCH_ALL and _name_in_local(email)):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": status, "verifier": "reoon",
                        "score": score, "is_catch_all": is_catch, "tier": "A",
                    },
                }

    return None
