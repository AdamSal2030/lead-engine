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


_ROLE_LOCALS = {"founder", "ceo", "owner", "cto", "coo", "president",
                "director", "md", "gm", "partner", "principal",
                "chief", "cofounder", "co-founder", "operator",
                "head", "managing", "exec"}


def _is_acceptable_email(email: str, name: str) -> bool:
    """True if email is worth including (personal OR role-based)."""
    local = email.split("@")[0].lower()
    if local in _ROLE_LOCALS:
        return True  # founder@, ceo@, owner@ are decision-maker addresses
    return _is_personal_email(email, name)


async def verify_lead(lead: dict) -> dict | None:
    """Verify email candidates using MillionVerifier (primary) then Reoon (fallback).

    Acceptance tiers:
      Tier A (ok):        MV result="ok" — confirmed deliverable
      Tier A (catch-all): MV result="catch_all" + email is personal OR role-based
                          (founder@, ceo@, owner@ etc.) — domain catches all, email
                          format is plausible for the decision-maker
      Tier A (unknown):   MV result="unknown" + email is personal → try Reoon;
                          if Reoon also says safe/valid → accept
      Reoon fallback:     MV disabled/errored → use Reoon safe/valid result

    We try up to 15 candidates; ranking already puts best ones first.
    """
    from pipeline import mv_verifier as mv
    candidates = lead.get("email_candidates", [])
    if not candidates:
        return None

    sorted_c = list(candidates)[:15]
    founder_name = lead.get("name", "")

    for email in sorted_c:
        # Hard gate: never select generic role addresses (info@, support@, contact@, etc.)
        # as the outreach email regardless of verification result — they go to support
        # desks, not founders, and trigger spam complaints.
        if not _is_acceptable_email(email, founder_name):
            continue

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

            # Catch-all domain — accept if email is personal OR a decision-maker
            # role address. Small-biz domains are often catch-all; these addresses
            # reach real people even if SMTP can't confirm the exact mailbox.
            if result == "catch_all" and _is_acceptable_email(email, founder_name):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": "catch_all", "verifier": "mv",
                        "result": result, "quality": mv_res.get("quality"),
                        "is_catch_all": True, "score": 70, "tier": "A",
                    },
                }

            # Unknown — MV couldn't reach SMTP server. Don't hard-reject;
            # fall through to Reoon which uses multiple IPs/methods.
            if result == "unknown":
                pass  # fall through to Reoon below

            elif result in ("invalid", "disposable"):
                continue  # hard reject — confirmed non-existent, skip Reoon

        # FALLBACK: Reoon (when MV failed, errored, or gave unknown/catch_all-rejected)
        res = await verify_email(email)
        if not res:
            continue
        status = res.get("status")
        is_catch = res.get("is_catch_all")
        score = res.get("overall_score") or 0

        # Accept safe/valid: if catch-all, still accept if email looks acceptable
        if status in ("safe", "valid"):
            if not is_catch or _is_acceptable_email(email, founder_name):
                return {
                    **lead,
                    "verified_email": email,
                    "verification": {
                        "status": status, "verifier": "reoon",
                        "score": score, "is_catch_all": is_catch, "tier": "A",
                    },
                }

        # Reoon "risky" — accept on catch-all domains if email looks personal/role.
        # "risky" from Reoon usually just means catch-all, not truly bad.
        if status == "risky" and is_catch and _is_acceptable_email(email, founder_name):
            return {
                **lead,
                "verified_email": email,
                "verification": {
                    "status": "risky_catch_all", "verifier": "reoon",
                    "score": score, "is_catch_all": True, "tier": "A",
                },
            }

        # Reoon "risky" on non-catch-all — accept if the email clearly contains
        # the founder's name. Small-biz domains often have misconfigured SPF/MX
        # which makes Reoon rate them risky even when the mailbox is real.
        if status == "risky" and not is_catch and _is_personal_email(email, founder_name):
            return {
                **lead,
                "verified_email": email,
                "verification": {
                    "status": "risky_personal", "verifier": "reoon",
                    "score": max(score, 55), "is_catch_all": False, "tier": "A",
                },
            }

        # Reoon "unknown" — SMTP server didn't respond / inconclusive. Accept if
        # the email looks like a real personal address (name-based local part).
        # These are worth the occasional bounce — missing them costs more leads.
        if status == "unknown" and _is_personal_email(email, founder_name):
            return {
                **lead,
                "verified_email": email,
                "verification": {
                    "status": "unknown_personal", "verifier": "reoon",
                    "score": 50, "is_catch_all": is_catch, "tier": "A",
                },
            }

    return None
