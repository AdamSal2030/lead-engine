from __future__ import annotations
"""Skrapp email-finder wrapper.

Layer 4 in our cost-tiered pipeline. Fires only when:
  - SKRAPP_API_KEY is set
  - Quota not exhausted (circuit breaker)
  - We have a real first+last name and a small-biz domain
  - We've already tried free extraction and got nothing strong

Caches results per (domain, first, last) to never double-charge.
"""
import asyncio
import logging
import urllib.parse
import httpx
from sqlalchemy import select, insert, update
from config import settings
from db import SessionLocal, Counter, Base
from sqlalchemy import Column, Integer, String, DateTime, Boolean

log = logging.getLogger("finder")

# --- Module state ---
_quota_exhausted = False  # circuit breaker — flips True after 401/403, never fires until restart
_counter_loaded = False
SKRAPP_CALLS = 0
SKRAPP_HITS = 0  # successful finds (got an email back)
_lock = asyncio.Lock()

# Catch-all giants — Skrapp can't give us a real mailbox here
MEGA_DOMAINS = {
    "forbes.com","yelp.com","deloitte.com","wipro.com","kaplan.com.sg","amazon.com",
    "google.com","apple.com","microsoft.com","ibm.com","oracle.com","accenture.com",
    "kpmg.com","ey.com","pwc.com","mckinsey.com","bain.com","bcg.com","weforum.org",
    "newyorkfed.org","theglobeandmail.com","cnn.com","bbc.com","nyt.com","wsj.com",
    "ssrn.com","linkedin.com","facebook.com","meta.com","valiantceo.com","tesla.com",
    "ceomonthly.com","thefounderhour.com","businessinsider.com","reuters.com",
    "techcrunch.com","verge.com","wired.com","gizmodo.com",
}


async def _load_counter():
    """Load persistent Skrapp call counter from DB."""
    global SKRAPP_CALLS, SKRAPP_HITS, _counter_loaded
    if _counter_loaded:
        return
    async with _lock:
        if _counter_loaded:
            return
        async with SessionLocal() as s:
            c = (await s.execute(select(Counter).where(Counter.key == "skrapp_calls"))).scalar_one_or_none()
            SKRAPP_CALLS = c.value if c else 0
            h = (await s.execute(select(Counter).where(Counter.key == "skrapp_hits"))).scalar_one_or_none()
            SKRAPP_HITS = h.value if h else 0
        _counter_loaded = True


async def _persist():
    try:
        async with SessionLocal() as s:
            for key, val in [("skrapp_calls", SKRAPP_CALLS), ("skrapp_hits", SKRAPP_HITS)]:
                row = (await s.execute(select(Counter).where(Counter.key == key))).scalar_one_or_none()
                if row:
                    await s.execute(update(Counter).where(Counter.key == key).values(value=val))
                else:
                    await s.execute(insert(Counter).values(key=key, value=val))
            await s.commit()
    except Exception:
        pass


# --- Persistent cache table ---
class SkrappCache(Base):
    """Per-(domain, first, last) cache so we never re-spend a credit."""
    __tablename__ = "skrapp_cache"
    id = Column(Integer, primary_key=True)
    cache_key = Column(String(300), unique=True, index=True, nullable=False)
    email = Column(String(200))     # empty if Skrapp had no match
    quality = Column(String(40))    # 'valid' / 'catch-all' / 'unknown' / '' on miss
    pattern = Column(String(80))


def _cache_key(first: str, last: str, domain: str) -> str:
    return f"{domain.lower()}|{first.lower()}|{last.lower()}"


async def _cache_lookup(first: str, last: str, domain: str):
    async with SessionLocal() as s:
        row = (await s.execute(
            select(SkrappCache).where(SkrappCache.cache_key == _cache_key(first, last, domain))
        )).scalar_one_or_none()
        return row


async def _cache_save(first: str, last: str, domain: str, email: str | None, quality: str | None, pattern: str | None):
    try:
        async with SessionLocal() as s:
            s.add(SkrappCache(
                cache_key=_cache_key(first, last, domain),
                email=email or "",
                quality=quality or "",
                pattern=pattern or "",
            ))
            await s.commit()
    except Exception:
        pass  # likely uniqueness race — fine


def _is_eligible_domain(domain: str) -> bool:
    if not domain or "." not in domain or len(domain) > 60:
        return False
    if domain.lower() in MEGA_DOMAINS:
        return False
    if any(b in domain for b in ["wixsite", "squarespace", "weebly", "wordpress.com", "blogspot",
                                  "linktr.ee", "carrd.co"]):
        return False
    return True


async def find_email(first_name: str, last_name: str, domain: str) -> dict | None:
    """Try Skrapp once for this (first, last, domain). Cached.

    Returns {"email": ..., "quality": ..., "pattern": ..., "source": "skrapp"} or None.
    """
    global SKRAPP_CALLS, SKRAPP_HITS, _quota_exhausted

    if not settings.SKRAPP_API_KEY or not settings.SKRAPP_ENABLED:
        return None
    if _quota_exhausted:
        return None
    if not first_name or not last_name or not domain:
        return None
    if not _is_eligible_domain(domain):
        return None

    first, last = first_name.strip(), last_name.strip()
    if len(first) < 2 or len(last) < 2:
        return None

    await _load_counter()

    # Cache hit?
    cached = await _cache_lookup(first, last, domain)
    if cached:
        if cached.email:
            return {"email": cached.email, "quality": cached.quality, "pattern": cached.pattern,
                    "source": "skrapp_cache"}
        return None  # cached miss

    # Real call
    SKRAPP_CALLS += 1
    if SKRAPP_CALLS % 5 == 0:
        await _persist()

    try:
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.get(
                "https://api.skrapp.io/api/v2/find",
                params={"firstName": first, "lastName": last, "domain": domain},
                headers={"X-Access-Key": settings.SKRAPP_API_KEY},
            )
        if r.status_code in (401, 403):
            log.warning(f"Skrapp quota exhausted or unauthorized — circuit-breaking. status={r.status_code}")
            _quota_exhausted = True
            return None
        if r.status_code == 429:
            log.info("Skrapp rate-limited — skipping this call")
            return None
        if r.status_code != 200:
            log.debug(f"Skrapp HTTP {r.status_code} for {first} {last} @ {domain}")
            await _cache_save(first, last, domain, None, None, None)
            return None

        data = r.json()
        email = data.get("email")
        quality = (data.get("quality") or {}).get("status")
        pattern = data.get("pattern")

        # Cache the result (success or empty)
        await _cache_save(first, last, domain, email, quality, pattern)

        if not email:
            return None
        # We accept "valid" and "unknown" — Reoon will gate-keep next
        # Skip "catch-all" — Skrapp gave up, generic pattern guess
        if quality == "catch-all":
            return None

        SKRAPP_HITS += 1
        if SKRAPP_HITS % 5 == 0:
            await _persist()

        return {"email": email, "quality": quality, "pattern": pattern, "source": "skrapp"}
    except Exception as e:
        log.debug(f"Skrapp error: {e}")
        return None


def get_state() -> dict:
    return {
        "enabled": settings.SKRAPP_ENABLED and bool(settings.SKRAPP_API_KEY),
        "quota_exhausted": _quota_exhausted,
        "calls": SKRAPP_CALLS,
        "hits": SKRAPP_HITS,
    }
