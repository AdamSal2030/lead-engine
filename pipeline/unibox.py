from __future__ import annotations
"""Instantly Unibox integration.
Periodically pulls replies (ue_type=2) and marks matching leads as 'responded' in DB.
"""
import asyncio
import httpx
import logging
from datetime import datetime
from sqlalchemy import select, update, insert
from config import settings
from db import SessionLocal, VerifiedLead, Counter

log = logging.getLogger("unibox")

INSTANTLY_BASE = "https://api.instantly.ai/api/v2"


def _headers() -> dict:
    return {"Authorization": f"Bearer {settings.INSTANTLY_API_KEY}"}


async def fetch_replies(limit_pages: int = 5) -> int:
    """Fetch recent reply emails (ue_type=2) from Instantly unibox.
    Match by from_address against verified_leads.email. Mark them responded.
    Returns count of newly-marked-responded leads."""
    if not settings.INSTANTLY_API_KEY:
        return 0

    newly_responded = 0
    cursor: str | None = None
    pages = 0
    seen_leads: set[str] = set()

    try:
        async with httpx.AsyncClient(timeout=30, headers=_headers()) as cli:
            while pages < limit_pages:
                params = {"limit": 100, "ue_type": 2}
                if cursor:
                    params["starting_after"] = cursor
                r = await cli.get(f"{INSTANTLY_BASE}/emails", params=params)
                if r.status_code != 200:
                    log.warning(f"Instantly emails fetch HTTP {r.status_code}")
                    break
                data = r.json()
                items = data.get("items", []) if isinstance(data, dict) else []
                if not items:
                    break

                # Collect reply senders (the LEAD who replied)
                reply_senders: set[str] = set()
                for it in items:
                    # ue_type=2 means it's a reply received — `from_address_email` is the lead's email
                    from_addr = (it.get("from_address_email") or "").lower().strip()
                    if from_addr and "@" in from_addr:
                        reply_senders.add(from_addr)

                # Match against our DB
                if reply_senders:
                    async with SessionLocal() as s:
                        # Find verified leads matching these reply senders
                        for email in reply_senders:
                            if email in seen_leads:
                                continue
                            seen_leads.add(email)
                            row = (await s.execute(
                                select(VerifiedLead).where(
                                    VerifiedLead.email == email,
                                    VerifiedLead.responded == False,
                                )
                            )).scalar_one_or_none()
                            if row:
                                await s.execute(update(VerifiedLead).where(
                                    VerifiedLead.id == row.id
                                ).values(responded=True, responded_at=datetime.utcnow()))
                                newly_responded += 1
                        await s.commit()

                cursor = data.get("next_starting_after") if isinstance(data, dict) else None
                if not cursor or len(items) < 100:
                    break
                pages += 1

        if newly_responded:
            log.info(f"Unibox sync: marked {newly_responded} new lead(s) as responded")
    except Exception as e:
        log.exception(f"Unibox sync error: {e}")

    return newly_responded


async def unibox_loop():
    """Background task: poll unibox periodically for replies."""
    if not settings.INSTANTLY_API_KEY:
        log.info("Unibox sync disabled (no INSTANTLY_API_KEY)")
        return
    log.info(f"Unibox sync loop started (every {settings.INSTANTLY_SYNC_INTERVAL_MINUTES}m)")
    # First sync after 60s grace period (let app finish booting)
    await asyncio.sleep(60)
    while True:
        try:
            await fetch_replies(limit_pages=5)
        except Exception as e:
            log.exception(f"unibox_loop iteration failed: {e}")
        await asyncio.sleep(settings.INSTANTLY_SYNC_INTERVAL_MINUTES * 60)


async def get_reply_insights() -> dict:
    """Deep-dive analysis of who's replying — to find lookalike patterns.
    Returns:
      - top sources (where the repliers came from)
      - free-email vs business breakdown
      - top company keywords (niche clustering)
      - role patterns
    """
    from sqlalchemy import func as sql_func
    from collections import Counter as PyCounter
    async with SessionLocal() as s:
        responders = (await s.execute(
            select(VerifiedLead).where(VerifiedLead.responded == True)
        )).scalars().all()

    if not responders:
        return {"total": 0, "msg": "No replies tracked yet. Run /unibox/sync to pull recent."}

    # Domain breakdown: free providers vs business
    FREE = {"gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com","aol.com",
            "protonmail.com","proton.me","me.com","mac.com","live.com"}
    free_count = sum(1 for r in responders if r.email and r.email.split("@")[-1].lower() in FREE)
    biz_count = len(responders) - free_count

    # Top sources
    sources_c = PyCounter(r.source or "?" for r in responders)
    # Top roles
    roles_c = PyCounter((r.role or "Unknown").strip() for r in responders)
    # Top niche keywords from website DOMAINS (company field is unreliable text-capture).
    # Strip TLD, split on common camelcase / hyphen / number boundaries.
    import re
    STOP = {
        # articles, prepositions, conjunctions
        "the","and","of","for","to","in","on","at","by","a","an","is","with","or","but",
        "from","this","that","not","are","was","were","been","have","has","had","do","does",
        "did","will","would","should","could","can","may","might","must","shall","they","them",
        "their","there","these","those","what","which","who","whom","whose","when","where","why",
        "how","all","any","each","every","other","some","such","only","own","same","than",
        "too","very","also","just","even","though","still","like","over","under","through",
        # super common english
        "high","low","big","small","large","old","new","good","best","top","high","main",
        "people","things","time","year","years","day","days","life","work","home","way",
        "make","made","get","got","take","took","know","knew","think","thought","see","saw",
        "come","came","go","went","find","found","look","said","say","ask","help","want",
        "need","try","use","used","feel","felt","right","left","first","last","next","both",
        # business filler that's noise not signal
        "llc","inc","corp","ltd","group","studio","agency","company","companies","co",
        "global","world","international","national","local","online","digital","virtual",
        "solutions","services","systems","management","consulting","center","centre",
        "professional","experience","experiences","brand","brands","official","website",
        "business","businesses","brand","marketing","sales","store","shop","website",
        # geography / cities (high frequency, low signal for niche)
        "houston","dallas","austin","chicago","seattle","boston","denver","atlanta",
        "miami","phoenix","orlando","portland","detroit","minnesota","oklahoma",
        "michigan","california","arizona","texas","florida","newyork","losangeles","ny","la",
        # noise
        "stre","explained","cone",
    }
    word_c = PyCounter()
    for r in responders:
        if not r.website: continue
        host = r.website.lower().replace("https://","").replace("http://","").split("/")[0].replace("www.","")
        # Strip TLD: take everything before the last dot-segment
        parts = host.split(".")
        if len(parts) < 2: continue
        slug = ".".join(parts[:-1])  # keep middle dots if multi-level domain
        # Tokenize: split on non-letters AND on lowerCamel boundaries
        # First split on non-letter chars
        tokens = re.findall(r"[a-z]{3,20}", slug)
        # Then break camelCase-like sequences (these are rare in domains but safe)
        for w in tokens:
            if w not in STOP and not w.isnumeric():
                word_c[w] += 1

    # Top company TLDs
    tld_c = PyCounter()
    for r in responders:
        if r.website:
            host = r.website.lower().replace("https://", "").replace("http://", "").split("/")[0].replace("www.", "")
            parts = host.split(".")
            if len(parts) >= 2:
                tld_c["." + parts[-1]] += 1

    return {
        "total_responders": len(responders),
        "domain_split": {
            "free_provider": free_count,
            "business_domain": biz_count,
            "free_pct": round(100 * free_count / len(responders), 1),
        },
        "top_sources": sources_c.most_common(10),
        "top_roles": roles_c.most_common(8),
        "top_niche_keywords": word_c.most_common(20),
        "top_tlds": tld_c.most_common(10),
        "sample_responders": [
            {"name": r.name, "email": r.email, "company": r.company,
             "website": r.website, "source": r.source,
             "responded_at": r.responded_at.isoformat() if r.responded_at else None}
            for r in sorted(responders, key=lambda x: -(x.responded_at.timestamp() if x.responded_at else 0))[:20]
        ],
    }


async def get_reply_stats() -> dict:
    """Return per-source reply stats for dashboard."""
    from sqlalchemy import func as sql_func, case
    async with SessionLocal() as s:
        total = (await s.execute(select(sql_func.count()).select_from(VerifiedLead))).scalar_one() or 0
        responded = (await s.execute(
            select(sql_func.count()).select_from(VerifiedLead).where(VerifiedLead.responded == True)
        )).scalar_one() or 0
        # Per-source breakdown — sum 1/0 via CASE
        responded_case = case((VerifiedLead.responded == True, 1), else_=0)
        per_source = (await s.execute(
            select(VerifiedLead.source,
                   sql_func.count(VerifiedLead.id),
                   sql_func.sum(responded_case))
            .group_by(VerifiedLead.source)
        )).all()
    return {
        "total_leads": total,
        "total_responded": responded,
        "reply_rate": (100.0 * responded / total) if total else 0.0,
        "per_source": [
            {"source": src or "?", "total": int(t or 0), "responded": int(r or 0),
             "reply_rate": (100.0 * (r or 0) / (t or 1))}
            for src, t, r in per_source
        ],
    }
