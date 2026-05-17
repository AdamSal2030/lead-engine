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
