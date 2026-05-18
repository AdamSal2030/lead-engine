from __future__ import annotations
"""Instantly bounce tracking.

Polls the Instantly unibox for bounced / invalid emails (ue_type=3)
and marks matching verified leads as bounced in our DB.

Why this matters: bounce rate per niche and source is the primary signal
for the intelligence engine. A niche with 30% bounce = bad targeting or
poor email quality. A niche with 3% bounce + good reply rate = winner.

Runs inside the unibox_loop (same interval as reply sync) and on demand
via GET /unibox/sync.
"""
import logging
from datetime import datetime
import httpx
from sqlalchemy import select, update
from config import settings
from db import SessionLocal, VerifiedLead

log = logging.getLogger("bounce_sync")

INSTANTLY_BASE = "https://api.instantly.ai/api/v2"

# Instantly ue_type values (from their docs):
#   1 = sent, 2 = reply received, 3 = bounce, 4 = opt-out / unsubscribe, 5 = complaint
BOUNCE_UE_TYPES = [3]


def _headers() -> dict:
    return {"Authorization": f"Bearer {settings.INSTANTLY_API_KEY}"}


async def fetch_bounces(limit_pages: int = 5) -> int:
    """Pull recent bounce events from Instantly and mark leads in DB.

    Returns count of newly-marked-bounced leads.
    """
    if not settings.INSTANTLY_API_KEY:
        return 0

    newly_bounced = 0
    seen_emails: set[str] = set()

    try:
        async with httpx.AsyncClient(timeout=30, headers=_headers()) as cli:
            for ue_type in BOUNCE_UE_TYPES:
                cursor: str | None = None
                pages = 0

                while pages < limit_pages:
                    params: dict = {"limit": 100, "ue_type": ue_type}
                    if cursor:
                        params["starting_after"] = cursor

                    r = await cli.get(f"{INSTANTLY_BASE}/emails", params=params)
                    if r.status_code != 200:
                        log.warning(f"Instantly bounce fetch HTTP {r.status_code} (ue_type={ue_type})")
                        break

                    data = r.json()
                    items = data.get("items", []) if isinstance(data, dict) else []
                    if not items:
                        break

                    # For bounce events the `to_address_email` is the lead that bounced
                    bounce_targets: set[str] = set()
                    for it in items:
                        to_addr = (it.get("to_address_email") or "").lower().strip()
                        if to_addr and "@" in to_addr:
                            bounce_targets.add(to_addr)

                    if bounce_targets:
                        async with SessionLocal() as s:
                            for email in bounce_targets:
                                if email in seen_emails:
                                    continue
                                seen_emails.add(email)
                                row = (await s.execute(
                                    select(VerifiedLead).where(
                                        VerifiedLead.email == email,
                                        VerifiedLead.bounced == False,
                                    )
                                )).scalar_one_or_none()
                                if row:
                                    await s.execute(
                                        update(VerifiedLead)
                                        .where(VerifiedLead.id == row.id)
                                        .values(bounced=True, bounced_at=datetime.utcnow())
                                    )
                                    newly_bounced += 1
                            await s.commit()

                    cursor = data.get("next_starting_after") if isinstance(data, dict) else None
                    if not cursor or len(items) < 100:
                        break
                    pages += 1

        if newly_bounced:
            log.info(f"Bounce sync: marked {newly_bounced} lead(s) as bounced")

    except Exception:
        log.exception("Bounce sync error")

    return newly_bounced
