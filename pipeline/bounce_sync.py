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
import asyncio
import logging
from datetime import datetime
import httpx
from sqlalchemy import select, update
from config import settings
from db import SessionLocal, VerifiedLead

log = logging.getLogger("bounce_sync")

INSTANTLY_BASE = "https://api.instantly.ai/api/v2"

# Instantly ue_type values:
#   1 = sent, 2 = reply received, 3 = bounce, 4 = opt-out / unsubscribe, 5 = complaint
BOUNCE_UE_TYPE = 3

# Instantly enforces ~20 requests/minute. Sleep between pages to stay under it.
_PAGE_DELAY_SECONDS = 3.5


def _headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}"}


def _bounced_address(it: dict) -> str | None:
    """Extract the bounced lead's address from an Instantly email event.

    CRITICAL: `to_address_email` is ALWAYS null in the v2 payload (the field
    doesn't even exist). The lead's address lives in `lead` (primary) and is
    mirrored in `to_address_email_list`. We read those instead.
    """
    lead = (it.get("lead") or "").lower().strip()
    if lead and "@" in lead:
        return lead
    tal = it.get("to_address_email_list")
    if isinstance(tal, str):
        cand = tal.split(",")[0].lower().strip()
        if cand and "@" in cand:
            return cand
    elif isinstance(tal, list) and tal:
        cand = (tal[0] or "").lower().strip()
        if cand and "@" in cand:
            return cand
    return None


def _mask(addr: str) -> str:
    local, _, dom = (addr or "").partition("@")
    return f"{local[:2]}***@{dom}" if dom else (addr or "")


async def _fetch_bounces_for_key(api_key: str, key_label: str, limit_pages: int,
                                 seen_emails: set[str], stats: dict) -> int:
    """Pull bounces for a single Instantly key. Returns newly-marked count.

    The `ue_type` query param does NOT filter server-side, so we pull the
    recent feed unfiltered and keep only `ue_type == 3` (bounce) events,
    reading the bounced address from `lead`, matching client-side.

    `stats` accumulates diagnostics: events_seen, matched, unmatched_samples.
    """
    newly_bounced = 0
    try:
        async with httpx.AsyncClient(timeout=30, headers=_headers(api_key)) as cli:
            cursor: str | None = None
            pages = 0
            while pages < limit_pages:
                params: dict = {"limit": 100}
                if cursor:
                    params["starting_after"] = cursor

                r = await cli.get(f"{INSTANTLY_BASE}/emails", params=params)
                if r.status_code == 429:
                    log.warning(f"Instantly rate limit (key={key_label}) — backing off")
                    await asyncio.sleep(8)
                    continue
                if r.status_code != 200:
                    log.warning(f"Instantly bounce fetch HTTP {r.status_code} "
                                f"(key={key_label})")
                    break

                data = r.json()
                items = data.get("items", []) if isinstance(data, dict) else []
                if not items:
                    break

                # Keep only real bounce events; read the address from `lead`.
                bounce_targets: set[str] = set()
                for it in items:
                    if it.get("ue_type") != BOUNCE_UE_TYPE:
                        continue
                    stats["events_seen"] += 1
                    addr = _bounced_address(it)
                    if addr:
                        bounce_targets.add(addr)

                if bounce_targets:
                    async with SessionLocal() as s:
                        for email in bounce_targets:
                            if email in seen_emails:
                                continue
                            seen_emails.add(email)
                            row = (await s.execute(
                                select(VerifiedLead).where(
                                    VerifiedLead.email == email,
                                )
                            )).scalar_one_or_none()
                            if row is None:
                                # Bounced in Instantly but not in our DB (e.g. uploaded
                                # from an older export / already purged). Record sample.
                                if len(stats["unmatched_samples"]) < 12:
                                    stats["unmatched_samples"].append(_mask(email))
                                stats["unmatched_in_db"] += 1
                                continue
                            if row.bounced:
                                stats["already_marked"] += 1
                                continue
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
                await asyncio.sleep(_PAGE_DELAY_SECONDS)
    except Exception:
        log.exception(f"Bounce sync error (key={key_label})")
    return newly_bounced


async def fetch_bounces_detailed(limit_pages: int = 5) -> dict:
    """Like fetch_bounces but returns full diagnostics.

    Returns {newly_bounced, events_seen, already_marked, unmatched_in_db,
             unmatched_samples, accounts}.
    """
    keys = settings.instantly_keys()
    stats = {"newly_bounced": 0, "events_seen": 0, "already_marked": 0,
             "unmatched_in_db": 0, "unmatched_samples": [], "accounts": len(keys)}
    if not keys:
        return stats

    seen_emails: set[str] = set()  # shared across keys so we never double-count
    for i, key in enumerate(keys, start=1):
        stats["newly_bounced"] += await _fetch_bounces_for_key(
            key, f"key{i}", limit_pages, seen_emails, stats)
    return stats


async def fetch_bounces(limit_pages: int = 5) -> int:
    """Pull recent bounce events from ALL configured Instantly accounts and mark
    matching leads in DB. Returns count of newly-marked-bounced leads."""
    keys = settings.instantly_keys()
    if not keys:
        return 0

    stats = await fetch_bounces_detailed(limit_pages=limit_pages)
    newly_bounced = stats["newly_bounced"]
    log.info(f"Bounce sync diag: events_seen={stats['events_seen']} "
             f"new={newly_bounced} already={stats['already_marked']} "
             f"not_in_db={stats['unmatched_in_db']}")

    if newly_bounced:
        log.info(f"Bounce sync: marked {newly_bounced} lead(s) as bounced "
                 f"across {len(keys)} Instantly account(s)")
    return newly_bounced
