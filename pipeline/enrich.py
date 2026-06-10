from __future__ import annotations
"""Skrapp enrichment — turn our discovered founders into REAL emails.

We already have ~23k raw leads (name + company + domain) from discovery. The old
pipeline guessed their emails (bouncy). This instead asks Skrapp's official Email
Finder for each one's REAL email (Skrapp only charges a credit when it finds one),
then MV-verifies + dedupes + niche-tags + stores. Fully automated, no browser.

Runs as a background task; progress is exposed via get_progress().
"""
import asyncio
import logging
import urllib.parse
from sqlalchemy import select, func
from db import SessionLocal, RawLead, VerifiedLead
from pipeline import finder as skrapp
from pipeline.importer import ingest_rows
from config import settings

log = logging.getLogger("enrich")

_progress: dict = {"running": False, "processed": 0, "skrapp_hits": 0,
                   "added": 0, "last_id": 0, "target": 0, "done": False, "msg": ""}


def get_progress() -> dict:
    return dict(_progress)


def _domain_of(website: str) -> str:
    if not website:
        return ""
    w = website if website.startswith("http") else "https://" + website
    try:
        return urllib.parse.urlparse(w).netloc.replace("www.", "").lower()
    except Exception:
        return ""


def _split_name(name: str) -> tuple[str, str]:
    parts = (name or "").strip().split()
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[-1]


async def _enrich_one(rl: dict, sem: asyncio.Semaphore) -> dict | None:
    """Skrapp-find the real email for one raw lead. Returns an importer row or None."""
    first, last = _split_name(rl["name"])
    domain = _domain_of(rl["website"])
    if not first or not last or not domain:
        return None
    async with sem:
        res = await skrapp.find_email(first, last, domain)
    if not res or not res.get("email"):
        return None
    return {
        "email": res["email"].lower(), "name": rl["name"],
        "first_name": first, "last_name": last,
        "company": rl.get("company") or "", "role": rl.get("role") or "",
        "website": rl.get("website") or "", "industry": "", "location": "",
    }


async def run_enrichment(limit: int = 2000, after_id: int = 0,
                         batch_commit: int = 200) -> dict:
    """Process up to `limit` raw leads (id > after_id) through Skrapp + ingest.
    Updates module-level _progress as it goes."""
    global _progress
    _progress = {"running": True, "processed": 0, "skrapp_hits": 0, "added": 0,
                 "last_id": after_id, "target": limit, "done": False, "msg": "starting"}

    if not settings.SKRAPP_API_KEY:
        _progress.update(running=False, done=True, msg="No SKRAPP_API_KEY configured")
        return get_progress()

    sem = asyncio.Semaphore(settings.SKRAPP_CONCURRENCY)
    total_added = 0
    last_id = after_id
    processed = 0

    while processed < limit:
        chunk = min(batch_commit, limit - processed)
        async with SessionLocal() as s:
            # Skip raw leads whose website we already have a verified lead for —
            # avoids spending a Skrapp credit re-finding an email we already own.
            already = select(VerifiedLead.website).where(VerifiedLead.website.isnot(None))
            rows = (await s.execute(
                select(RawLead.id, RawLead.name, RawLead.website,
                       RawLead.company, RawLead.role)
                .where(RawLead.id > last_id,
                       RawLead.name.isnot(None), RawLead.website.isnot(None),
                       RawLead.website.notin_(already))
                .order_by(RawLead.id).limit(chunk)
            )).all()
        if not rows:
            break

        raws = [{"id": r[0], "name": r[1], "website": r[2],
                 "company": r[3], "role": r[4]} for r in rows]
        last_id = raws[-1]["id"]

        found = await asyncio.gather(*[_enrich_one(rl, sem) for rl in raws])
        leads = [r for r in found if r]
        _progress["skrapp_hits"] += len(leads)

        if leads:
            stats = await ingest_rows(leads, source="skrapp_enrich", verify=True)
            total_added += stats.get("added", 0)

        processed += len(raws)
        _progress.update(processed=processed, added=total_added, last_id=last_id,
                         msg=f"{processed}/{limit} processed, {total_added} added")

        if skrapp.get_state().get("quota_exhausted"):
            _progress["msg"] = "Skrapp quota exhausted — stopping"
            break

    async with SessionLocal() as s:
        portal_total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
    _progress.update(running=False, done=True, added=total_added,
                     portal_total=portal_total,
                     msg=f"Done: {processed} processed, {_progress['skrapp_hits']} Skrapp hits, {total_added} new clean leads.")
    log.info(f"Enrichment finished: {get_progress()}")
    return get_progress()
