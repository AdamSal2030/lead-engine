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
from pipeline.niche import classify
from config import settings

log = logging.getLogger("enrich")

# Target money niches — only spend credits on these. Empty = all niches.
TARGET_NICHES = frozenset(
    n.strip() for n in (settings.SKRAPP_TARGET_NICHES or "").split(",") if n.strip()
) or frozenset({
    "Marketing Agency", "Coaching", "Consulting", "Author / Speaker", "Real Estate",
    "SaaS / Tech", "Creative Services", "Recruiting & HR", "Legal & Finance",
    "Fitness & Wellness", "Education & Training", "E-commerce", "Founder / Startup",
})

_progress: dict = {"running": False, "processed": 0, "skrapp_hits": 0,
                   "added": 0, "skipped_known": 0, "skipped_niche": 0,
                   "last_id": 0, "target": 0, "done": False, "msg": ""}


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


async def count_eligible() -> dict:
    """DRY RUN — scan ALL raw leads and count how many are genuinely new
    (domain not already owned) AND on-target niche. Spends ZERO credits."""
    async with SessionLocal() as s:
        vl_sites = (await s.execute(
            select(VerifiedLead.website).where(VerifiedLead.website.isnot(None)))).scalars().all()
        raw = (await s.execute(
            select(RawLead.name, RawLead.website, RawLead.company, RawLead.role)
            .where(RawLead.name.isnot(None), RawLead.website.isnot(None)))).all()
    known = {_domain_of(w) for w in vl_sites if _domain_of(w)}
    eligible = 0
    seen = set()
    by_niche: dict = {}
    skipped_known = skipped_niche = no_domain = 0
    for name, website, company, role in raw:
        first, last = _split_name(name)
        dom = _domain_of(website)
        if not dom or not first or not last:
            no_domain += 1
            continue
        if dom in known or dom in seen:
            skipped_known += 1
            continue
        niche = classify(role, company, None, website)
        if TARGET_NICHES and niche not in TARGET_NICHES:
            skipped_niche += 1
            continue
        seen.add(dom)
        eligible += 1
        by_niche[niche] = by_niche.get(niche, 0) + 1
    return {"total_raw": len(raw), "eligible_new_ontarget": eligible,
            "skipped_already_owned": skipped_known, "skipped_off_niche": skipped_niche,
            "skipped_no_name_or_domain": no_domain,
            "by_niche": dict(sorted(by_niche.items(), key=lambda x: -x[1]))}


async def run_enrichment(limit: int = 2000, after_id: int = 0,
                         batch_commit: int = 200) -> dict:
    """Process up to `limit` raw leads (id > after_id) through Skrapp + ingest.
    Updates module-level _progress as it goes."""
    global _progress
    _progress = {"running": True, "processed": 0, "skrapp_hits": 0, "added": 0,
                 "skipped_known": 0, "skipped_niche": 0,
                 "last_id": after_id, "target": limit, "done": False, "msg": "starting"}

    if not settings.SKRAPP_API_KEY:
        _progress.update(running=False, done=True, msg="No SKRAPP_API_KEY configured")
        return get_progress()

    # Preload every domain we already have a verified lead for — robust
    # (normalized) dedup so we never spend a credit on a company we already own.
    async with SessionLocal() as s:
        vl_sites = (await s.execute(
            select(VerifiedLead.website).where(VerifiedLead.website.isnot(None)))).scalars().all()
    known_domains = {_domain_of(w) for w in vl_sites if _domain_of(w)}

    sem = asyncio.Semaphore(settings.SKRAPP_CONCURRENCY)
    total_added = 0
    last_id = after_id
    processed = 0   # = credit-eligible leads actually sent to Skrapp

    SCAN = 500
    while processed < limit:
        async with SessionLocal() as s:
            rows = (await s.execute(
                select(RawLead.id, RawLead.name, RawLead.website,
                       RawLead.company, RawLead.role)
                .where(RawLead.id > last_id,
                       RawLead.name.isnot(None), RawLead.website.isnot(None))
                .order_by(RawLead.id).limit(SCAN)
            )).all()
        if not rows:
            break
        last_id = rows[-1][0]

        # Filter in Python: skip already-known domains and off-target niches
        eligible = []
        for r in rows:
            rid, name, website, company, role = r
            dom = _domain_of(website)
            if not dom or dom in known_domains:
                _progress["skipped_known"] += 1
                continue
            niche = classify(role, company, None, website)
            if TARGET_NICHES and niche not in TARGET_NICHES:
                _progress["skipped_niche"] += 1
                continue
            known_domains.add(dom)  # dedupe within this run too
            eligible.append({"id": rid, "name": name, "website": website,
                             "company": company, "role": role})
            if len(eligible) >= (limit - processed):
                break

        if not eligible:
            continue  # whole scan window was dupes/off-niche; keep scanning

        found = await asyncio.gather(*[_enrich_one(rl, sem) for rl in eligible])
        leads = [r for r in found if r]
        _progress["skrapp_hits"] += len(leads)

        if leads:
            stats = await ingest_rows(leads, source="skrapp_enrich", verify=True)
            total_added += stats.get("added", 0)

        processed += len(eligible)
        _progress.update(processed=processed, added=total_added, last_id=last_id,
                         msg=f"{processed}/{limit} on-target processed, {total_added} new "
                             f"(skipped {_progress['skipped_known']} known, "
                             f"{_progress['skipped_niche']} off-niche)")

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
