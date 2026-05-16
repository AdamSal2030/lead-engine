from __future__ import annotations
"""Run loop: collect URLs → parse → find emails → verify → store → deliver."""
import asyncio
import json
import logging
import random
import time
from datetime import datetime
from sqlalchemy import select, func, update
from db import SessionLocal, SeenURL, RawLead, VerifiedLead, Batch
from config import settings
from pipeline.sources import collect_all_urls, source_label
from pipeline.parser import parse_article, find_emails
from pipeline.verifier import verify_lead
from pipeline.delivery import deliver_batch

log = logging.getLogger("orchestrator")

# Global state for run-in-progress (only one at a time)
_run_lock = asyncio.Lock()
_current_batch: dict | None = None


async def get_current_status() -> dict | None:
    return _current_batch


async def is_running() -> bool:
    return _run_lock.locked()


async def get_unseen_urls(all_by_source: dict[str, list[str]]) -> list[tuple[str, str]]:
    """Filter out URLs we've already processed."""
    async with SessionLocal() as s:
        result = await s.execute(select(SeenURL.url))
        seen = {row[0] for row in result.all()}
    out = []
    for source, urls in all_by_source.items():
        for u in urls:
            if u not in seen:
                out.append((u, source))
    random.shuffle(out)
    return out


async def mark_seen(url: str, source: str, status: str):
    async with SessionLocal() as s:
        s.add(SeenURL(url=url, source=source, status=status))
        try:
            await s.commit()
        except Exception:
            await s.rollback()


async def save_raw_lead(lead: dict, emails: list[str]) -> int | None:
    async with SessionLocal() as s:
        existing = await s.execute(
            select(RawLead).where(RawLead.source_url == lead["source_url"])
        )
        if existing.scalar_one_or_none():
            return None
        row = RawLead(
            source_url=lead["source_url"],
            source=lead["source"],
            name=lead.get("name", ""),
            website=lead.get("website", ""),
            company=lead.get("company"),
            role=lead.get("role"),
            email_candidates=json.dumps(emails),
        )
        s.add(row)
        await s.commit()
        return row.id


async def save_verified(verified: dict, batch_id: int) -> bool:
    """Save Tier A lead. Returns True if newly added, False if email duplicate."""
    parts = verified["name"].strip().split()
    first = parts[0] if parts else ""
    last = parts[-1] if len(parts) > 1 else ""
    async with SessionLocal() as s:
        existing = await s.execute(
            select(VerifiedLead).where(VerifiedLead.email == verified["verified_email"].lower())
        )
        if existing.scalar_one_or_none():
            return False
        v = verified["verification"]
        s.add(VerifiedLead(
            source_url=verified["source_url"],
            source=verified["source"],
            name=verified["name"],
            first_name=first, last_name=last,
            website=verified.get("website"),
            company=verified.get("company"),
            role=verified.get("role"),
            email=verified["verified_email"].lower(),
            reoon_status=v.get("status"),
            reoon_score=v.get("score"),
            is_catch_all=v.get("is_catch_all"),
            tier=v.get("tier"),
            batch_id=batch_id,
        ))
        try:
            await s.commit()
            return True
        except Exception:
            await s.rollback()
            return False


async def process_one_url(url: str, source: str, sem: asyncio.Semaphore) -> dict | None:
    """Parse a single founder URL; return raw lead dict with email_candidates, or None."""
    async with sem:
        try:
            parsed = await parse_article(url)
            if not parsed:
                await mark_seen(url, source, "no_parse")
                return None
            emails = await find_emails(parsed["website"], parsed["name"])
            if not emails:
                await mark_seen(url, source, "no_emails")
                return None
            await mark_seen(url, source, "parsed")
            await save_raw_lead(parsed, emails)
            return {**parsed, "email_candidates": emails}
        except Exception as e:
            log.debug(f"process_one_url {url}: {e}")
            await mark_seen(url, source, "error")
            return None


async def run_batch(target: int, trigger: str = "manual") -> dict:
    """Main entrypoint. Runs until target Tier A leads added or pool exhausted."""
    global _current_batch
    if _run_lock.locked():
        return {"ok": False, "msg": "Another batch is already running."}

    async with _run_lock:
        # Create batch row
        async with SessionLocal() as s:
            b = Batch(target=target, trigger=trigger, status="running")
            s.add(b)
            await s.commit()
            batch_id = b.id

        _current_batch = {
            "batch_id": batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "target": target,
            "trigger": trigger,
            "scraped": 0,
            "raw_with_emails": 0,
            "verified": 0,
        }

        log.info(f"=== Batch {batch_id} starting, target={target}, trigger={trigger} ===")
        start_time = time.time()

        try:
            sem_scrape = asyncio.Semaphore(settings.SCRAPE_CONCURRENCY)
            sem_verify = asyncio.Semaphore(settings.MAX_VERIFY_CONCURRENCY)

            verified_count = 0
            verify_tasks: list[asyncio.Task] = []
            wait_iterations = 0
            max_wall_seconds = settings.BATCH_MAX_HOURS * 3600

            async def verify_and_save(raw: dict):
                async with sem_verify:
                    v = await verify_lead(raw)
                    if v:
                        added = await save_verified(v, batch_id)
                        if added:
                            nonlocal_inc()

            def nonlocal_inc():
                nonlocal verified_count
                verified_count += 1
                _current_batch["verified"] = verified_count

            # OUTER LOOP — keep collecting URLs until target hit or max-time reached
            while verified_count < target:
                # Safety: max wall time
                if (time.time() - start_time) > max_wall_seconds:
                    log.warning(f"Batch {batch_id} hit max wall-time ({settings.BATCH_MAX_HOURS}h). Delivering partial.")
                    break

                # 1. Collect URLs
                all_urls = await collect_all_urls()
                unseen = await get_unseen_urls(all_urls)
                log.info(f"  outer iter {wait_iterations + 1}: {len(unseen)} unseen URLs available")

                if not unseen:
                    if settings.PARTIAL_BATCHES:
                        log.info("Pool exhausted and PARTIAL_BATCHES=True. Finishing with what we have.")
                        break
                    # Wait for new content
                    wait_iterations += 1
                    log.info(f"Sources exhausted. Waiting {settings.RETRY_SITEMAP_SECONDS}s for new posts. "
                             f"verified={verified_count}/{target}")
                    _current_batch["status_msg"] = f"Waiting for new URLs (iter {wait_iterations})"
                    await asyncio.sleep(settings.RETRY_SITEMAP_SECONDS)
                    continue

                # 2. Process this chunk
                scrape_tasks = []
                for url, source in unseen:
                    scrape_tasks.append(asyncio.create_task(process_one_url(url, source, sem_scrape)))

                for fut in asyncio.as_completed(scrape_tasks):
                    raw = await fut
                    _current_batch["scraped"] += 1
                    if raw:
                        _current_batch["raw_with_emails"] += 1
                        t = asyncio.create_task(verify_and_save(raw))
                        verify_tasks.append(t)

                    if verified_count >= target:
                        log.info(f"Target {target} reached. Stopping URL queue.")
                        for sc in scrape_tasks:
                            if not sc.done():
                                sc.cancel()
                        break

                    if _current_batch["scraped"] % 100 == 0:
                        el = time.time() - start_time
                        log.info(
                            f"  [{_current_batch['scraped']} scraped | "
                            f"{_current_batch['raw_with_emails']} raw | "
                            f"{verified_count} verified | {el/60:.1f}min]"
                        )

            # Wait for any pending verifies
            if verify_tasks:
                await asyncio.gather(*verify_tasks, return_exceptions=True)

            # 3. Build deliverable CSV for THIS batch
            csv_path = await deliver_batch(batch_id)
            log.info(f"Batch {batch_id} CSV: {csv_path}")

            async with SessionLocal() as s:
                final_count = (await s.execute(
                    select(func.count()).select_from(VerifiedLead).where(VerifiedLead.batch_id == batch_id)
                )).scalar_one()
                await s.execute(update(Batch).where(Batch.id == batch_id).values(
                    status="completed", finished_at=datetime.utcnow(),
                    delivered_count=final_count, csv_path=csv_path,
                    delivered_email=settings.DELIVERY_EMAIL,
                ))
                await s.commit()

            _current_batch["status"] = "completed"
            return {"ok": True, "batch_id": batch_id, "verified": final_count, "csv": csv_path}

        except Exception as e:
            log.exception("Batch failed")
            async with SessionLocal() as s:
                await s.execute(update(Batch).where(Batch.id == batch_id).values(
                    status="failed", finished_at=datetime.utcnow(), notes=str(e)[:500]
                ))
                await s.commit()
            return {"ok": False, "msg": str(e)}
        finally:
            _current_batch = None
