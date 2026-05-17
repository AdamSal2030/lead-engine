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
from pipeline.parser import parse_article, find_emails, rank_emails
from pipeline.verifier import verify_lead
from pipeline.delivery import deliver_batch
from pipeline import finder as skrapp_finder
import urllib.parse

log = logging.getLogger("orchestrator")

# Global state for run-in-progress (only one at a time)
_run_lock = asyncio.Lock()
_current_batch: dict | None = None


async def get_current_status() -> dict | None:
    return _current_batch


async def is_running() -> bool:
    return _run_lock.locked()


async def get_unseen_urls(all_by_source: dict[str, list[str]],
                           retry_stuck: bool = False) -> list[tuple[str, str]]:
    """Filter out URLs we've already processed.
    If retry_stuck=True, also include URLs marked 'no_emails' or 'no_parse' or 'error'
    (gives them another shot in case parser/network improved)."""
    async with SessionLocal() as s:
        if retry_stuck:
            # Only treat 'parsed' (successful) URLs as truly seen
            result = await s.execute(select(SeenURL.url).where(SeenURL.status == "parsed"))
        else:
            result = await s.execute(select(SeenURL.url))
        seen = {row[0] for row in result.all()}

    out = []
    for source, urls in all_by_source.items():
        for u in urls:
            if u not in seen:
                out.append((u, source))
    random.shuffle(out)
    return out


async def clear_stuck_seen() -> int:
    """Remove SeenURL rows marked as 'no_emails', 'no_parse', or 'error' so they get retried.
    Returns count of cleared rows."""
    from sqlalchemy import delete
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(SeenURL.status.in_(["no_emails", "no_parse", "error"]))
        )
        await s.commit()
        return result.rowcount


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
    """4-layer email discovery:
      L1: emails in the article body (free) — gmail/yahoo founders often drop here
      L2: emails on the founder's website (free)
      L3: pattern-guess from website domain (free)
      L4: Skrapp finder (1 credit) — only if L1-L3 produced nothing or only generics
    """
    async with sem:
        try:
            parsed = await parse_article(url)
            if not parsed:
                await mark_seen(url, source, "no_parse")
                return None

            # L1: emails in the article body itself (could be founder's personal gmail)
            article_emails = parsed.get("article_emails", []) or []
            # L2 + L3: scrape founder website + pattern-guess
            website_emails = await find_emails(parsed["website"], parsed["name"])

            # Combine, dedupe, rank
            combined = list(set(article_emails + website_emails))

            # Decide if we need Skrapp (L4): no emails OR only generics
            need_skrapp = True
            if combined:
                # If we have ANY personal-looking or free-provider email, we're good
                has_personal = False
                for e in combined:
                    local, _, dom = e.partition("@")
                    if dom in {"gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com",
                               "aol.com","protonmail.com","proton.me","live.com","msn.com",
                               "me.com","mac.com"}:
                        has_personal = True; break
                    if local.lower() not in {"info","hello","contact","support","team","admin",
                                              "office","sales","help","press","media"}:
                        has_personal = True; break
                if has_personal:
                    need_skrapp = False

            # L4: Skrapp finder, only if needed
            if need_skrapp and skrapp_finder.get_state().get("enabled"):
                # Extract first/last from name, domain from website
                words = parsed["name"].split()
                if len(words) >= 2:
                    first, last = words[0], words[-1]
                    try:
                        parsed_url = urllib.parse.urlparse(
                            parsed["website"] if parsed["website"].startswith("http")
                            else "https://" + parsed["website"]
                        )
                        domain = parsed_url.netloc.replace("www.", "").lower()
                        skrapp_res = await skrapp_finder.find_email(first, last, domain)
                        if skrapp_res and skrapp_res.get("email"):
                            combined.insert(0, skrapp_res["email"])
                    except Exception:
                        pass

            if not combined:
                await mark_seen(url, source, "no_emails")
                return None

            # Rank emails: personal first, generic last
            ranked = rank_emails(combined, parsed["name"])

            await mark_seen(url, source, "parsed")
            await save_raw_lead(parsed, ranked)
            return {**parsed, "email_candidates": ranked}
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
            # Verify concurrency scales with # of Reoon keys
            from pipeline.reoon_pool import get_pool as _pool
            n_keys = max(1, len(_pool()))
            verify_concurrency = settings.MAX_VERIFY_CONCURRENCY * n_keys
            sem_scrape = asyncio.Semaphore(settings.SCRAPE_CONCURRENCY)
            sem_verify = asyncio.Semaphore(verify_concurrency)
            log.info(f"  Using {n_keys} Reoon key(s); verify concurrency = {verify_concurrency}")

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
                    # Try retrying URLs that previously failed (no_emails / no_parse / error)
                    # — gives them another chance with our updated parser + UAs
                    if wait_iterations == 0:
                        log.info("Pool exhausted on first pass. Clearing stuck-seen URLs to retry them...")
                        cleared = await clear_stuck_seen()
                        log.info(f"  cleared {cleared} stuck URLs for retry")
                        unseen = await get_unseen_urls(all_urls)
                        log.info(f"  now {len(unseen)} unseen URLs after clearing stuck ones")

                    if not unseen:
                        if settings.PARTIAL_BATCHES:
                            log.info("Pool exhausted and PARTIAL_BATCHES=True. Finishing.")
                            break
                        wait_iterations += 1
                        log.info(f"Sources exhausted. Waiting {settings.RETRY_SITEMAP_SECONDS}s. "
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
