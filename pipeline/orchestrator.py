from __future__ import annotations
"""Run loop: collect URLs → parse → find emails → verify → store → deliver."""
import asyncio
import json
import logging
import random
import re
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

# Skrapp niche allow-list (parsed once from settings). Empty set = no gating.
# Skrapp only spends a credit when a lead's classified niche is in this set;
# off-target leads still get free email extraction.
SKRAPP_TARGET_NICHES: frozenset[str] = frozenset(
    n.strip() for n in (settings.SKRAPP_TARGET_NICHES or "").split(",") if n.strip()
)

# Global state for run-in-progress (only one at a time)
_run_lock = asyncio.Lock()
_current_batch: dict | None = None


async def get_current_status() -> dict | None:
    return _current_batch


async def is_running() -> bool:
    return _run_lock.locked()


async def get_unseen_urls(all_by_source: dict[str, list[str]],
                           retry_stuck: bool = False) -> list[tuple[str, str]]:
    """Filter out already-processed URLs and return remainder in quality-weighted order.

    URLs from high-quality sources (per intelligence engine weights) are placed
    earlier in the queue so the batch hits its target with better leads first.
    Same-weight sources are randomised within their tier.
    """
    async with SessionLocal() as s:
        if retry_stuck:
            result = await s.execute(select(SeenURL.url).where(SeenURL.status == "parsed"))
        else:
            result = await s.execute(select(SeenURL.url))
        seen = {row[0] for row in result.all()}

    out = []
    for source, urls in all_by_source.items():
        for u in urls:
            if u not in seen:
                out.append((u, source))

    # Load source weights set by the intelligence engine (default 1.0)
    try:
        from pipeline.intelligence import get_source_weights
        weights = await get_source_weights()
    except Exception:
        weights = {}

    # Sort: highest weight first, randomise within same-weight tier
    out.sort(key=lambda item: (-weights.get(item[1], 1.0), random.random()))
    return out


async def clear_stuck_seen(include_no_parse: bool = False) -> int:
    """Remove transiently-failed SeenURL rows so they get retried.

    Always clears: no_emails, error
    include_no_parse=True: also clears no_parse — use when Claude parser is
    available since it can successfully extract from articles the regex missed.
    Never clears claude_no_parse — those were already tried by Claude and failed;
    clearing them would waste Haiku quota re-trying pages that have no usable data.
    """
    from sqlalchemy import delete
    statuses = ["no_emails", "error"]
    if include_no_parse:
        statuses.append("no_parse")
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(SeenURL.status.in_(statuses))
        )
        await s.commit()
        return result.rowcount


async def clear_no_parse_seen() -> int:
    """Clear only no_parse entries — gives Claude a shot at previously regex-failed URLs."""
    from sqlalchemy import delete
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(SeenURL.status == "no_parse")
        )
        await s.commit()
        return result.rowcount


async def clear_old_no_parse(days: int = 14) -> int:
    """Clear plain no_parse entries older than `days` days.

    These are URLs where the regex parser failed but Claude was never tried
    (either disabled or daily cap hit). Retry them periodically because:
      - Article content gets updated over time
      - The regex parser improves across deploys
    Unlike claude_no_parse (Claude already tried), these are cheap to retry.
    """
    from sqlalchemy import delete
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(
                SeenURL.status == "no_parse",
                SeenURL.first_seen < cutoff,
            )
        )
        await s.commit()
        return result.rowcount


async def clear_old_claude_no_parse(days: int = 7) -> int:
    """Clear claude_no_parse entries older than `days` days.

    Our parser improves over time and page content changes — URLs that Claude
    couldn't extract from a week ago deserve a fresh attempt. The daily Claude
    cap (CLAUDE_MAX_PER_DAY) prevents runaway spend on re-tries.
    """
    from sqlalchemy import delete
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(
                SeenURL.status == "claude_no_parse",
                SeenURL.first_seen < cutoff,
            )
        )
        await s.commit()
        return result.rowcount


# Article sources that produce personal-name leads — safe to recycle
# because save_verified() deduplicates by email address.
ARTICLE_SOURCES = {
    "canvasrebel", "boldjourney", "valiantceo", "founderhour",
    "authority_magazine", "ideamensch", "hackernews", "betalist",
    "indiehackers",
    # Voyage network
    "voyagela", "voyageatl", "voyagemia", "voyagedallas", "voyagehouston",
    "voyageraleigh", "voyagestl", "voyagekc", "voyageaustin", "voyagechicago",
    "voyageohio", "voyageminnesota", "voyageutah", "voyagebaltimore",
    "voyagecharlotte", "voyagevirginia", "voyagewisconsin", "voyagewashington",
    "voyagealabama", "voyagemichigan", "voyagephoenix", "voyagedenver",
    "voyagesf", "voyagerichmond", "voyageindy", "voyagesd", "voyagememphis",
    "voyagephilly", "voyagenashville", "voyageportland", "voyageseattle", "voyageny",
    # ShoutOut network
    "shoutoutla", "shoutoutatl", "shoutoutdfw", "shoutoutsocal", "shoutoutnorcal",
    # PR sites
    "ceoweekly", "famoustimes", "disruptmagazine", "ceomonthly",
    "americanentrepreneurship", "ceoblognation", "addicted2success",
    "thriveglobal", "beingentrepreneur", "gritdaily", "influencive",
    # NewsAnchored strict
    "nyweekly", "lawire", "kivodaily", "usinsider", "usbusinessnews",
    "worldreporter", "marketdaily", "economicinsider", "portlandnews",
    "miamiwire", "nywire", "atlwire", "texastoday", "sanfranciscopost",
    "cagazette", "californiaobserver", "thechicagojournal", "womensjournal",
    "blknews", "influencerdaily", "artistweekly", "usreporter",
    "theamericannews", "realestatetoday",
}


async def clear_stale_parsed(days: int = 30) -> int:
    """Clear 'parsed' status for ARTICLE source URLs older than N days.

    KEY INSIGHT: 'parsed' means email candidates were found and sent to the
    verifier — NOT that a verified lead was produced. Many 'parsed' URLs had
    candidates that all failed verification (e.g. all guesses were 'invalid').
    The pipeline has improved since then (better patterns, better verifier
    thresholds) — these URLs deserve another shot.

    Safe because:
    - save_raw_lead() deduplicates by source_url → no duplicate raw rows
    - save_verified() deduplicates by email → no duplicate verified leads
    - Only fires for article sources (personal names), never directory sources
      (company names) which would just waste time reprocessing junk.
    """
    from sqlalchemy import delete
    from datetime import datetime, timedelta
    cutoff = datetime.utcnow() - timedelta(days=days)
    async with SessionLocal() as s:
        result = await s.execute(
            delete(SeenURL).where(
                SeenURL.status == "parsed",
                SeenURL.source.in_(ARTICLE_SOURCES),
                SeenURL.first_seen < cutoff,
            )
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

    # Niche: carry from parsed result; classify now if missing
    niche = verified.get("niche") or ""
    if not niche:
        from pipeline.niche import classify
        niche = classify(verified.get("role"), verified.get("company"), None, verified.get("website"))

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
            niche=niche,
            hook=verified.get("hook") or "",
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
            # Route directory URLs through their dedicated parser (no article needed)
            if source in ("clutch", "designrush"):
                from pipeline.directory_parser import (
                    parse_clutch_profile, parse_designrush_profile
                )
                if source == "clutch":
                    parsed = await parse_clutch_profile(url)
                else:
                    parsed = await parse_designrush_profile(url)
            elif source == "indiehackers":
                from pipeline.directory_parser import parse_indiehackers_profile
                parsed = await parse_indiehackers_profile(url)
            elif source == "hackernews":
                from pipeline.directory_parser import parse_hn_showhn
                parsed = await parse_hn_showhn(url)
            elif source == "betalist":
                from pipeline.directory_parser import parse_betalist_startup
                parsed = await parse_betalist_startup(url)
            elif source == "goodfirms":
                from pipeline.directory_parser import parse_goodfirms_profile
                parsed = await parse_goodfirms_profile(url)
            elif source == "trustpilot":
                from pipeline.directory_parser import parse_trustpilot_business
                parsed = await parse_trustpilot_business(url)
            elif source == "appsumo":
                from pipeline.directory_parser import parse_appsumo_product
                parsed = await parse_appsumo_product(url)
            else:
                parsed = await parse_article(url)

            if not parsed:
                await mark_seen(url, source, "no_parse")
                return None
            # Claude was tried but couldn't extract name+website — mark permanently
            # so this URL is never handed to Claude again (saves daily quota)
            if parsed.get("_failed") == "claude":
                await mark_seen(url, source, "claude_no_parse")
                return None

            # L1: emails in the article body itself (could be founder's personal gmail)
            article_emails = parsed.get("article_emails", []) or []
            # L2 + L3: scrape founder website + pattern-guess
            website_emails = await find_emails(parsed["website"], parsed["name"])

            # Combine, dedupe, rank
            combined = list(set(article_emails + website_emails))

            # Decide if we need Skrapp (L4).
            # Only skip Skrapp if we already have a HIGH-CONFIDENCE personal email:
            #   - A free-provider email (gmail, yahoo, etc.) — founder uses personal address
            #   - OR an email where the local part contains the founder's first or last name
            # Don't skip just because we have ANY non-generic email — "founders@acme.com"
            # or "newsletter@acme.com" are not good enough to skip Skrapp.
            _founder_parts = [p.lower() for p in parsed["name"].split()] if parsed.get("name") else []
            _first_n = re.sub(r"[^a-z]", "", _founder_parts[0]) if _founder_parts else ""
            _last_n = re.sub(r"[^a-z]", "", _founder_parts[-1]) if len(_founder_parts) > 1 else ""
            FREE_PROVIDERS = {"gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com",
                              "aol.com","protonmail.com","proton.me","live.com","msn.com",
                              "me.com","mac.com","fastmail.com","pm.me","zoho.com"}
            need_skrapp = True
            for e in combined:
                local_e, _, dom_e = e.partition("@")
                local_lower = local_e.lower()
                if dom_e in FREE_PROVIDERS:
                    need_skrapp = False; break
                if _first_n and len(_first_n) >= 3 and _first_n in local_lower:
                    need_skrapp = False; break
                if _last_n and len(_last_n) >= 3 and _last_n in local_lower:
                    need_skrapp = False; break

            # Extract domain once for L4 + L5
            _words = parsed["name"].split()
            _first = _words[0] if _words else ""
            _last = _words[-1] if len(_words) > 1 else ""
            try:
                _pu = urllib.parse.urlparse(
                    parsed["website"] if parsed["website"].startswith("http")
                    else "https://" + parsed["website"]
                )
                _domain = _pu.netloc.replace("www.", "").lower()
            except Exception:
                _domain = ""

            # L3.5: Hunter domain-search — finds REAL emails for a domain without
            # needing a person name at all. Fires for ALL leads that have a domain.
            # This is the key fix for directory leads (Trustpilot, Clutch, GoodFirms)
            # where we have a company domain but no founder name.
            # Hunter returns confirmed emails from their database — far better than guesses.
            if _domain:
                try:
                    from pipeline.hunter import domain_search as _hunter_ds, get_state as _hunter_state
                    if _hunter_state().get("enabled"):
                        _hd_results = await _hunter_ds(_domain)
                        _found_personal = False
                        for _hr in _hd_results:
                            _hr_email = (_hr.get("email") or "").strip()
                            if not _hr_email:
                                continue
                            combined.insert(0, _hr_email)  # prepend — real address, high confidence
                            if not _found_personal and _hr.get("type") == "personal":
                                _found_personal = True
                                # Upgrade company-only lead → person when Hunter knows who it is
                                if parsed.get("_is_company"):
                                    _fn = (_hr.get("first_name") or "").strip()
                                    _ln = (_hr.get("last_name") or "").strip()
                                    if _fn and _ln:
                                        parsed["name"] = f"{_fn} {_ln}"
                                        parsed["_is_company"] = False
                                        _words = parsed["name"].split()
                                        _first = _words[0] if _words else ""
                                        _last = _words[-1] if len(_words) > 1 else ""
                        if _found_personal:
                            need_skrapp = False  # Hunter already found a real personal email
                except Exception:
                    pass

            # For directory sources with only a company name (no personal name),
            # try ONLY the company-slug address (e.g. "Acme Digital" →
            # acme@acmedigital.com) — often a real solo-founder inbox, and it's
            # MV-verified before it can become a lead. We deliberately do NOT
            # generate generic role guesses (founder@/ceo@/info@): those are the
            # catch-all bounce source and get filtered by the verifier anyway.
            if (settings.ALLOW_EMAIL_GUESSING and parsed.get("_is_company")
                    and _domain and need_skrapp):
                _company_raw = (parsed.get("company") or parsed.get("name") or "").lower()
                _company_slug = re.sub(r"[^a-z0-9]", "", _company_raw.split()[0]) if _company_raw.split() else ""
                if _company_slug and len(_company_slug) >= 3 and _company_slug not in {"the", "our", "inc", "llc", "ltd"}:
                    combined.append(f"{_company_slug}@{_domain}")

            # L4: Skrapp finder — niche-gated so credits only go to the niches we want.
            # off-target leads keep every free email we already found; they just
            # never spend a Skrapp credit. Empty allow-list = fire on all niches.
            _lead_niche = (parsed.get("niche") or "").strip()
            _skrapp_niche_ok = (not SKRAPP_TARGET_NICHES) or (_lead_niche in SKRAPP_TARGET_NICHES)
            if need_skrapp and _skrapp_niche_ok and skrapp_finder.get_state().get("enabled"):
                if len(_words) >= 2 and _domain:
                    try:
                        skrapp_res = await skrapp_finder.find_email(_first, _last, _domain)
                        if skrapp_res and skrapp_res.get("email"):
                            combined.insert(0, skrapp_res["email"])
                            need_skrapp = False  # got one — skip Hunter
                    except Exception:
                        pass

            # L5: Hunter.io — fires when Skrapp also found nothing
            if need_skrapp and len(_words) >= 2 and _domain:
                try:
                    from pipeline.hunter import find_email as hunter_find, get_state as hunter_state
                    if hunter_state().get("enabled"):
                        hunter_res = await hunter_find(_first, _last, _domain)
                        if hunter_res and hunter_res.get("email"):
                            combined.insert(0, hunter_res["email"])
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
            # ── Re-open the retry pool on every batch start ────────────────────
            # Clear no_emails + error every batch (transient — worth retrying).
            # Do NOT clear no_parse here: those are structural parse failures; with
            # our improved patterns they will be cleared weekly by the app-level job.
            # Clearing no_parse every batch floods the queue with junk URLs that eat
            # all 6 hours while genuinely parseable sources get starved.
            retry_cleared = await clear_stuck_seen(include_no_parse=False)
            log.info(f"  Cleared {retry_cleared} no_emails/error URLs for retry")
            # Time-based refresh: no_parse URLs older than 14 days get another shot
            # — article content changes and the regex parser improves over time.
            # (claude_no_parse takes 7 days; plain no_parse is cheaper to retry)
            np_stale = await clear_old_no_parse(days=14)
            if np_stale:
                log.info(f"  Cleared {np_stale} stale no_parse URLs (>14 days) for retry")

            # Time-based refresh: claude_no_parse URLs older than 7 days get another
            # shot — our parser has improved and the page content may have changed.
            cnp_cleared = await clear_old_claude_no_parse(days=7)
            if cnp_cleared:
                log.info(f"  Cleared {cnp_cleared} claude_no_parse URLs older than 7 days")

            # Stale-parsed refresh: article source URLs older than 14 days get
            # reprocessed. 'parsed' means candidates were found but verification
            # may have failed. 14-day cycle doubles throughput vs the old 30-day.
            sp_cleared = await clear_stale_parsed(days=14)
            if sp_cleared:
                log.info(f"  Recycled {sp_cleared} stale 'parsed' article URLs (>30 days old)")

            # Auto-scale verification concurrency based on active verifier.
            # MillionVerifier handles 1000 RPM → needs 25-40 concurrent to saturate.
            # Reoon handles 20 RPM per key → more than 4/key causes rate-limit thrashing.
            from pipeline.reoon_pool import get_pool as _pool
            from pipeline import mv_verifier as _mv
            n_keys = max(1, len(_pool()))
            if _mv.get_state().get("enabled"):
                # MV is primary — 1000 RPM warrants high concurrency
                verify_concurrency = max(settings.MAX_VERIFY_CONCURRENCY, 40)
                log.info(f"  MillionVerifier active — verify concurrency = {verify_concurrency}")
            else:
                # Reoon-only: 4 concurrent per key keeps inside the 20 RPM limit
                verify_concurrency = 4 * n_keys
                log.info(f"  Reoon-only ({n_keys} key(s)) — verify concurrency = {verify_concurrency}")
            sem_scrape = asyncio.Semaphore(settings.SCRAPE_CONCURRENCY)
            sem_verify = asyncio.Semaphore(verify_concurrency)

            verified_count = 0
            verify_tasks: list[asyncio.Task] = []
            wait_iterations = 0
            max_wall_seconds = settings.BATCH_MAX_HOURS * 3600
            _exhausted = False

            # Batch-level counters for diagnosing where leads die
            _stage = {"parsed": 0, "no_parse": 0, "no_emails": 0,
                      "verify_ok": 0, "verify_fail": 0}

            async def verify_and_save(raw: dict):
                async with sem_verify:
                    v = await verify_lead(raw)
                    if v:
                        added = await save_verified(v, batch_id)
                        if added:
                            _stage["verify_ok"] += 1
                            nonlocal_inc()
                        # else: email duplicate — still a hit, just not a new lead
                    else:
                        _stage["verify_fail"] += 1

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
                    # Try retrying URLs that previously failed
                    if wait_iterations == 0:
                        log.info("Pool exhausted. Clearing stuck-seen URLs (no_emails + error)...")
                        cleared = await clear_stuck_seen()
                        log.info(f"  cleared {cleared} stuck URLs for retry")

                        # Also clear no_parse so Claude can attempt them with the improved
                        # pre-screen + text extractor. This is the main source of new leads
                        # when all article URLs have been processed at least once.
                        if settings.ANTHROPIC_API_KEY and settings.CLAUDE_PARSE_ENABLED:
                            np_cleared = await clear_no_parse_seen()
                            log.info(f"  cleared {np_cleared} no_parse URLs — Claude will retry with improved pre-screen")

                        unseen = await get_unseen_urls(all_urls)
                        log.info(f"  now {len(unseen)} unseen URLs available after clearing")

                    if not unseen:
                        if settings.PARTIAL_BATCHES:
                            log.info("Pool exhausted and PARTIAL_BATCHES=True. Finishing.")
                            _exhausted = True
                            break
                        wait_iterations += 1
                        # After MAX_WAIT_ITERATIONS, deliver partial and let perpetual loop sleep
                        if wait_iterations > settings.MAX_WAIT_ITERATIONS:
                            log.warning(
                                f"Sources exhausted after {wait_iterations} retries "
                                f"({wait_iterations * settings.RETRY_SITEMAP_SECONDS // 60}min). "
                                f"Delivering partial batch and backing off."
                            )
                            _exhausted = True
                            break
                        log.info(f"Sources exhausted. Waiting {settings.RETRY_SITEMAP_SECONDS}s. "
                                 f"verified={verified_count}/{target} (iter {wait_iterations}/{settings.MAX_WAIT_ITERATIONS})")
                        _current_batch["status_msg"] = f"Waiting for new URLs (iter {wait_iterations}/{settings.MAX_WAIT_ITERATIONS})"
                        await asyncio.sleep(settings.RETRY_SITEMAP_SECONDS)
                        continue

                # 2. Process URLs in chunks to avoid creating tens-of-thousands of
                #    asyncio Tasks in memory at once. Each chunk fully completes before
                #    the next one starts, keeping memory bounded.
                CHUNK_SIZE = 1000
                target_reached = False
                for chunk_start in range(0, len(unseen), CHUNK_SIZE):
                    if target_reached:
                        break
                    chunk = unseen[chunk_start: chunk_start + CHUNK_SIZE]
                    scrape_tasks = [
                        asyncio.create_task(process_one_url(url, source, sem_scrape))
                        for url, source in chunk
                    ]

                    for fut in asyncio.as_completed(scrape_tasks):
                        raw = await fut
                        _current_batch["scraped"] += 1
                        if raw:
                            _current_batch["raw_with_emails"] += 1
                            _stage["parsed"] += 1
                            t = asyncio.create_task(verify_and_save(raw))
                            verify_tasks.append(t)
                        else:
                            _stage["no_parse"] += 1

                        if verified_count >= target:
                            log.info(f"Target {target} reached. Stopping URL queue.")
                            for sc in scrape_tasks:
                                if not sc.done():
                                    sc.cancel()
                            target_reached = True
                            break

                        if _current_batch["scraped"] % 200 == 0:
                            el = time.time() - start_time
                            log.info(
                                f"  [{_current_batch['scraped']} scraped | "
                                f"parse_ok={_stage['parsed']} no_parse={_stage['no_parse']} | "
                                f"raw_emails={_current_batch['raw_with_emails']} | "
                                f"verify_ok={_stage['verify_ok']} verify_fail={_stage['verify_fail']} | "
                                f"leads={verified_count} | {el/60:.1f}min]"
                            )

            # Wait for any pending verifies
            if verify_tasks:
                await asyncio.gather(*verify_tasks, return_exceptions=True)

            # Final pipeline funnel summary — tells you EXACTLY where leads died
            total_scraped = _current_batch["scraped"]
            parse_rate = round(100 * _stage["parsed"] / max(total_scraped, 1), 1)
            verify_rate = round(100 * _stage["verify_ok"] / max(_stage["parsed"], 1), 1)
            log.info(
                f"=== Batch {batch_id} pipeline summary ===\n"
                f"  Scraped URLs:      {total_scraped}\n"
                f"  Parsed OK:         {_stage['parsed']} ({parse_rate}%)  ← low? pool is mostly failed URLs\n"
                f"  Parse failed:      {_stage['no_parse']}               ← high? add sources / clear pool\n"
                f"  Had email cands:   {_current_batch['raw_with_emails']}\n"
                f"  Verify OK:         {_stage['verify_ok']} ({verify_rate}% of parsed)  ← low? MV/Reoon rejecting\n"
                f"  Verify failed:     {_stage['verify_fail']}            ← high? check MV credits & Reoon key\n"
                f"  New leads added:   {verified_count}"
            )

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
            msg = "No unseen URLs — sources exhausted" if _exhausted else ""
            return {"ok": True, "batch_id": batch_id, "verified": final_count, "csv": csv_path, "msg": msg}

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
