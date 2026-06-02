from __future__ import annotations
"""FastAPI app + perpetual background lead-finder loop."""
import asyncio
import logging
import os
from datetime import datetime
from contextlib import asynccontextmanager
import secrets
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Depends
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from sqlalchemy import select, desc, func

from config import settings
from db import init_db, SessionLocal, Batch, VerifiedLead
from pipeline.orchestrator import run_batch, get_current_status, is_running
from pipeline.delivery import notify_sources_exhausted
from dashboard import render_dashboard

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("app")

# Perpetual-loop control
_perpetual_task: asyncio.Task | None = None
_unibox_task: asyncio.Task | None = None
_intelligence_task: asyncio.Task | None = None
_perpetual_paused = False
_last_exhausted_notice: datetime | None = None
_loop_state = {"current_target": 0, "completed_batches": 0}


async def perpetual_loop():
    """Run batch after batch, forever. Target ramps up as batches succeed."""
    global _last_exhausted_notice
    log.info("=== PERPETUAL LOOP STARTED — running until manually stopped ===")
    current_target = settings.BATCH_SIZE
    _loop_state["current_target"] = current_target
    while True:
        if _perpetual_paused:
            await asyncio.sleep(60)
            continue
        try:
            log.info(f"--- Starting batch with target={current_target} (max={settings.BATCH_SIZE_MAX}) ---")
            result = await run_batch(target=current_target, trigger="auto")
            verified = result.get("verified", 0) or 0
            log.info(f"Auto batch finished: verified={verified}, ok={result.get('ok')}, msg={result.get('msg', '')[:100]}")
            _loop_state["completed_batches"] += 1

            # Grow next target on any successful (non-exhausted) batch
            if verified > 0 and current_target < settings.BATCH_SIZE_MAX:
                new_target = min(current_target + settings.BATCH_SIZE_GROWTH, settings.BATCH_SIZE_MAX)
                if new_target != current_target:
                    log.info(f"  next batch target ramped: {current_target} → {new_target}")
                    current_target = new_target
                    _loop_state["current_target"] = current_target

            # Detect exhaustion (no unseen URLs)
            if verified == 0 and "No unseen URLs" in (result.get("msg") or ""):
                # Notify once per 6 hours max
                now = datetime.utcnow()
                if (not _last_exhausted_notice
                        or (now - _last_exhausted_notice).total_seconds() > 6 * 3600):
                    try:
                        await notify_sources_exhausted()
                        _last_exhausted_notice = now
                    except Exception:
                        log.exception("Failed to send exhaustion email")
                log.info(f"Sources exhausted. Sleeping {settings.EXHAUSTED_RETRY_SECONDS}s before retry.")
                await asyncio.sleep(settings.EXHAUSTED_RETRY_SECONDS)
            else:
                await asyncio.sleep(settings.BETWEEN_BATCH_SECONDS)
        except Exception:
            log.exception("Perpetual loop error — sleeping 60s")
            await asyncio.sleep(60)


async def cleanup_zombie_batches():
    """On startup: mark orphaned 'running' batches as 'interrupted' AND regenerate CSVs
    for any batch (interrupted or completed) that has leads but no CSV recorded."""
    from sqlalchemy import update as sql_update
    from pipeline.delivery import regenerate_csv_for_batch

    async with SessionLocal() as s:
        result = await s.execute(sql_update(Batch).where(Batch.status == "running").values(
            status="interrupted",
            finished_at=datetime.utcnow(),
            notes="orphaned by redeploy",
        ))
        await s.commit()
        if result.rowcount:
            log.info(f"Marked {result.rowcount} orphaned batch(es) as 'interrupted'.")

        # Find batches that have verified leads but no csv_path (interrupted before delivery)
        leads_by_batch_q = (
            "SELECT batch_id, COUNT(*) FROM verified_leads "
            "WHERE batch_id IN (SELECT id FROM batches WHERE csv_path IS NULL OR csv_path = '') "
            "GROUP BY batch_id"
        )
        from sqlalchemy import text
        result = await s.execute(text(leads_by_batch_q))
        rows = result.all()

    regenerated = 0
    for batch_id, count in rows:
        if count > 0:
            try:
                await regenerate_csv_for_batch(batch_id)
                regenerated += 1
            except Exception as e:
                log.warning(f"Couldn't regen CSV for batch {batch_id}: {e}")
    if regenerated:
        log.info(f"Regenerated CSVs for {regenerated} batch(es) with orphaned leads.")


async def _startup_background():
    """Heavy startup tasks run as a background task so the server can start serving
    immediately (health check passes). Runs once after the event loop is running."""
    # Small delay so the main loop is fully up before we hit the DB
    await asyncio.sleep(2)

    # Load persistent verifier counters
    try:
        from pipeline.verifier import _load_counter
        await _load_counter()
        from pipeline.finder import _load_counter as _sk_load
        await _sk_load()
        from pipeline.mv_verifier import _load_counter as _mv_load
        await _mv_load()
    except Exception as e:
        log.warning(f"Counter load failed: {e}")

    # Mark orphaned batches as interrupted, regenerate missing CSVs
    try:
        await asyncio.wait_for(cleanup_zombie_batches(), timeout=30)
    except asyncio.TimeoutError:
        log.warning("cleanup_zombie_batches() timed out — skipping.")
    except Exception as e:
        log.warning(f"cleanup_zombie_batches() failed: {e} — skipping.")

    # If Claude parser is configured, clear no_parse URLs so they get retried.
    # Runs at most once per ISO week per persistent DB.
    if settings.ANTHROPIC_API_KEY and settings.CLAUDE_PARSE_ENABLED:
        try:
            from datetime import date as _date
            from sqlalchemy import text as _sql_text
            _week_key = f"no_parse_cleared_{_date.today().isocalendar()[0]}W{_date.today().isocalendar()[1]}"
            async with SessionLocal() as _s:
                _marker = (await _s.execute(
                    _sql_text(f"SELECT value FROM counters WHERE key='{_week_key}'")
                )).scalar_one_or_none()
            if _marker is None:
                from pipeline.orchestrator import clear_no_parse_seen
                from db import Counter
                cleared = await asyncio.wait_for(clear_no_parse_seen(), timeout=120)
                async with SessionLocal() as _s:
                    await _s.execute(_sql_text("DELETE FROM counters WHERE key LIKE 'no_parse_cleared_%'"))
                    _s.add(Counter(key=_week_key, value=1))
                    await _s.commit()
                log.info(f"Weekly no_parse clear: freed {cleared} URLs for Claude retry ({_week_key}).")
            else:
                log.info(f"no_parse already cleared this week ({_week_key}) — skipping.")
        except asyncio.TimeoutError:
            log.warning("Weekly no_parse clear timed out — will retry on pool exhaustion.")
        except Exception as e:
            log.warning(f"Weekly no_parse clear failed: {e} — will retry on pool exhaustion.")

    log.info("Background startup complete.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _perpetual_task
    # Print the public URL for easy discovery in Railway deployment logs
    _public_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN") or os.environ.get("RAILWAY_STATIC_URL", "")
    if _public_domain:
        log.info(f"=== LEAD ENGINE STARTING === https://{_public_domain} ===")
    else:
        log.info("=== LEAD ENGINE STARTING ===")

    # Import modules that declare new tables so init_db() picks them up
    import pipeline.finder  # SkrappCache
    import pipeline.company_resolver  # CompanyDomainCache

    # init_db is the only thing that MUST run before we serve requests
    await init_db()

    # Everything else runs in the background so the health check passes immediately
    asyncio.create_task(_startup_background())

    if settings.PERPETUAL_ENABLED:
        _perpetual_task = asyncio.create_task(perpetual_loop())
        log.info("Perpetual loop scheduled.")

    # Unibox sync loop (Instantly reply + bounce tracking)
    if settings.instantly_keys():
        global _unibox_task
        from pipeline.unibox import unibox_loop
        _unibox_task = asyncio.create_task(unibox_loop())
        log.info("Unibox sync loop scheduled.")

    # Intelligence loop — runs every 24h, requires ANTHROPIC_API_KEY
    if settings.ANTHROPIC_API_KEY:
        global _intelligence_task
        from pipeline.intelligence import intelligence_loop
        _intelligence_task = asyncio.create_task(intelligence_loop(run_every_hours=24))
        log.info("Intelligence loop scheduled (first run in 2h).")

    yield

    if _perpetual_task and not _perpetual_task.done():
        _perpetual_task.cancel()
    if _unibox_task and not _unibox_task.done():
        _unibox_task.cancel()
    if _intelligence_task and not _intelligence_task.done():
        _intelligence_task.cancel()


app = FastAPI(title="Lead Engine", lifespan=lifespan)


def check_auth(authorization: str | None):
    if not settings.API_TOKEN:
        return
    if not authorization or authorization != f"Bearer {settings.API_TOKEN}":
        raise HTTPException(401, "Unauthorized")


from typing import Optional
_basic = HTTPBasic(auto_error=False)

def require_dash_login(creds: Optional[HTTPBasicCredentials] = Depends(_basic)):
    """HTTP Basic Auth for browser-facing endpoints. If env unset, all open."""
    if not settings.DASH_USERNAME or not settings.DASH_PASSWORD:
        return  # auth not configured → open access
    if not creds:
        raise HTTPException(
            status_code=401,
            detail="Auth required",
            headers={"WWW-Authenticate": 'Basic realm="Lead Engine"'},
        )
    user_ok = secrets.compare_digest(creds.username, settings.DASH_USERNAME)
    pass_ok = secrets.compare_digest(creds.password, settings.DASH_PASSWORD)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401,
            detail="Wrong credentials",
            headers={"WWW-Authenticate": 'Basic realm="Lead Engine"'},
        )


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_dash_login)])
async def root():
    html = await render_dashboard(
        loop_state=_loop_state,
        perpetual_paused=_perpetual_paused,
        current_batch=await get_current_status(),
        is_running_now=await is_running(),
    )
    return HTMLResponse(content=html)


@app.get("/api", dependencies=[Depends(require_dash_login)])
async def api_root():
    """JSON endpoint (same data as the HTML dashboard, for scripts)."""
    return {
        "service": "lead-engine",
        "mode": "perpetual" if settings.PERPETUAL_ENABLED else "manual-only",
        "perpetual_paused": _perpetual_paused,
        "currently_running_batch": await is_running(),
        "current_batch": await get_current_status(),
        "ramp": {
            "current_target": _loop_state["current_target"],
            "max_target": settings.BATCH_SIZE_MAX,
            "growth_per_batch": settings.BATCH_SIZE_GROWTH,
            "completed_batches": _loop_state["completed_batches"],
        },
        "between_batch_seconds": settings.BETWEEN_BATCH_SECONDS,
    }


@app.post("/pause")
async def pause(authorization: str = Header(None)):
    """Pause the perpetual loop (Bearer-token auth — for programmatic / curl)."""
    check_auth(authorization)
    global _perpetual_paused
    _perpetual_paused = True
    return {"ok": True, "msg": "Paused."}


@app.post("/resume")
async def resume(authorization: str = Header(None)):
    """Resume the perpetual loop (Bearer-token auth)."""
    check_auth(authorization)
    global _perpetual_paused
    _perpetual_paused = False
    return {"ok": True, "msg": "Resumed."}


# Dashboard-authed control endpoints — same action, callable from browser via the dashboard
@app.post("/control/pause", dependencies=[Depends(require_dash_login)])
async def control_pause():
    global _perpetual_paused
    _perpetual_paused = True
    return JSONResponse({"ok": True, "paused": True}, headers={"Location": "/"}, status_code=302)


@app.post("/control/resume", dependencies=[Depends(require_dash_login)])
async def control_resume():
    global _perpetual_paused
    _perpetual_paused = False
    return JSONResponse({"ok": True, "paused": False}, headers={"Location": "/"}, status_code=302)


@app.post("/purge-source/{source}")
async def purge_source(source: str, authorization: str = Header(None)):
    """Delete all verified leads + raw leads from a specific source.
    Useful when a source's parser turns out to be broken (e.g. Brainz)."""
    check_auth(authorization)
    from sqlalchemy import delete as sql_delete
    from db import RawLead, SeenURL
    deleted_v = deleted_r = deleted_s = 0
    async with SessionLocal() as s:
        # Case-insensitive match
        r = await s.execute(sql_delete(VerifiedLead).where(VerifiedLead.source.ilike(source)))
        deleted_v = r.rowcount
        r = await s.execute(sql_delete(RawLead).where(RawLead.source.ilike(source)))
        deleted_r = r.rowcount
        r = await s.execute(sql_delete(SeenURL).where(SeenURL.source.ilike(source)))
        deleted_s = r.rowcount
        await s.commit()
    return {"ok": True, "source": source,
            "deleted": {"verified": deleted_v, "raw": deleted_r, "seen": deleted_s}}


@app.get("/intelligence", dependencies=[Depends(require_dash_login)])
async def intelligence_report():
    """Return the most recent intelligence analysis report."""
    from pipeline.intelligence import get_last_report
    report = await get_last_report()
    if not report:
        return {"ok": False, "msg": "No intelligence report yet. Run POST /intelligence/run to generate one."}
    return {"ok": True, "report": report}


@app.post("/intelligence/run", dependencies=[Depends(require_dash_login)])
async def run_intelligence(background_tasks: BackgroundTasks):
    """Trigger an immediate intelligence cycle (runs in background)."""
    from pipeline.intelligence import run_cycle

    async def _run():
        await run_cycle()

    background_tasks.add_task(_run)
    return {"ok": True, "msg": "Intelligence cycle started. Check /intelligence in ~30s for results."}


@app.get("/unibox/sync")
@app.post("/unibox/sync")
async def trigger_unibox_sync():
    """Manually trigger an immediate unibox sync."""
    if not settings.instantly_keys():
        return {"ok": False, "msg": "No INSTANTLY_API_KEY configured"}
    from pipeline.unibox import fetch_replies
    count = await fetch_replies(limit_pages=10)
    return {"ok": True, "newly_responded": count, "accounts": len(settings.instantly_keys())}


@app.get("/admin/instantly-debug")
async def admin_instantly_debug(ue_type: int = 3, limit: int = 5):
    """TEMP diagnostic — introspect the raw Instantly /emails payload per account.

    Shows HTTP status, item count, and (email-masked) field structure so we can
    see WHERE the bounced lead's address actually lives. ue_type=3=bounce, 2=reply.
    Masks addresses to local-initial + domain so no full PII is returned."""
    import httpx as _httpx
    keys = settings.instantly_keys()
    if not keys:
        return {"ok": False, "msg": "No INSTANTLY_API_KEY configured"}

    def _mask(addr):
        addr = (addr or "").strip()
        if "@" not in addr:
            return addr or None
        local, _, dom = addr.partition("@")
        return f"{local[:2]}***@{dom}"

    def _mask_list(v):
        if isinstance(v, list):
            return [_mask(x) for x in v][:3]
        return _mask(v) if isinstance(v, str) else v

    BOUNCE_HINTS = ("mailer-daemon", "postmaster", "mail delivery", "delivery status",
                    "undeliverable", "delivery has failed", "returned mail", "failure notice")

    out = []
    for i, key in enumerate(keys, start=1):
        acct = {"account": f"key{i}"}
        try:
            async with _httpx.AsyncClient(timeout=40,
                    headers={"Authorization": f"Bearer {key}"}) as cli:
                # Scan several pages to tally ue_type distribution + find likely bounces
                ue_counts: dict = {}
                likely_bounces = []
                cursor = None
                scanned = 0
                for _pg in range(8):
                    params = {"limit": 100}
                    if cursor:
                        params["starting_after"] = cursor
                    r = await cli.get("https://api.instantly.ai/api/v2/emails", params=params)
                    if r.status_code != 200:
                        acct["http_status"] = r.status_code
                        acct["body"] = r.text[:300]
                        break
                    acct["http_status"] = 200
                    data = r.json()
                    items = data.get("items", []) if isinstance(data, dict) else []
                    if not items:
                        break
                    for it in items:
                        scanned += 1
                        ut = it.get("ue_type")
                        ue_counts[str(ut)] = ue_counts.get(str(ut), 0) + 1
                        frm = (it.get("from_address_email") or "").lower()
                        subj = (it.get("subject") or "").lower()
                        body_prev = (it.get("content_preview") or "").lower()
                        if any(h in frm or h in subj or h in body_prev for h in BOUNCE_HINTS):
                            if len(likely_bounces) < 6:
                                likely_bounces.append({
                                    "ue_type": ut,
                                    "i_status": it.get("i_status"),
                                    "from": _mask(it.get("from_address_email")),
                                    "lead": _mask(it.get("lead")),
                                    "to_list": _mask_list(it.get("to_address_email_list")),
                                    "subject": (it.get("subject") or "")[:70],
                                })
                    cursor = data.get("next_starting_after")
                    if not cursor or len(items) < 100:
                        break
                acct["scanned"] = scanned
                acct["ue_type_distribution"] = ue_counts
                acct["likely_bounces_found"] = len(likely_bounces)
                acct["likely_bounce_samples"] = likely_bounces
        except Exception as e:
            acct["error"] = str(e)[:200]
        out.append(acct)
    return {"ok": True, "note": "ue_type param appears non-filtering; tallied client-side",
            "accounts": out}


@app.get("/admin/sync-bounces")
@app.post("/admin/sync-bounces")
async def admin_sync_bounces(pages: int = 20):
    """Pull bounce events from Instantly and mark matching leads bounced=True.

    Bounced leads are then automatically excluded from ALL exports/downloads.
    Call this after your Instantly campaigns have run to scrub dead emails out
    of the next download. `pages` controls how far back to scan (100/page)."""
    if not settings.instantly_keys():
        return {"ok": False, "msg": "No INSTANTLY_API_KEY configured in environment"}
    from pipeline.bounce_sync import fetch_bounces
    count = await fetch_bounces(limit_pages=pages)
    return {"ok": True, "newly_bounced": count, "accounts": len(settings.instantly_keys()),
            "msg": f"Marked {count} lead(s) as bounced across {len(settings.instantly_keys())} "
                   f"Instantly account(s). They're now excluded from all downloads."}


@app.get("/admin/bounce-report", dependencies=[Depends(require_dash_login)])
async def admin_bounce_report():
    """Bounce + catch-all breakdown so you can see export health at a glance."""
    from sqlalchemy import case
    async with SessionLocal() as s:
        total = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        bounced = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(VerifiedLead.bounced == True)
        )).scalar_one()
        catch_all = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(VerifiedLead.is_catch_all == True)
        )).scalar_one()
        # Exportable = not bounced and (catch-all excluded per setting)
        from pipeline.delivery import _deliverable_filter
        exportable = (await s.execute(
            select(func.count()).select_from(VerifiedLead).where(*_deliverable_filter())
        )).scalar_one()
        # Bounce rate by source (top offenders)
        from sqlalchemy import text as _t
        rows = (await s.execute(_t(
            "SELECT source, COUNT(*) AS total, "
            "SUM(CASE WHEN bounced THEN 1 ELSE 0 END) AS bounced "
            "FROM verified_leads GROUP BY source ORDER BY bounced DESC LIMIT 15"
        ))).all()
    by_source = [
        {"source": r[0], "total": r[1], "bounced": r[2],
         "bounce_rate": round((r[2] or 0) / r[1] * 100, 1) if r[1] else 0}
        for r in rows
    ]
    return {
        "ok": True,
        "total_leads": total,
        "bounced": bounced,
        "bounce_rate_pct": round(bounced / total * 100, 1) if total else 0,
        "catch_all": catch_all,
        "exportable_now": exportable,
        "excluded_from_export": total - exportable,
        "settings": {
            "ACCEPT_CATCH_ALL": settings.ACCEPT_CATCH_ALL,
            "EXPORT_CATCH_ALL": settings.EXPORT_CATCH_ALL,
        },
        "by_source": by_source,
    }


@app.get("/unibox/stats")
async def unibox_stats():
    """Reply stats per source."""
    from pipeline.unibox import get_reply_stats
    return await get_reply_stats()


@app.get("/unibox/insights")
async def unibox_insights():
    """Deep analysis of who's replying — for lookalike targeting decisions."""
    from pipeline.unibox import get_reply_insights
    return await get_reply_insights()


@app.get("/retry-stuck")
@app.post("/retry-stuck")
async def retry_stuck():
    """Clear no_emails + error URLs, and stale no_parse (>14 days), for next-batch retry."""
    from pipeline.orchestrator import clear_stuck_seen, clear_old_no_parse
    cleared = await clear_stuck_seen(include_no_parse=False)
    np_stale = await clear_old_no_parse(days=0)  # force-clear ALL no_parse when called manually
    total = cleared + np_stale
    return {
        "ok": True,
        "cleared": total,
        "breakdown": {"no_emails_error": cleared, "no_parse": np_stale},
        "msg": f"Cleared {total} URLs (no_emails/error: {cleared}, no_parse: {np_stale}). They'll be reprocessed next batch.",
    }


@app.get("/retry-no-parse")
@app.post("/retry-no-parse")
async def retry_no_parse():
    """Clear no_parse URLs so Claude parser can attempt them on the next batch.
    Only useful when ANTHROPIC_API_KEY is set. Open endpoint — call from browser."""
    if not settings.ANTHROPIC_API_KEY:
        return {"ok": False, "msg": "ANTHROPIC_API_KEY not set — Claude parser inactive. Set the key first."}
    from pipeline.orchestrator import clear_no_parse_seen
    cleared = await clear_no_parse_seen()
    return {"ok": True, "cleared": cleared, "msg": f"Cleared {cleared} no_parse URLs. Claude will retry them in the next batch."}


@app.get("/admin/reset-all")
@app.post("/admin/reset-all")
async def admin_reset_all():
    """NUCLEAR option — clears ALL seen_urls rows including 'parsed'.

    Use this when the engine is stuck with 0 leads and pool recycling hasn't
    helped. The engine restarts completely fresh on the next batch.

    Safe because:
    - save_verified() deduplicates by email — no duplicate leads stored
    - save_raw_lead() deduplicates by source_url — no duplicate raw rows
    Only the 'already processed' dedup filter is cleared, not the actual leads.
    """
    from sqlalchemy import delete as sql_delete
    from db import SeenURL
    async with SessionLocal() as s:
        r = await s.execute(sql_delete(SeenURL))
        total = r.rowcount
        await s.commit()
    log.info(f"admin reset-all: cleared ALL {total} seen_urls rows")
    return {
        "ok": True,
        "total_cleared": total,
        "msg": (
            f"Cleared ALL {total} seen_urls rows (including 'parsed'). "
            "The engine will reprocess every source from scratch. "
            "Verified leads are untouched — deduplication prevents re-adding them."
        ),
    }


@app.get("/admin/reset-seen")
@app.post("/admin/reset-seen")
async def admin_reset_seen(
    no_parse: bool = True,
    claude_no_parse: bool = True,
    no_emails: bool = True,
):
    """Nuclear pool reset — clears SeenURL rows so the engine treats them as fresh.

    Useful when:
     - URL pool is exhausted and you want a full restart
     - You've added new sources and want them processed ASAP
     - The engine has been running a long time and seen everything

    Does NOT delete verified leads or raw leads — only the dedup filter.

    Query params (all default True):
      no_parse=true          — regex-failed articles (Claude will retry)
      claude_no_parse=true   — Claude-failed articles (expensive to retry)
      no_emails=true         — parsed OK but no emails found
    Error rows are always cleared.
    """
    from sqlalchemy import delete as sql_delete
    from db import SeenURL
    cleared: dict[str, int] = {}
    async with SessionLocal() as s:
        if no_parse:
            r = await s.execute(sql_delete(SeenURL).where(SeenURL.status == "no_parse"))
            cleared["no_parse"] = r.rowcount
        if claude_no_parse:
            r = await s.execute(sql_delete(SeenURL).where(SeenURL.status == "claude_no_parse"))
            cleared["claude_no_parse"] = r.rowcount
        if no_emails:
            r = await s.execute(sql_delete(SeenURL).where(SeenURL.status == "no_emails"))
            cleared["no_emails"] = r.rowcount
        r = await s.execute(sql_delete(SeenURL).where(SeenURL.status == "error"))
        cleared["error"] = r.rowcount
        await s.commit()
    total = sum(cleared.values())
    log.info(f"admin reset-seen: cleared {total} rows — {cleared}")
    return {
        "ok": True,
        "cleared": cleared,
        "total": total,
        "msg": (
            f"Cleared {total} SeenURL rows. The engine will reprocess all of these "
            "on the next batch. Verified leads are untouched."
        ),
    }


@app.get("/health")
async def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}


@app.get("/debug-quick")
async def debug_quick():
    """Fast health snapshot — skips sitemap fetches. Use this when /debug is too slow."""
    out = {"ts": datetime.utcnow().isoformat()}
    try:
        from pipeline.mv_verifier import get_state as mv_state, _load_counter as _mv_load
        await _mv_load()
        out["mv"] = mv_state()
    except Exception as e:
        out["mv"] = {"error": str(e)[:200]}
    try:
        from pipeline.finder import get_state as skrapp_state, _load_counter as _sk_load
        await _sk_load()
        out["skrapp"] = skrapp_state()
    except Exception as e:
        out["skrapp"] = {"error": str(e)[:200]}
    try:
        from pipeline.hunter import get_state as hunter_state
        out["hunter"] = hunter_state()
    except Exception as e:
        out["hunter"] = {"error": str(e)[:200]}
    try:
        from pipeline.reoon_pool import get_pool as _rpool
        p = _rpool()
        out["reoon_pool"] = {"keys": len(p)}
    except Exception as e:
        out["reoon_pool"] = {"error": str(e)[:200]}
    async with SessionLocal() as s:
        from db import RawLead, SeenURL
        from sqlalchemy import text
        out["db"] = {
            "seen_urls": (await s.execute(select(func.count()).select_from(SeenURL))).scalar_one(),
            "raw_leads": (await s.execute(select(func.count()).select_from(RawLead))).scalar_one(),
            "verified_leads": (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one(),
            "responded": (await s.execute(select(func.count()).select_from(VerifiedLead).where(VerifiedLead.responded == True))).scalar_one(),
        }
        r = await s.execute(text("SELECT status, COUNT(*) FROM seen_urls GROUP BY status"))
        out["seen_status"] = {row[0]: row[1] for row in r.all()}
    out["loop"] = {
        "paused": _perpetual_paused,
        "currently_running": await is_running(),
        "current_batch": await get_current_status(),
    }
    return out


@app.get("/debug")
async def debug():
    """Diagnostic endpoint (open) — shows source pool sizes, Reoon health, recent batch state. No PII."""
    from pipeline.sources import collect_all_urls
    from pipeline.verifier import verify_email

    diag = {"ts": datetime.utcnow().isoformat()}

    # 1. Source pools
    try:
        all_urls = await asyncio.wait_for(collect_all_urls(), timeout=60)
        diag["sources"] = {k: len(v) for k, v in all_urls.items()}
        diag["total_pool"] = sum(diag["sources"].values())
    except Exception as e:
        diag["sources_error"] = str(e)[:200]

    # 2. Reoon health
    try:
        r = await asyncio.wait_for(verify_email("test@gmail.com"), timeout=20)
        diag["reoon"] = {"reachable": r is not None, "sample_response_keys": list(r.keys())[:8] if r else None}
    except Exception as e:
        diag["reoon"] = {"reachable": False, "error": str(e)[:200]}

    # 3. DB counts
    async with SessionLocal() as s:
        from db import SeenURL, RawLead, VerifiedLead
        diag["db"] = {
            "seen_urls": (await s.execute(select(func.count()).select_from(SeenURL))).scalar_one(),
            "raw_leads": (await s.execute(select(func.count()).select_from(RawLead))).scalar_one(),
            "verified_leads": (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one(),
        }
        # Last 5 batches with their state
        recent = (await s.execute(
            select(Batch).order_by(desc(Batch.id)).limit(5)
        )).scalars().all()
        diag["recent_batches"] = [
            {"id": b.id, "status": b.status, "target": b.target,
             "delivered": b.delivered_count, "trigger": b.trigger,
             "started": b.started_at.isoformat() if b.started_at else None,
             "finished": b.finished_at.isoformat() if b.finished_at else None,
             "notes": (b.notes or "")[:200]}
            for b in recent
        ]

    # 3b. Seen-URL status breakdown
    try:
        async with SessionLocal() as s:
            from sqlalchemy import text as sql_text
            r = await s.execute(sql_text("SELECT status, COUNT(*) FROM seen_urls GROUP BY status"))
            diag["seen_status"] = {row[0]: row[1] for row in r.all()}
    except Exception as e:
        diag["seen_status_err"] = str(e)[:100]

    # 4. Skrapp state
    try:
        from pipeline.finder import get_state as skrapp_state, _load_counter as _sk_load
        await _sk_load()
        diag["skrapp"] = skrapp_state()
    except Exception as e:
        diag["skrapp"] = {"error": str(e)[:200]}

    # 4b. MillionVerifier state
    try:
        from pipeline.mv_verifier import get_state as mv_state, _load_counter as _mv_load
        await _mv_load()
        diag["mv"] = mv_state()
    except Exception as e:
        diag["mv"] = {"error": str(e)[:200]}

    # 4c-ii. Hunter state
    try:
        from pipeline.hunter import get_state as hunter_state
        diag["hunter"] = hunter_state()
    except Exception as e:
        diag["hunter"] = {"error": str(e)[:200]}

    # 4c. Reoon pool state
    try:
        from pipeline.reoon_pool import get_pool as _rpool
        p = _rpool()
        diag["reoon_pool"] = {"keys": len(p), "has_keys": p.has_keys()}
    except Exception as e:
        diag["reoon_pool"] = {"error": str(e)[:200]}

    # 5. Current loop state
    diag["loop"] = {
        "paused": _perpetual_paused,
        "current_target": _loop_state.get("current_target"),
        "completed_batches": _loop_state.get("completed_batches"),
        "currently_running": await is_running(),
        "current_batch": await get_current_status(),
    }
    return diag


@app.get("/status", dependencies=[Depends(require_dash_login)])
async def status():
    async with SessionLocal() as s:
        total_verified = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
        recent_batches = (await s.execute(
            select(Batch).order_by(desc(Batch.id)).limit(10)
        )).scalars().all()
    return {
        "total_verified_all_time": total_verified,
        "currently_running": await is_running(),
        "current_batch": await get_current_status(),
        "recent_batches": [
            {
                "id": b.id, "status": b.status,
                "trigger": b.trigger, "target": b.target,
                "delivered_count": b.delivered_count,
                "started_at": b.started_at.isoformat() if b.started_at else None,
                "finished_at": b.finished_at.isoformat() if b.finished_at else None,
                "csv_path": b.csv_path,
            } for b in recent_batches
        ],
    }


@app.post("/run")
async def trigger_run(
    background_tasks: BackgroundTasks,
    target: int = None,
    authorization: str = Header(None),
):
    check_auth(authorization)
    if await is_running():
        return JSONResponse({"ok": False, "msg": "A batch is already running."}, status_code=409)
    target = target or settings.DEFAULT_TARGET

    async def runner():
        await run_batch(target=target, trigger="manual")

    background_tasks.add_task(runner)
    return {"ok": True, "msg": f"Batch started, target={target}. Check /status for progress."}


@app.get("/download-all.csv", dependencies=[Depends(require_dash_login)])
async def download_all():
    """Download EVERY verified Tier-A lead ever, in one CSV."""
    from pipeline.delivery import deliver_all_leads_csv
    path = await deliver_all_leads_csv()
    return FileResponse(path, filename=os.path.basename(path), media_type="text/csv")


@app.get("/download-range", dependencies=[Depends(require_dash_login)])
async def download_range(start: int = 1, end: int = 2000):
    """Download a numbered slice of clean leads, e.g. start=8001&end=10000.

    Numbering is over the deliverable set only (bounced + catch-all excluded),
    oldest-first, so the same range always means the same leads. Lets you pull
    'the next 2000' without re-downloading what you already have."""
    if end < start:
        raise HTTPException(400, "end must be >= start")
    if end - start + 1 > 50000:
        raise HTTPException(400, "range too large (max 50,000 rows per download)")
    from pipeline.delivery import deliver_leads_range
    path, rows, total = await deliver_leads_range(start, end)
    if rows == 0:
        raise HTTPException(404, f"No leads in range {start}-{end} (only {total} clean leads exist)")
    return FileResponse(path, filename=os.path.basename(path), media_type="text/csv")


@app.get("/admin/purge-bad")
@app.post("/admin/purge-bad")
async def admin_purge_bad(bounced: bool = True, catch_all: bool = True):
    """Permanently DELETE bad leads from the DB so the portal only holds clean ones.

    bounced=true   — delete leads Instantly confirmed bounced
    catch_all=true — delete catch-all-domain leads (silent-bounce risk)

    Run /admin/sync-bounces first so the bounced flags are current. Catch-all
    leads can be purged immediately (no Instantly needed). Good leads untouched."""
    from sqlalchemy import delete as sql_delete, or_ as _or
    deleted = {}
    async with SessionLocal() as s:
        if bounced:
            r = await s.execute(sql_delete(VerifiedLead).where(VerifiedLead.bounced == True))
            deleted["bounced"] = r.rowcount
        if catch_all:
            r = await s.execute(sql_delete(VerifiedLead).where(VerifiedLead.is_catch_all == True))
            deleted["catch_all"] = r.rowcount
        await s.commit()
        remaining = (await s.execute(select(func.count()).select_from(VerifiedLead))).scalar_one()
    total_deleted = sum(deleted.values())
    log.info(f"admin purge-bad: deleted {total_deleted} leads — {deleted}")
    return {"ok": True, "deleted": deleted, "total_deleted": total_deleted,
            "remaining_leads": remaining,
            "msg": f"Deleted {total_deleted} bad lead(s). {remaining} clean leads remain in the portal."}


@app.get("/download/{filename}", dependencies=[Depends(require_dash_login)])
async def download(filename: str):
    """Open: filenames are unguessable (batch_id + timestamp), no auth required."""
    safe = os.path.basename(filename)
    path = os.path.join(settings.DATA_DIR, safe)
    if not os.path.exists(path):
        raise HTTPException(404, "Not found")
    return FileResponse(path, filename=safe, media_type="text/csv")


@app.get("/leads/recent", dependencies=[Depends(require_dash_login)])
async def recent_leads(limit: int = 50):
    async with SessionLocal() as s:
        result = await s.execute(
            select(VerifiedLead).order_by(desc(VerifiedLead.id)).limit(limit)
        )
        leads = result.scalars().all()
    return [
        {"id": l.id, "name": l.name, "email": l.email, "company": l.company,
         "website": l.website, "source": l.source, "tier": l.tier,
         "score": l.reoon_score, "created_at": l.created_at.isoformat() if l.created_at else None}
        for l in leads
    ]


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
