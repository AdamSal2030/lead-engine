from __future__ import annotations
"""FastAPI app + perpetual background lead-finder loop."""
import asyncio
import logging
import os
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import select, desc, func

from config import settings
from db import init_db, SessionLocal, Batch, VerifiedLead
from pipeline.orchestrator import run_batch, get_current_status, is_running
from pipeline.delivery import notify_sources_exhausted

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("app")

# Perpetual-loop control
_perpetual_task: asyncio.Task | None = None
_perpetual_paused = False
_last_exhausted_notice: datetime | None = None


async def perpetual_loop():
    """Run batch after batch, forever. Sleep briefly between, longer when sources dry."""
    global _last_exhausted_notice
    log.info("=== PERPETUAL LOOP STARTED — running until manually stopped ===")
    while True:
        if _perpetual_paused:
            await asyncio.sleep(60)
            continue
        try:
            result = await run_batch(target=settings.BATCH_SIZE, trigger="auto")
            verified = result.get("verified", 0) or 0
            log.info(f"Auto batch finished: verified={verified}, ok={result.get('ok')}, msg={result.get('msg', '')[:100]}")

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _perpetual_task
    await init_db()
    if settings.PERPETUAL_ENABLED:
        _perpetual_task = asyncio.create_task(perpetual_loop())
        log.info("Perpetual loop scheduled.")
    yield
    if _perpetual_task and not _perpetual_task.done():
        _perpetual_task.cancel()


app = FastAPI(title="Lead Engine", lifespan=lifespan)


def check_auth(authorization: str | None):
    if not settings.API_TOKEN:
        return
    if not authorization or authorization != f"Bearer {settings.API_TOKEN}":
        raise HTTPException(401, "Unauthorized")


@app.get("/")
async def root():
    return {
        "service": "lead-engine",
        "mode": "perpetual" if settings.PERPETUAL_ENABLED else "manual-only",
        "perpetual_paused": _perpetual_paused,
        "currently_running_batch": await is_running(),
        "current_batch": await get_current_status(),
        "batch_size": settings.BATCH_SIZE,
        "between_batch_seconds": settings.BETWEEN_BATCH_SECONDS,
    }


@app.post("/pause")
async def pause(authorization: str = Header(None)):
    """Pause the perpetual loop (current batch finishes, no new batches start)."""
    check_auth(authorization)
    global _perpetual_paused
    _perpetual_paused = True
    return {"ok": True, "msg": "Paused. Resume with POST /resume."}


@app.post("/resume")
async def resume(authorization: str = Header(None)):
    """Resume the perpetual loop."""
    check_auth(authorization)
    global _perpetual_paused
    _perpetual_paused = False
    return {"ok": True, "msg": "Resumed. Next batch will start within 60s."}


@app.get("/health")
async def health():
    return {"ok": True, "ts": datetime.utcnow().isoformat()}


@app.get("/status")
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


@app.get("/download/{filename}")
async def download(filename: str):
    """Open: filenames are unguessable (batch_id + timestamp), no auth required."""
    safe = os.path.basename(filename)
    path = os.path.join(settings.DATA_DIR, safe)
    if not os.path.exists(path):
        raise HTTPException(404, "Not found")
    return FileResponse(path, filename=safe, media_type="text/csv")


@app.get("/leads/recent")
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
