from __future__ import annotations
"""FastAPI app + APScheduler weekly cron."""
import asyncio
import logging
import os
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select, desc, func

from config import settings
from db import init_db, SessionLocal, Batch, VerifiedLead
from pipeline.orchestrator import run_batch, get_current_status, is_running

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("app")

scheduler = AsyncIOScheduler()


async def cron_job():
    log.info("Cron trigger: starting scheduled batch")
    await run_batch(target=settings.DEFAULT_TARGET, trigger="cron")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    if settings.CRON_ENABLED:
        scheduler.add_job(
            cron_job,
            CronTrigger(day_of_week=settings.CRON_DAY, hour=settings.CRON_HOUR, minute=0),
            id="weekly_run", replace_existing=True, max_instances=1,
        )
        scheduler.start()
        log.info(f"Scheduler started — weekly run on {settings.CRON_DAY} @ {settings.CRON_HOUR:02}:00 UTC")
    yield
    scheduler.shutdown(wait=False)


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
        "running": await is_running(),
        "current_batch": await get_current_status(),
        "cron_enabled": settings.CRON_ENABLED,
        "cron_schedule": f"{settings.CRON_DAY} @ {settings.CRON_HOUR:02}:00 UTC",
    }


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
async def download(filename: str, authorization: str = Header(None)):
    check_auth(authorization)
    # only allow filenames inside DATA_DIR
    safe = os.path.basename(filename)
    path = os.path.join(settings.DATA_DIR, safe)
    if not os.path.exists(path):
        raise HTTPException(404, "Not found")
    return FileResponse(path, filename=safe, media_type="text/csv")


@app.get("/leads/recent")
async def recent_leads(limit: int = 50, authorization: str = Header(None)):
    check_auth(authorization)
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
