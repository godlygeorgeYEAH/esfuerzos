"""
main.py - Reune VE API v2.0

Transport: WAHA WhatsApp (self-hosted, number +5731157915931).
Receives webhooks from WAHA at POST /webhook/waha (message event).

Services:
  - APScheduler: scraper jobs every 5 min (poll) and 1 hour (full sweep)
  - InsightFace buffalo_sc: 512-dim face embeddings on CPU
  - SentenceTransformer: 768-dim text embeddings
  - StaticFiles: serves /root/sos_images and /root/crisis_images at /sos_images, /crisis_images
  - Admin: POST /admin/bulk_import triggers batch import of pre-existing data
"""

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager

import httpx
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, BackgroundTasks, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from insightface.app import FaceAnalysis
from sentence_transformers import SentenceTransformer
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from slowapi.util import get_remote_address

from config import get_settings
from consolidation_pipeline import (
    compute_text_embeddings,
    run_cedula_exact_match,
    run_text_cross_match,
    run_face_cross_match,
    run_full_consolidation,
)
from dedup_pipeline import run_dedup_pipeline
from face_backfill import run_face_backfill
from notify_pipeline import run_match_notifier
from scraper_orchestrator import _make_scrapers, _run_full, _run_poll, _startup_sweep
from waha_intake import router as waha_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])



@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Loading SentenceTransformer...")
    app.state.text_model = SentenceTransformer(settings.embeddings_model)
    app.state.supabase_url = settings.supabase_url
    app.state.supabase_service_key = settings.supabase_service_role_key
    logger.info("Text model loaded.")

    logger.info("Loading InsightFace buffalo_sc...")
    face_model = FaceAnalysis("buffalo_sc", providers=["CPUExecutionProvider"])
    face_model.prepare(ctx_id=-1, det_size=(640, 640))
    app.state.face_model = face_model
    logger.info("Face model loaded.")

    scrapers = _make_scrapers()
    scheduler = AsyncIOScheduler(timezone="UTC")
    for name in scrapers:
        scheduler.add_job(
            _run_poll, IntervalTrigger(seconds=300),
            args=[name, scrapers], id=f"{name}_poll", max_instances=1,
        )
        scheduler.add_job(
            _run_full, IntervalTrigger(seconds=3600),
            args=[name, scrapers], id=f"{name}_full", max_instances=1,
        )
    scheduler.start()
    app.state.scrapers = scrapers
    app.state.scheduler = scheduler

    # Periodic embedding + cross-match for newly scraped records
    scheduler.add_job(
        compute_text_embeddings, IntervalTrigger(seconds=1800),
        args=[app], id="embed_new_reports", max_instances=1,
    )
    scheduler.add_job(
        run_text_cross_match, IntervalTrigger(seconds=3600),
        args=[app], id="text_cross_match", max_instances=1,
    )

    # Background deduplication: cluster near-duplicate reports across scrapers
    # and annotate non-canonical rows so fuzzy search/review can collapse them.
    scheduler.add_job(
        run_dedup_pipeline, IntervalTrigger(seconds=14400),  # every 4h
        args=[app], id="dedup_pipeline", max_instances=1,
    )

    # Proactive notifier: WhatsApp the family when a background cross-match
    # later finds a high-confidence match for their report.
    scheduler.add_job(
        run_match_notifier, IntervalTrigger(seconds=600),  # every 10 min
        args=[app], id="match_notifier", max_instances=1,
    )

    # Face backfill: embed scraped photos (foto_url) so photo matching has data.
    scheduler.add_job(
        run_face_backfill, IntervalTrigger(seconds=600),  # every 10 min, small batches
        args=[app], id="face_backfill", max_instances=1,
    )

    asyncio.create_task(_startup_sweep(scrapers))
    logger.info("Startup complete: %d scraper jobs, WAHA transport active", len(scheduler.get_jobs()))

    yield

    scheduler.shutdown(wait=False)
    for scraper in scrapers.values():
        await scraper.close()
    logger.info("Shutdown complete.")


app = FastAPI(title="Reune VE API", version="2.0.0", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# C3: CORS from env var
_origins = [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve local photo directories
for _mount_path, _dir in [
    ("/sos_images", "/root/sos_images"),
    ("/crisis_images", "/root/crisis_images"),
    ("/reconexion_images", "/root/reconexion_images"),
    ("/venezreporta_images", "/root/venezreporta_images"),
]:
    if os.path.isdir(_dir):
        app.mount(_mount_path, StaticFiles(directory=_dir), name=_mount_path.lstrip("/"))

app.include_router(waha_router)


@app.get("/health")
async def health():
    results = {
        "waha": False,
        "supabase": False,
        "text_model": hasattr(app.state, "text_model") and app.state.text_model is not None,
        "face_model": hasattr(app.state, "face_model") and app.state.face_model is not None,
        "scrapers": list(getattr(app.state, "scrapers", {}).keys()),
    }
    waha_url = settings.waha_url.rstrip("/")
    async with httpx.AsyncClient(timeout=15) as cl:
        try:
            r = await cl.get(f"{waha_url}/api/sessions")
            results["waha"] = r.status_code < 500
        except Exception:
            pass
        try:
            r = await cl.get(
                f"{settings.supabase_url}/rest/v1/reports?limit=0",
                headers={"Authorization": f"Bearer {settings.supabase_service_role_key}",
                         "apikey": settings.supabase_service_role_key},
            )
            results["supabase"] = r.status_code < 400
        except Exception:
            pass
    return {"ok": True, **results}


# C1: Admin endpoint with X-Admin-Key header
@app.post("/admin/bulk_import")
async def admin_bulk_import(
    background_tasks: BackgroundTasks,
    source: str = "all",
    x_admin_key: str = Header(default=""),
):
    """Trigger batch import of pre-existing data. Requires X-Admin-Key header."""
    if settings.admin_key and not secrets.compare_digest(x_admin_key, settings.admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")
    from bulk_importer import run_full_import, import_sos_persons, import_crisis_posts
    if source == "sos_persons":
        background_tasks.add_task(import_sos_persons, app, limit=50000, offset=0, process_faces=True)
    elif source == "crisis_posts":
        background_tasks.add_task(import_crisis_posts, app, limit=100000, offset=0)
    else:
        background_tasks.add_task(run_full_import, app)
    return {"ok": True, "message": f"Bulk import started (source={source})"}


@app.post("/admin/consolidate")
async def admin_consolidate(
    background_tasks: BackgroundTasks,
    phase: int = 0,
    x_admin_key: str = Header(default=""),
):
    """
    Trigger the data consolidation pipeline.

    phase=0 (default): run all three phases
    phase=1: text embedding only
    phase=2: text cross-match only
    phase=3: face cross-match only

    Requires X-Admin-Key header when ADMIN_KEY env var is set.
    Each phase is idempotent and safe to re-run.
    """
    if settings.admin_key and not secrets.compare_digest(x_admin_key, settings.admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")

    if phase == 1:
        background_tasks.add_task(compute_text_embeddings, app)
        msg = "Phase 1: text embedding started"
    elif phase == 2:
        background_tasks.add_task(run_text_cross_match, app)
        msg = "Phase 2: text cross-match started"
    elif phase == 3:
        background_tasks.add_task(run_face_cross_match, app)
        msg = "Phase 3: face cross-match started"
    elif phase == 4:
        background_tasks.add_task(run_cedula_exact_match, app)
        msg = "Phase 0: cedula exact match started"
    else:
        background_tasks.add_task(run_full_consolidation, app)
        msg = "Full consolidation pipeline started (phases 1-3)"

    return {"ok": True, "message": msg}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
