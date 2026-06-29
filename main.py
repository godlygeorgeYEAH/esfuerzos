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
from fastapi.responses import HTMLResponse, Response
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
# B1: NO global per-IP default. All WhatsApp webhooks arrive from a single source
# (the WAHA container), so a per-IP cap would throttle ALL users to one bucket and
# drop messages in a real surge. Rate limiting is done PER-PHONE in waha_intake
# instead. The limiter object stays for optional explicit per-route limits.
limiter = Limiter(key_func=get_remote_address, default_limits=[])



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

# SECURITY (F5/V11): the local photo directories are NOT mounted publicly.
# They contained crisis-victim face images served unauthenticated, which (with
# the API exposed) allowed biometric harvesting. The face pipeline downloads
# images by URL and does not need these mounts. If an authenticated admin viewer
# is needed later, serve via a token-gated endpoint or Supabase signed URLs.

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
    _check_admin(x_admin_key)
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

    Requires X-Admin-Key header. Admin is disabled if ADMIN_KEY is not set.
    Each phase is idempotent and safe to re-run.
    """
    _check_admin(x_admin_key)

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


def _check_admin(x_admin_key: str) -> None:
    # Fail-closed (F2/V2): if no admin key is configured, admin is DISABLED, not open.
    if not settings.admin_key:
        raise HTTPException(status_code=503, detail="Admin disabled: ADMIN_KEY not configured")
    if not secrets.compare_digest(x_admin_key, settings.admin_key):
        raise HTTPException(status_code=403, detail="Invalid admin key")


@app.post("/admin/llm-scrape")
async def admin_llm_scrape(
    url: str = "",
    text: str = "",
    dry_run: bool = False,
    x_admin_key: str = Header(default=""),
):
    """LLM panel (Mode A): extract person records from a URL or pasted text into
    the llm_leads REVIEW QUEUE (human-approved before entering the corpus)."""
    _check_admin(x_admin_key)
    from llm_extractor import extract_to_queue
    result = await extract_to_queue(url=url or None, text=text or None, dry_run=dry_run)
    return {"ok": True, **result}


@app.post("/admin/llm-approve")
async def admin_llm_approve(lead_id: str, x_admin_key: str = Header(default="")):
    """Promote a reviewed llm_leads row into the canonical reports table."""
    _check_admin(x_admin_key)
    from llm_extractor import approve_lead
    return await approve_lead(lead_id, app)


# Hospital/shelter "found" sources: a match against one of these is a real
# reunification lead (the person was physically located), not another "se busca".
_HOSPITAL_SOURCES = {
    "hospital_consolidado", "hospitales_26jun", "pacientes_terremoto",
    "google_drive_hospital", "hospitales_ve",
}


@app.get("/admin/matches")
async def admin_list_matches(
    status: str = "pending",
    mode: str = "high",
    min_score: float = 0.0,
    limit: int = 50,
    x_admin_key: str = Header(default=""),
):
    """Human review queue: BUSCADO ↔ ENCONTRADO candidates, best first.

    The point of review is to connect a missing person to a FOUND record
    (hospital/shelter). To keep the queue actionable we, by default:
      - mode='high': only face matches or near-exact (combined>=0.9, e.g. cédula).
        Text-only name guesses (the bulk) are noise at corpus scale; pass
        mode='all' to include them.
      - drop matches whose missing OR found side is a known duplicate
        (raw_data.possible_duplicate_of) so the SAME person listed in several
        sources is not reviewed many times.
      - collapse to the single best found candidate per buscado.
    """
    _check_admin(x_admin_key)
    sb = settings.supabase_url.rstrip("/")
    k = settings.supabase_service_role_key
    hdr = {"apikey": k, "Authorization": f"Bearer {k}"}
    params = {
        "select": "id,missing_id,found_id,face_score,text_score,combined_score,status,created_at",
        "status": f"eq.{status}",
        "order": "combined_score.desc",
        "limit": "1000" if mode != "all" else "400",  # fetch wide, then filter/collapse
    }
    if mode == "high":
        # face match OR near-exact (cédula lands combined=1.0)
        params["or"] = "(face_score.gt.0,combined_score.gte.0.9)"
    elif min_score > 0:
        params["combined_score"] = f"gte.{min_score}"
    async with httpx.AsyncClient(timeout=15) as cl:
        r = await cl.get(f"{sb}/rest/v1/matches", headers=hdr, params=params)
        matches = r.json() if r.status_code == 200 else []
        ids = list({m[k2] for m in matches for k2 in ("missing_id", "found_id") if m.get(k2)})
        # Enrich in chunks: a single in.(...) with ~2000 UUIDs overflows the URL.
        reps = {}
        sel = ("id,full_name,age,last_seen_location,source,source_url,kind,"
               "distinguishing_marks,person_state,dup:raw_data->>possible_duplicate_of")
        for i in range(0, len(ids), 100):
            chunk = ids[i:i + 100]
            rr = await cl.get(f"{sb}/rest/v1/reports", headers=hdr,
                              params={"select": sel, "id": f"in.({','.join(chunk)})"})
            if rr.status_code == 200:
                for x in rr.json():
                    reps[x["id"]] = x
    out = []
    seen_missing = set()
    for m in matches:
        miss = reps.get(m.get("missing_id"))
        found = reps.get(m.get("found_id"))
        if not miss or not found:
            continue
        # Skip cross-source duplicates of the same person (either side).
        if miss.get("dup") or found.get("dup"):
            continue
        found["is_hospital"] = found.get("source") in _HOSPITAL_SOURCES
        if mode in ("high", "hospital"):
            # The reunification that matters: buscado -> ENCONTRADO en hospital/refugio.
            if not found["is_hospital"]:
                continue
            # A real match links DIFFERENT sources, not two entries on the same
            # board (same-source face=1.0 is usually a reused/placeholder photo).
            if miss.get("source") and miss.get("source") == found.get("source"):
                continue
        # One best (highest combined, already sorted) candidate per buscado.
        if m.get("missing_id") in seen_missing:
            continue
        seen_missing.add(m.get("missing_id"))
        m["missing"] = miss
        m["found"] = found
        out.append(m)
        if len(out) >= limit:
            break
    return {"ok": True, "count": len(out), "mode": mode, "matches": out}


@app.post("/admin/match-review")
async def admin_match_review(
    match_id: str,
    decision: str,
    x_admin_key: str = Header(default=""),
):
    """Human verifies a match: decision = 'confirmed' | 'rejected'. Confirmed
    matches get picked up by the notifier and the family is alerted."""
    _check_admin(x_admin_key)
    if decision not in ("confirmed", "rejected"):
        raise HTTPException(status_code=400, detail="decision must be confirmed|rejected")
    from datetime import datetime, timezone
    sb = settings.supabase_url.rstrip("/")
    k = settings.supabase_service_role_key
    hdr = {"apikey": k, "Authorization": f"Bearer {k}", "Content-Type": "application/json",
           "Prefer": "return=minimal"}
    payload = {"status": decision, "reviewer": "dashboard",
               "reviewed_at": datetime.now(timezone.utc).isoformat()}
    async with httpx.AsyncClient(timeout=15) as cl:
        resp = await cl.patch(f"{sb}/rest/v1/matches", headers=hdr,
                              params={"id": f"eq.{match_id}"},
                              json=payload)
        ok = resp.status_code in (200, 204)
    return {"ok": ok, "match_id": match_id, "status": decision}


# Approval dashboard (human review UI). Served from FastAPI behind the firewall;
# reach it over an SSH tunnel: ssh -L 8080:localhost:8080 root@<vps> then open
# http://localhost:8080/admin/dashboard. The HTML shell carries no data — every
# data call from the page is gated by the ADMIN_KEY the reviewer enters.
_DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "admin_dashboard.html")


@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard():
    try:
        with open(_DASHBOARD_PATH, encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="dashboard asset missing")


# Hospital/shelter sources whose 'found' reports mean the person was physically
# located. Used to emphasize real matches in the dashboard (kept in sync with
# the HOSP set in admin_dashboard.html).
_PHOTO_KEYS = ("foto_url", "photoUrl", "photo_url", "foto", "image_url", "imageUrl")
_PHOTO_BASE = {"venezuela_te_busca": "https://venezuelatebusca.com"}


def _resolve_photo_url(report: dict) -> str | None:
    """Best displayable image URL for a report: original source photo from
    raw_data first (resolving relative paths), else a stored photo URL."""
    raw = report.get("raw_data") or {}
    source = report.get("source") or ""
    if isinstance(raw, dict):
        for key in _PHOTO_KEYS:
            v = raw.get(key)
            if isinstance(v, str) and v.strip():
                v = v.strip()
                if v.startswith("http"):
                    return v
                if v.startswith("/") and source in _PHOTO_BASE:
                    return _PHOTO_BASE[source] + v
    return None


@app.get("/admin/photo/{report_id}")
async def admin_photo(report_id: str, k: str = "", x_admin_key: str = Header(default="")):
    """Admin-gated image proxy: streams a report's photo so faces can be reviewed
    without re-exposing a public photo mount. Auth via header or ?k= (the latter
    lets <img src> work; access is SSH-tunnel-only behind the firewall)."""
    _check_admin(x_admin_key or k)
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    hdr = {"apikey": key, "Authorization": f"Bearer {key}"}
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as cl:
        rr = await cl.get(f"{sb}/rest/v1/reports", headers=hdr, params={
            "id": f"eq.{report_id}", "select": "source,raw_data", "limit": "1"})
        rows = rr.json() if rr.status_code == 200 else []
        url = _resolve_photo_url(rows[0]) if rows else None
        if not url:
            # Fall back to a stored photo URL (storage_path is a full URL).
            pr = await cl.get(f"{sb}/rest/v1/photos", headers=hdr, params={
                "report_id": f"eq.{report_id}", "select": "storage_path", "limit": "1"})
            prows = pr.json() if pr.status_code == 200 else []
            sp = (prows[0].get("storage_path") if prows else None) or ""
            url = sp if sp.startswith("http") else None
        if not url:
            raise HTTPException(status_code=404, detail="no photo")
        try:
            img = await cl.get(url, headers={"User-Agent": "Mozilla/5.0 (ReuneVE-admin)"})
            if img.status_code != 200 or not img.content:
                raise HTTPException(status_code=404, detail="photo fetch failed")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=404, detail="photo unreachable")
    ctype = img.headers.get("content-type", "image/jpeg")
    if not ctype.startswith("image/"):
        ctype = "image/jpeg"
    return Response(content=img.content, media_type=ctype,
                    headers={"Cache-Control": "private, max-age=300"})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
