"""
face_backfill.py - Embed scraped photos so WhatsApp photo matching has data.

Problem: photo (face) matching only compares against reports that already have a
face embedding. At launch only ~2.3k of 64k+ reports did, so a family's photo
almost always returned 0 matches. Yet venezreporta (~46k) and sos_laguaira carry
a foto_url in raw_data. This job downloads and embeds those faces in controlled
batches, building the searchable face DB over time.

Approach (incremental, cursor-based):
  - A created_at cursor persists in /app/data/face_backfill_cursor.txt (app_data
    volume), so progress survives restarts and the job walks the whole backlog
    forward without re-scanning.
  - Each run: fetch the next N reports with a foto_url created after the cursor,
    insert a photos row, and run the existing face pipeline (embed + match search).
  - InsightFace runs in a thread (face_pipeline), so the event loop / webhook
    stays responsive. Batch is kept small to avoid starving live photo handling.

Registered on APScheduler in main.py. Safe to run alongside live traffic.
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any

import httpx

from face_pipeline import process_photo_for_report

logger = logging.getLogger(__name__)

_CURSOR_PATH = "/app/data/face_backfill_cursor.txt"
_EPOCH = "1970-01-01T00:00:00+00:00"
_BATCH = 60  # reports per run; InsightFace CPU ~1s each → ~1 min/run

# Scrapers store the photo under different keys; some are relative paths that
# need a per-source base URL to be downloadable.
_PHOTO_KEYS = ("foto_url", "photoUrl", "photo_url", "foto", "image_url", "imageUrl")
_PHOTO_BASE = {
    "venezuela_te_busca": "https://venezuelatebusca.com",
}


def _read_cursor() -> str:
    try:
        with open(_CURSOR_PATH, "r", encoding="utf-8") as f:
            val = f.read().strip()
            return val or _EPOCH
    except FileNotFoundError:
        return _EPOCH
    except Exception as exc:
        logger.warning("face_backfill: cursor read failed: %s", exc)
        return _EPOCH


def _write_cursor(value: str) -> None:
    try:
        os.makedirs(os.path.dirname(_CURSOR_PATH), exist_ok=True)
        with open(_CURSOR_PATH, "w", encoding="utf-8") as f:
            f.write(value)
    except Exception as exc:
        logger.warning("face_backfill: cursor write failed: %s", exc)


def _foto_url(raw_data: Any, source: str) -> str | None:
    """Extract a downloadable photo URL from a report's raw_data, resolving
    relative paths against the source's base URL."""
    if not isinstance(raw_data, dict):
        return None
    for k in _PHOTO_KEYS:
        v = raw_data.get(k)
        if not v or not isinstance(v, str):
            continue
        v = v.strip()
        if v.startswith("http"):
            return v
        if v.startswith("/"):
            base = _PHOTO_BASE.get(source)
            if base:
                return base + v
    return None


async def run_face_backfill(app: Any) -> dict:
    sb = app.state.supabase_url.rstrip("/")
    key = app.state.supabase_service_key
    hdr = {"apikey": key, "Authorization": f"Bearer {key}"}
    cursor = _read_cursor()
    processed = embedded = errors = 0
    max_seen = cursor

    try:
        async with httpx.AsyncClient(timeout=20) as cl:
            r = await cl.get(
                f"{sb}/rest/v1/reports",
                headers=hdr,
                params={
                    "select": "id,kind,source,created_at,raw_data",
                    # any of the known photo keys present
                    "or": "(raw_data->>foto_url.not.is.null,raw_data->>photoUrl.not.is.null,"
                          "raw_data->>photo_url.not.is.null,raw_data->>imageUrl.not.is.null)",
                    "created_at": f"gt.{cursor}",
                    "order": "created_at.asc",
                    "limit": str(_BATCH),
                },
            )
            if r.status_code != 200:
                logger.warning("face_backfill: fetch %d: %s", r.status_code, r.text[:120])
                return {"processed": 0, "embedded": 0, "errors": 1}
            rows = r.json() or []
            if not rows:
                return {"processed": 0, "embedded": 0, "errors": 0, "note": "backlog drained"}

            # Which of these already have a photos row? (skip re-embedding)
            ids = ",".join(f'"{x["id"]}"' for x in rows)
            pr = await cl.get(
                f"{sb}/rest/v1/photos",
                headers=hdr,
                params={"select": "report_id", "report_id": f"in.({ids})"},
            )
            have_photo = {p["report_id"] for p in (pr.json() if pr.status_code == 200 else [])}

            for row in rows:
                max_seen = max(max_seen, row["created_at"])
                rid = row["id"]
                foto = _foto_url(row.get("raw_data"), row.get("source", ""))
                if not foto or rid in have_photo:
                    continue
                processed += 1
                try:
                    await cl.post(
                        f"{sb}/rest/v1/photos",
                        headers={**hdr, "Content-Type": "application/json",
                                 "Prefer": "resolution=ignore-duplicates,return=minimal"},
                        json={"id": str(uuid.uuid4()), "report_id": rid, "storage_path": foto},
                    )
                    # Embeds the face, stores it, and searches for matches.
                    await process_photo_for_report(rid, foto, app)
                    embedded += 1
                except Exception as exc:
                    errors += 1
                    logger.warning("face_backfill: report %s: %s", rid, exc)
    except Exception as exc:
        errors += 1
        logger.error("run_face_backfill error: %s", exc)

    if max_seen != cursor:
        _write_cursor(max_seen)
    result = {"processed": processed, "embedded": embedded, "errors": errors}
    logger.info("face_backfill: %s (cursor=%s)", result, max_seen[:19])
    return result
