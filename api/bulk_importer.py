"""
bulk_importer.py - One-time batch import of pre-existing crisis data.

Sources:
  /crisis_data/sos_personas.db  - 43k persons from SOS Venezuela with local photos
  /crisis_data/crisis_response.db - 52k social media crisis posts

Run via API endpoint:
  POST /admin/bulk_import?source=sos_persons&limit=500&offset=0
  POST /admin/bulk_import?source=crisis_posts&limit=500&offset=0

Or directly:
  docker exec reune-api python -c "
    import asyncio; from bulk_importer import run_full_import
    asyncio.run(run_full_import())
  "

Photos served at:
  http://13.140.166.72:8080/crisis_images/{uuid}/{uuid}.webp
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Paths inside container (mounted from VPS /root/crisis)
SOS_DB = "/crisis_data/sos_personas.db"
CRISIS_DB = "/crisis_data/crisis_response.db"
# Photos mounted from /root/crisis/sos_images -> /root/sos_images inside container
SOS_IMAGES_DIR = "/root/sos_images"

VPS_PUBLIC = os.environ.get("VPS_PUBLIC_URL", "http://13.140.166.72:8080")
SOS_IMAGES_URL_BASE = f"{VPS_PUBLIC}/sos_images"

BATCH_SIZE = 50


def _sb_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


async def _upsert_report(client: httpx.AsyncClient, sb_url: str, sb_key: str, data: dict) -> str | None:
    resp = await client.post(
        f"{sb_url}/rest/v1/reports",
        headers={**_sb_headers(sb_key), "Prefer": "resolution=merge-duplicates,return=representation"},
        json=data,
    )
    if resp.status_code not in (200, 201):
        logger.debug("upsert_report %d: %s", resp.status_code, resp.text[:100])
        return None
    rows = resp.json()
    return rows[0]["id"] if rows else None


async def _upsert_photo(client: httpx.AsyncClient, sb_url: str, sb_key: str, report_id: str, photo_url: str) -> None:
    await client.post(
        f"{sb_url}/rest/v1/photos",
        headers={**_sb_headers(sb_key), "Prefer": "resolution=ignore-duplicates,return=minimal"},
        json={"id": str(uuid.uuid4()), "report_id": report_id, "storage_path": photo_url},
    )


async def import_sos_persons(
    app: Any,
    limit: int = 500,
    offset: int = 0,
    process_faces: bool = True,
) -> dict:
    """Import a batch of sos_persons into Supabase + run face pipeline."""
    if not Path(SOS_DB).exists():
        return {"error": f"DB not found: {SOS_DB}"}

    conn = sqlite3.connect(SOS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM sos_persons ORDER BY fecha_scraped DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()

    sb_url = app.state.supabase_url.rstrip("/")
    sb_key = app.state.supabase_service_key

    inserted = 0
    face_processed = 0
    face_matched = 0
    errors = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for row in rows:
            row_id = row["id"]
            source_url = f"sos_venezuela:{row_id}"
            photo_path = Path(SOS_IMAGES_DIR) / row_id / f"{row_id}.webp"
            photo_url = f"{SOS_IMAGES_URL_BASE}/{row_id}/{row_id}.webp" if photo_path.exists() else None

            # Map status to kind
            status = (row["status"] or "").lower()
            kind = "found" if "encontrado" in status or status == "found" else "missing"

            report_data = {
                "kind": kind,
                "full_name": row["display_name"] or "Desconocido",
                "last_seen_location": ", ".join(filter(None, [row["municipio"], row["parroquia"]])) or None,
                "source": "sos_venezuela",
                "source_url": source_url,
                "reporter_phone": None,
            }

            try:
                report_id = await _upsert_report(client, sb_url, sb_key, report_data)
                if not report_id:
                    errors += 1
                    continue
                inserted += 1

                if photo_url:
                    await _upsert_photo(client, sb_url, sb_key, report_id, photo_url)

            except Exception as exc:
                logger.error("import_sos_persons row %s: %s", row_id, exc)
                errors += 1

    # Face processing in a second pass (outside the httpx client context)
    if process_faces and hasattr(app.state, "face_model"):
        from face_pipeline import process_photo_for_report

        # Re-fetch report IDs that have unprocessed photos
        async with httpx.AsyncClient(timeout=30) as client:
            # Get all photo URLs we just inserted that don't have embeddings
            r = await client.get(
                f"{sb_url}/rest/v1/photos",
                headers=_sb_headers(sb_key),
                params={
                    "quality_ok": "not.eq.true",
                    "storage_path": f"like.{SOS_IMAGES_URL_BASE}%",
                    "select": "report_id,storage_path",
                    "limit": str(limit),
                    "offset": str(offset),
                },
            )
            photos = r.json() if r.status_code == 200 else []

        for p in photos:
            try:
                match_id = await process_photo_for_report(p["report_id"], p["storage_path"], app)
                face_processed += 1
                if match_id:
                    face_matched += 1
            except Exception as exc:
                logger.error("face processing %s: %s", p["storage_path"], exc)

    return {
        "source": "sos_persons",
        "batch": {"limit": limit, "offset": offset},
        "inserted": inserted,
        "face_processed": face_processed,
        "face_matched": face_matched,
        "errors": errors,
        "total_in_batch": len(rows),
    }


async def import_crisis_posts(
    app: Any,
    limit: int = 500,
    offset: int = 0,
) -> dict:
    """Import crisis_posts (social media) as text-only reports for text matching."""
    if not Path(CRISIS_DB).exists():
        return {"error": f"DB not found: {CRISIS_DB}"}

    conn = sqlite3.connect(CRISIS_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM crisis_posts ORDER BY fecha_publicacion DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()

    sb_url = app.state.supabase_url.rstrip("/")
    sb_key = app.state.supabase_service_key

    inserted = 0
    errors = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for row in rows:
            post_id = row["post_id"]
            texto = row["texto"] or ""
            tipo = (row["tipo"] or "").lower()

            # Skip posts with no relevant content
            if len(texto.strip()) < 10:
                continue

            # Infer kind from tipo field
            if "busco" in tipo or "missing" in tipo or "desaparecido" in tipo:
                kind = "missing"
            elif "encontr" in tipo or "found" in tipo or "hospital" in tipo:
                kind = "found"
            else:
                kind = "missing"  # default for unclassified crisis posts

            report_data = {
                "kind": kind,
                "full_name": f"Post: {(row['autor_nombre'] or 'Desconocido')[:100]}",
                "last_seen_location": row["ubicacion"] or None,
                "distinguishing_marks": texto[:500],
                "source": "crisis_posts",
                "source_url": f"crisis_post:{post_id}",
                "reporter_phone": None,
            }

            try:
                report_id = await _upsert_report(client, sb_url, sb_key, report_data)
                if report_id:
                    inserted += 1
                else:
                    errors += 1
            except Exception as exc:
                logger.error("import_crisis_posts row %s: %s", post_id, exc)
                errors += 1

    return {
        "source": "crisis_posts",
        "batch": {"limit": limit, "offset": offset},
        "inserted": inserted,
        "errors": errors,
        "total_in_batch": len(rows),
    }


async def run_full_import(app: Any) -> dict:
    """Run the complete batch import: sos_persons then crisis_posts."""
    results = {"sos_persons": [], "crisis_posts": []}

    # Get total counts
    sos_count = 0
    crisis_count = 0
    if Path(SOS_DB).exists():
        conn = sqlite3.connect(SOS_DB)
        sos_count = conn.execute("SELECT COUNT(*) FROM sos_persons").fetchone()[0]
        conn.close()
    if Path(CRISIS_DB).exists():
        try:
            conn = sqlite3.connect(CRISIS_DB, timeout=5)
            crisis_count = conn.execute("SELECT COUNT(*) FROM crisis_posts").fetchone()[0]
            conn.close()
        except Exception as exc:
            logger.warning("crisis_response.db not readable: %s — skipping crisis_posts import", exc)

    logger.info("Bulk import: %d sos_persons, %d crisis_posts", sos_count, crisis_count)

    # Import SOS persons in batches
    for offset in range(0, sos_count, BATCH_SIZE):
        r = await import_sos_persons(app, limit=BATCH_SIZE, offset=offset, process_faces=True)
        results["sos_persons"].append(r)
        logger.info("sos_persons batch offset=%d: %s", offset, r)
        await asyncio.sleep(0.1)  # yield to event loop

    # Import crisis posts in batches (no face processing)
    for offset in range(0, crisis_count, BATCH_SIZE):
        r = await import_crisis_posts(app, limit=BATCH_SIZE, offset=offset)
        results["crisis_posts"].append(r)
        logger.info("crisis_posts batch offset=%d: %s", offset, r)
        await asyncio.sleep(0.1)

    total_sos = sum(b.get("inserted", 0) for b in results["sos_persons"])
    total_crisis = sum(b.get("inserted", 0) for b in results["crisis_posts"])
    total_faces = sum(b.get("face_processed", 0) for b in results["sos_persons"])
    total_matches = sum(b.get("face_matched", 0) for b in results["sos_persons"])

    return {
        "sos_persons_imported": total_sos,
        "crisis_posts_imported": total_crisis,
        "faces_processed": total_faces,
        "face_matches_found": total_matches,
    }
