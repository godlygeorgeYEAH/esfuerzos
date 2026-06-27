"""
consolidation_pipeline.py - Bulk data processing for Reune VE.

Three phases, all idempotent (safe to re-run):

  Phase 1 – Text embeddings:
    Computes 768-dim SentenceTransformer embeddings for every real-person
    report that lacks one. Skips noise rows (EVENTO: prefix from terremotove).

  Phase 2 – Text cross-match:
    For each `found` report with a text embedding, runs match_reports_by_text
    RPC (pgvector cosine) and inserts qualifying pairs into `matches`.

  Phase 3 – Face cross-match:
    For each photo with a face embedding whose parent report is `missing`,
    runs match_reports_by_face RPC and inserts qualifying pairs into `matches`.
    (Currently limited by the absence of `found` photos; will yield results
    once hospital/WAHA found-side photos arrive.)

All DB calls use the service role key. No row is ever deleted.
Matches use ON CONFLICT ignore to skip existing pairs.

Usage (from the running API container):
    POST /admin/consolidate          — full pipeline
    POST /admin/consolidate?phase=1  — text embeddings only
    POST /admin/consolidate?phase=2  — text cross-match only
    POST /admin/consolidate?phase=3  — face cross-match only
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

import httpx

from embeddings import build_text_for_embedding, get_text_embedding

logger = logging.getLogger(__name__)

# Thresholds
TEXT_MATCH_THRESHOLD: float = 0.75
FACE_MATCH_THRESHOLD: float = 0.50
COMBINED_THRESHOLD: float = 0.65

FACE_WEIGHT: float = 0.35
TEXT_WEIGHT: float = 0.65

BATCH_SIZE: int = 50
MATCH_COUNT: int = 10

# Sources that may contain noise rows (events, not individual persons)
_NOISE_NAME_PREFIX = "evento:"


def _sb_headers(key: str, prefer: str = "") -> dict:
    h = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


# ---------------------------------------------------------------------------
# Phase 1: Compute text embeddings
# ---------------------------------------------------------------------------

async def compute_text_embeddings(
    app: Any,
    batch_size: int = BATCH_SIZE,
) -> dict:
    """
    Batch-compute text embeddings for reports missing them.

    Processes `found` records first (fewer, critical for cross-matching),
    then `missing`. Excludes noise rows where full_name starts with 'EVENTO:'.

    Returns: {"processed": N, "skipped_noise": N, "errors": N}
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key
    text_model = app.state.text_model

    processed = 0
    skipped_noise = 0
    errors = 0

    # Process `found` first (hospital/terremotove records), then `missing`.
    # `found` side is small (~189 real records) and needed first for matching.
    for kind_filter in ["found", "missing"]:
        offset = 0
        logger.info("Phase 1: processing kind=%s", kind_filter)

        while True:
            async with httpx.AsyncClient(timeout=20) as cl:
                r = await cl.get(
                    f"{sb_url}/rest/v1/reports",
                    headers=_sb_headers(sb_key, "count=exact"),
                    params={
                        "select": "id,full_name,age,last_seen_location,distinguishing_marks,clothing",
                        "text_embedding": "is.null",
                        "kind": f"eq.{kind_filter}",
                        "limit": str(batch_size),
                        "offset": str(offset),
                        "order": "created_at.asc",
                    },
                )
            if r.status_code not in (200, 206):
                logger.error("Phase 1: fetch batch failed %d: %s", r.status_code, r.text[:120])
                break

            rows = r.json()
            if not rows:
                break

            for row in rows:
                name = (row.get("full_name") or "").strip()
                if name.lower().startswith(_NOISE_NAME_PREFIX):
                    skipped_noise += 1
                    continue

                text = build_text_for_embedding(row)
                if not text.strip():
                    skipped_noise += 1
                    continue

                try:
                    emb = await get_text_embedding(text, text_model)
                    async with httpx.AsyncClient(timeout=15) as cl:
                        patch = await cl.patch(
                            f"{sb_url}/rest/v1/reports",
                            headers=_sb_headers(sb_key, "return=minimal"),
                            params={"id": f"eq.{row['id']}"},
                            json={"text_embedding": emb},
                        )
                    if patch.status_code in (200, 204):
                        processed += 1
                    else:
                        logger.warning(
                            "Phase 1: PATCH failed for %s: %d %s",
                            row["id"], patch.status_code, patch.text[:80],
                        )
                        errors += 1
                except Exception as exc:
                    logger.error("Phase 1: embedding error for %s: %s", row["id"], exc)
                    errors += 1

            logger.info(
                "Phase 1 kind=%s: offset=%d processed=%d noise=%d errors=%d",
                kind_filter, offset, processed, skipped_noise, errors,
            )
            offset += batch_size
            await asyncio.sleep(0)

    return {"processed": processed, "skipped_noise": skipped_noise, "errors": errors}


# ---------------------------------------------------------------------------
# Phase 2: Text cross-match (found -> missing)
# ---------------------------------------------------------------------------

async def run_text_cross_match(
    app: Any,
    batch_size: int = BATCH_SIZE,
) -> dict:
    """
    For every `found` report with a text embedding, search for similar
    `missing` reports via the match_reports_by_text RPC and insert qualifying
    pairs into `matches`.

    Pairs with combined_score >= COMBINED_THRESHOLD are inserted as
    status='pending' (human review required).

    Also runs the inverse pass: missing -> found, to catch any pairs
    missed by the found->missing direction.

    Returns: {"pairs_checked": N, "matches_inserted": N, "errors": N}
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key

    pairs_checked = 0
    matches_inserted = 0
    errors = 0

    for query_kind, target_kind in [("found", "missing"), ("missing", "found")]:
        logger.info("Phase 2: text cross-match %s -> %s", query_kind, target_kind)
        offset = 0

        while True:
            async with httpx.AsyncClient(timeout=20) as cl:
                r = await cl.get(
                    f"{sb_url}/rest/v1/reports",
                    headers=_sb_headers(sb_key),
                    params={
                        "select": "id,kind,text_embedding",
                        "kind": f"eq.{query_kind}",
                        "text_embedding": "not.is.null",
                        "limit": str(batch_size),
                        "offset": str(offset),
                        "order": "created_at.asc",
                    },
                )
            if r.status_code not in (200, 206):
                logger.error("Phase 2 fetch failed: %d %s", r.status_code, r.text[:80])
                break

            source_rows = r.json()
            if not source_rows:
                break

            for src in source_rows:
                pairs_checked += 1
                try:
                    ins = await _text_match_one(
                        src["id"], src["text_embedding"],
                        target_kind, sb_url, sb_key,
                    )
                    matches_inserted += ins
                except Exception as exc:
                    logger.error("Phase 2 error for report %s: %s", src["id"], exc)
                    errors += 1

            logger.info(
                "Phase 2 (%s->%s): offset=%d checked=%d inserted=%d",
                query_kind, target_kind, offset, pairs_checked, matches_inserted,
            )
            offset += batch_size
            await asyncio.sleep(0)

    return {
        "pairs_checked": pairs_checked,
        "matches_inserted": matches_inserted,
        "errors": errors,
    }


async def _text_match_one(
    source_id: str,
    text_embedding: list,
    target_kind: str,
    sb_url: str,
    sb_key: str,
) -> int:
    """Call match_reports_by_text and insert qualifying matches. Returns count inserted."""
    async with httpx.AsyncClient(timeout=20) as cl:
        r = await cl.post(
            f"{sb_url}/rest/v1/rpc/match_reports_by_text",
            headers=_sb_headers(sb_key),
            json={
                "query_embedding": text_embedding,
                "query_kind": target_kind,
                "match_threshold": TEXT_MATCH_THRESHOLD,
                "match_count": MATCH_COUNT,
            },
        )
    if r.status_code != 200:
        logger.warning(
            "_text_match_one RPC failed %d: %s", r.status_code, r.text[:120]
        )
        return 0

    candidates = r.json() or []
    inserted = 0

    for cand in candidates:
        cand_id = cand.get("id") or cand.get("report_id")
        if not cand_id or cand_id == source_id:
            continue

        text_score = float(cand.get("similarity", 0.0))
        if text_score < TEXT_MATCH_THRESHOLD:
            continue

        combined_score = TEXT_WEIGHT * text_score
        if combined_score < COMBINED_THRESHOLD:
            continue

        # Normalise to missing_id / found_id
        if target_kind == "missing":
            missing_id, found_id = cand_id, source_id
        else:
            missing_id, found_id = source_id, cand_id

        row = {
            "id": str(uuid.uuid4()),
            "missing_id": missing_id,
            "found_id": found_id,
            "text_score": round(text_score, 4),
            "face_score": 0.0,
            "combined_score": round(combined_score, 4),
            "status": "pending",
        }

        async with httpx.AsyncClient(timeout=10) as cl:
            resp = await cl.post(
                f"{sb_url}/rest/v1/matches",
                headers=_sb_headers(sb_key, "resolution=ignore-duplicates,return=minimal"),
                json=row,
            )
        if resp.status_code in (200, 201):
            inserted += 1
            logger.info(
                "Text match: missing=%s found=%s text=%.3f combined=%.3f",
                missing_id, found_id, text_score, combined_score,
            )
        elif resp.status_code == 409:
            pass  # already exists
        else:
            logger.warning("match insert failed %d: %s", resp.status_code, resp.text[:80])

    return inserted


# ---------------------------------------------------------------------------
# Phase 3: Face cross-match (missing photos -> found reports)
# ---------------------------------------------------------------------------

async def run_face_cross_match(
    app: Any,
    batch_size: int = BATCH_SIZE,
) -> dict:
    """
    For every `missing` photo with a face embedding, search for similar
    `found` reports (with photos) via match_reports_by_face RPC.

    Currently limited by lack of `found` photos in the dataset;
    will produce results once hospital/WAHA found-side photos arrive.

    Returns: {"photos_checked": N, "matches_inserted": N, "errors": N}
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key

    photos_checked = 0
    matches_inserted = 0
    errors = 0
    offset = 0

    logger.info("Phase 3: face cross-match (missing photos -> found)")

    while True:
        async with httpx.AsyncClient(timeout=20) as cl:
            # Get photos of missing persons with embeddings
            r = await cl.get(
                f"{sb_url}/rest/v1/photos",
                headers=_sb_headers(sb_key),
                params={
                    "select": "report_id,face_embedding",
                    "face_embedding": "not.is.null",
                    "quality_ok": "eq.true",
                    "limit": str(batch_size),
                    "offset": str(offset),
                    "order": "created_at.asc",
                },
            )
        if r.status_code not in (200, 206):
            logger.error("Phase 3 fetch failed: %d", r.status_code)
            break

        photo_rows = r.json()
        if not photo_rows:
            break

        # Filter to missing-kind reports only
        async with httpx.AsyncClient(timeout=20) as cl:
            ids = [p["report_id"] for p in photo_rows]
            id_filter = f"in.({','.join(ids)})"
            r2 = await cl.get(
                f"{sb_url}/rest/v1/reports",
                headers=_sb_headers(sb_key),
                params={"select": "id,kind", "id": id_filter, "limit": str(batch_size)},
            )
        kind_map = {row["id"]: row["kind"] for row in (r2.json() if r2.status_code in (200,206) else [])}

        for photo in photo_rows:
            report_id = photo["report_id"]
            if kind_map.get(report_id) != "missing":
                continue

            photos_checked += 1
            try:
                ins = await _face_match_one(
                    report_id, photo["face_embedding"],
                    "found", sb_url, sb_key,
                )
                matches_inserted += ins
            except Exception as exc:
                logger.error("Phase 3 error for report %s: %s", report_id, exc)
                errors += 1

        logger.info(
            "Phase 3: offset=%d checked=%d inserted=%d errors=%d",
            offset, photos_checked, matches_inserted, errors,
        )
        offset += batch_size
        await asyncio.sleep(0)

    return {
        "photos_checked": photos_checked,
        "matches_inserted": matches_inserted,
        "errors": errors,
    }


async def _face_match_one(
    source_report_id: str,
    face_embedding: list,
    target_kind: str,
    sb_url: str,
    sb_key: str,
) -> int:
    """Call match_reports_by_face and insert qualifying matches."""
    async with httpx.AsyncClient(timeout=20) as cl:
        r = await cl.post(
            f"{sb_url}/rest/v1/rpc/match_reports_by_face",
            headers=_sb_headers(sb_key),
            json={
                "query_embedding": face_embedding,
                "query_kind": target_kind,
                "match_threshold": FACE_MATCH_THRESHOLD,
                "match_count": MATCH_COUNT,
            },
        )
    if r.status_code != 200:
        logger.warning("_face_match_one RPC %d: %s", r.status_code, r.text[:120])
        return 0

    candidates = r.json() or []
    inserted = 0

    for cand in candidates:
        cand_report_id = cand.get("report_id") or cand.get("id")
        if not cand_report_id or cand_report_id == source_report_id:
            continue

        face_score = float(cand.get("similarity", 0.0))
        if face_score < FACE_MATCH_THRESHOLD:
            continue

        combined_score = FACE_WEIGHT * face_score
        if combined_score < COMBINED_THRESHOLD:
            continue

        if target_kind == "found":
            missing_id, found_id = source_report_id, cand_report_id
        else:
            missing_id, found_id = cand_report_id, source_report_id

        row = {
            "id": str(uuid.uuid4()),
            "missing_id": missing_id,
            "found_id": found_id,
            "face_score": round(face_score, 4),
            "text_score": 0.0,
            "combined_score": round(combined_score, 4),
            "status": "pending",
        }

        async with httpx.AsyncClient(timeout=10) as cl:
            resp = await cl.post(
                f"{sb_url}/rest/v1/matches",
                headers=_sb_headers(sb_key, "resolution=ignore-duplicates,return=minimal"),
                json=row,
            )
        if resp.status_code in (200, 201):
            inserted += 1
        elif resp.status_code != 409:
            logger.warning("face match insert %d: %s", resp.status_code, resp.text[:80])

    return inserted


# ---------------------------------------------------------------------------
# Auto-embed helper: call on every new report
# ---------------------------------------------------------------------------

async def embed_and_match_report(
    report_id: str,
    report_data: dict,
    app: Any,
) -> None:
    """
    Compute and store text_embedding for a freshly created/updated report,
    then run text cross-match against opposite-kind reports.

    Call this from scrapers and WAHA intake after upsert, passing the
    dict of report fields (must include at minimum full_name, age,
    last_seen_location, distinguishing_marks, clothing).

    Never raises; logs errors internally.
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key
    text_model = getattr(app.state, "text_model", None)
    if text_model is None:
        return

    name = (report_data.get("full_name") or "").strip()
    if name.lower().startswith(_NOISE_NAME_PREFIX):
        return

    text = build_text_for_embedding(report_data)
    if not text.strip():
        return

    try:
        emb = await get_text_embedding(text, text_model)
        async with httpx.AsyncClient(timeout=10) as cl:
            await cl.patch(
                f"{sb_url}/rest/v1/reports",
                headers=_sb_headers(sb_key, "return=minimal"),
                params={"id": f"eq.{report_id}"},
                json={"text_embedding": emb},
            )

        # Determine opposite kind for matching
        kind = report_data.get("kind", "missing")
        target_kind = "found" if kind == "missing" else "missing"
        await _text_match_one(report_id, emb, target_kind, sb_url, sb_key)

    except Exception as exc:
        logger.error("embed_and_match_report %s: %s", report_id, exc)


# ---------------------------------------------------------------------------
# Full pipeline entry point
# ---------------------------------------------------------------------------

async def run_full_consolidation(app: Any) -> dict:
    """Run all three phases in sequence. Idempotent."""
    logger.info("Starting full data consolidation pipeline")

    p1 = await compute_text_embeddings(app)
    logger.info("Phase 1 done: %s", p1)

    p2 = await run_text_cross_match(app)
    logger.info("Phase 2 done: %s", p2)

    p3 = await run_face_cross_match(app)
    logger.info("Phase 3 done: %s", p3)

    return {"phase1": p1, "phase2": p2, "phase3": p3}
