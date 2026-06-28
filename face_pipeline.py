"""
face_pipeline.py - InsightFace-based face embedding pipeline for Reune VE.

Downloads photos from storage_path URLs, runs InsightFace buffalo_sc to extract
512-dim face embeddings, persists them to Supabase, and triggers face-based
1:N matching against reports of the opposite kind.

Thresholds (separate from match_engine.py CompreFace thresholds):
  FACE_MATCH_THRESHOLD  0.50  minimum cosine similarity accepted from RPC
  FACE_WEIGHT           0.35  face contribution to combined score
  TEXT_WEIGHT           0.65  text contribution to combined score (future)
  COMBINED_THRESHOLD    0.65  minimum combined score to write a match row
  FACE_MATCH_COUNT      10    candidates fetched per RPC call

Supabase access: all DB calls use the service role key from app.state.
    app.state.supabase_url          - Supabase project URL
    app.state.supabase_service_key  - service role key (bypasses RLS)
    app.state.face_model            - InsightFace FaceAnalysis instance
    app.state.text_model            - SentenceTransformer instance

InsightFace model expected in app.state:
    FaceAnalysis("buffalo_sc", providers=["CPUExecutionProvider"])
    model.prepare(ctx_id=-1)  (CPU)
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import urllib.parse
import uuid
from typing import Any

import cv2
import httpx
import numpy as np

from config import get_settings
from embeddings import build_text_for_embedding, get_text_embedding

logger = logging.getLogger(__name__)
settings = get_settings()

# ---------------------------------------------------------------------------
# Thresholds and constants
# ---------------------------------------------------------------------------

FACE_MATCH_THRESHOLD: float = 0.50
FACE_WEIGHT: float = 0.35
TEXT_WEIGHT: float = 0.65
COMBINED_THRESHOLD: float = 0.65
FACE_MATCH_COUNT: int = 10

# ---------------------------------------------------------------------------
# Minimal Supabase REST helpers
# (mirrors match_engine._sb_* to keep this module self-contained)
# ---------------------------------------------------------------------------


def _sb_headers(key: str, prefer: str = "return=representation") -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


async def _sb_get(
    client: httpx.AsyncClient,
    url: str,
    key: str,
    params: dict,
) -> list[dict]:
    resp = await client.get(url, headers=_sb_headers(key), params=params)
    resp.raise_for_status()
    return resp.json() or []


async def _sb_patch(
    client: httpx.AsyncClient,
    url: str,
    key: str,
    params: dict,
    body: dict,
) -> None:
    resp = await client.patch(
        url,
        headers=_sb_headers(key, "return=minimal"),
        params=params,
        json=body,
    )
    resp.raise_for_status()


async def _sb_rpc(
    client: httpx.AsyncClient,
    sb_url: str,
    key: str,
    fn: str,
    body: dict,
) -> list[dict]:
    resp = await client.post(
        f"{sb_url}/rest/v1/rpc/{fn}",
        headers=_sb_headers(key),
        json=body,
    )
    resp.raise_for_status()
    return resp.json() or []


async def _sb_insert(
    client: httpx.AsyncClient,
    sb_url: str,
    key: str,
    table: str,
    row: dict,
) -> None:
    resp = await client.post(
        f"{sb_url}/rest/v1/{table}",
        headers=_sb_headers(key, "return=minimal"),
        json=row,
    )
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# Public pipeline functions
# ---------------------------------------------------------------------------


async def embed_photo_from_url(
    photo_url: str,
    face_model: Any,
) -> dict | None:
    """
    Download photo_url, decode with OpenCV, run InsightFace, return best face.

    Returns a dict with:
        embedding  list[float]  512-dim L2-normalised face vector
        det_score  float        InsightFace detection confidence
        bbox       list         [x1, y1, x2, y2] bounding box

    Returns None if the download fails, the image cannot be decoded,
    or InsightFace finds no face in the image.
    """
    # SSRF guard: enforce HTTPS + block private/loopback IPs for EXTERNAL hosts.
    # The internal WAHA media host is explicitly trusted — it serves WhatsApp
    # photos over plain HTTP on the docker network (e.g. http://waha:3000/...).
    # Without this allowance, every WhatsApp photo is rejected (non-HTTPS) and
    # the face pipeline never runs on the bot's primary input channel.
    is_trusted_internal = False
    try:
        parsed = urllib.parse.urlparse(photo_url)
        host = (parsed.hostname or "").lower()
        waha_host = (urllib.parse.urlparse(settings.waha_url).hostname or "").lower()
        is_trusted_internal = bool(host) and host == waha_host

        if not is_trusted_internal:
            if parsed.scheme != "https":
                logger.warning("embed_photo_from_url: rejected non-HTTPS URL %s", photo_url)
                return None
            try:
                addr = ipaddress.ip_address(host)
                if addr.is_private or addr.is_loopback or addr.is_link_local:
                    logger.warning("embed_photo_from_url: rejected private IP %s", photo_url)
                    return None
            except ValueError:
                pass  # hostname is a domain, not an IP -- allow it
    except Exception as exc:
        logger.warning("embed_photo_from_url: URL parse error for %s: %s", photo_url, exc)
        return None

    # WAHA file endpoints require the API key when WAHA_API_KEY is set.
    download_headers: dict = {}
    if is_trusted_internal and settings.waha_api_key:
        download_headers["X-Api-Key"] = settings.waha_api_key

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(photo_url, headers=download_headers)
            resp.raise_for_status()
            image_bytes = resp.content
    except Exception as exc:
        logger.warning("embed_photo_from_url: download failed for %s: %s", photo_url, exc)
        return None

    try:
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception as exc:
        logger.warning("embed_photo_from_url: decode error for %s: %s", photo_url, exc)
        return None

    if img is None:
        logger.warning("embed_photo_from_url: cv2 returned None for %s", photo_url)
        return None

    # C5: asyncio.to_thread for blocking ML call
    try:
        faces = await asyncio.to_thread(face_model.get, img)
    except Exception as exc:
        logger.warning("embed_photo_from_url: face_model.get raised %s for %s", exc, photo_url)
        return None

    if not faces:
        logger.info("embed_photo_from_url: no face detected in %s", photo_url)
        return None

    best = max(faces, key=lambda f: float(f.det_score))

    if best.embedding is None:
        logger.warning(
            "embed_photo_from_url: face detected but embedding is None for %s", photo_url
        )
        return None

    return {
        "embedding": best.embedding.tolist(),
        "det_score": float(best.det_score),
        "bbox": best.bbox.tolist(),
    }


async def process_photo_for_report(
    report_id: str,
    photo_url: str,
    app: Any,
) -> str | None:
    """
    Extract a face embedding from photo_url and persist it in Supabase.

    Steps:
    1. Run embed_photo_from_url.
    2. If no face: PATCH photos row with quality_ok=False, return None.
    3. If face found: PATCH photos row with face_embedding, det_score,
       face_bbox, quality_ok=True.
    4. Trigger _search_face_matches for the opposite report kind.

    Returns the first match_id inserted, or None if no match was found.
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key

    result = await embed_photo_from_url(photo_url, app.state.face_model)

    async with httpx.AsyncClient(timeout=30.0) as client:
        photo_filter = {
            "report_id": f"eq.{report_id}",
            "storage_path": f"eq.{photo_url}",
        }

        if result is None:
            try:
                await _sb_patch(
                    client,
                    f"{sb_url}/rest/v1/photos",
                    sb_key,
                    photo_filter,
                    {"quality_ok": False},
                )
            except Exception as exc:
                logger.warning(
                    "process_photo_for_report: could not set quality_ok=False "
                    "for report %s photo %s: %s",
                    report_id, photo_url, exc,
                )
            return None

        # Fetch report kind to determine opposite kind for matching.
        try:
            rows = await _sb_get(
                client,
                f"{sb_url}/rest/v1/reports",
                sb_key,
                {"id": f"eq.{report_id}", "select": "id,kind"},
            )
        except Exception as exc:
            logger.error(
                "process_photo_for_report: could not fetch report %s: %s", report_id, exc
            )
            return None

        if not rows:
            logger.error("process_photo_for_report: report %s not found", report_id)
            return None

        source_kind: str = rows[0].get("kind", "") or "missing"

        # Persist embedding.
        try:
            await _sb_patch(
                client,
                f"{sb_url}/rest/v1/photos",
                sb_key,
                photo_filter,
                {
                    "face_embedding": result["embedding"],
                    "det_score": result["det_score"],
                    "face_bbox": result["bbox"],
                    "quality_ok": True,
                },
            )
            logger.info(
                "Face embedding stored for report %s photo %s (det_score=%.3f)",
                report_id, photo_url, result["det_score"],
            )
        except Exception as exc:
            logger.error(
                "process_photo_for_report: PATCH failed for report %s photo %s: %s",
                report_id, photo_url, exc,
            )
            return None

    # Search BOTH kinds: the same face is worth surfacing whether the person is
    # listed as missing (another searcher) or found (located). A missing↔missing
    # hit connects two families looking for the same person.
    first_match: str | None = None
    for target_kind in ("missing", "found"):
        mid = await _search_face_matches(
            report_id, result["embedding"], target_kind, source_kind, sb_url, sb_key
        )
        if mid and not first_match:
            first_match = mid
    return first_match


async def _search_face_matches(
    source_report_id: str,
    face_embedding: list,
    target_kind: str,
    source_kind: str,
    sb_url: str,
    sb_key: str,
) -> str | None:
    """
    Call the match_reports_by_face Supabase RPC, score each candidate,
    and insert qualifying rows into the matches table.

    Combined score formula (text signal not yet available at photo-ingest time):
        combined_score = FACE_WEIGHT * face_score

    A match row is inserted only when combined_score >= COMBINED_THRESHOLD.
    All inserted rows get status='pending' (human review required).

    Returns the id of the first match row inserted, or None.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            candidates = await _sb_rpc(
                client,
                sb_url,
                sb_key,
                "match_reports_by_face",
                {
                    "query_embedding": face_embedding,
                    "query_kind": target_kind,
                    "match_threshold": FACE_MATCH_THRESHOLD,
                    "match_count": FACE_MATCH_COUNT,
                },
            )
        except Exception as exc:
            logger.error(
                "_search_face_matches: RPC failed for report %s: %s",
                source_report_id, exc,
            )
            return None

        first_match_id: str | None = None

        for candidate in candidates:
            candidate_report_id: str = candidate.get("report_id", "")
            if not candidate_report_id or candidate_report_id == source_report_id:
                continue

            face_score = float(candidate.get("similarity", 0.0))
            # At photo-ingest time there is no text signal yet.
            # Gate on face similarity alone; combined_score = face_score.
            if face_score < FACE_MATCH_THRESHOLD:
                continue
            combined_score = face_score

            # Candidate is target_kind, source is source_kind. Put the missing-kind
            # report in missing_id and the found-kind in found_id; if both are the
            # same kind, label loosely (source first) — the columns are just a pair.
            if target_kind == "missing" and source_kind == "found":
                missing_id, found_id = candidate_report_id, source_report_id
            elif target_kind == "found" and source_kind == "missing":
                missing_id, found_id = source_report_id, candidate_report_id
            else:
                missing_id, found_id = source_report_id, candidate_report_id

            match_id = str(uuid.uuid4())
            row: dict = {
                "id": match_id,
                "missing_id": missing_id,
                "found_id": found_id,
                "face_score": round(face_score, 4),
                "text_score": 0.0,
                "combined_score": round(combined_score, 4),
                "status": "pending",
            }

            try:
                await _sb_insert(client, sb_url, sb_key, "matches", row)
                logger.info(
                    "Match inserted: id=%s missing=%s found=%s face=%.3f combined=%.3f",
                    match_id, missing_id, found_id, face_score, combined_score,
                )
                if first_match_id is None:
                    first_match_id = match_id
            except Exception as exc:
                logger.warning(
                    "_search_face_matches: insert failed for match candidate %s "
                    "(source=%s): %s",
                    candidate_report_id, source_report_id, exc,
                )

        return first_match_id
