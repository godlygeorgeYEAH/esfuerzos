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

# 2026-07-01: several aggregators (venezreporta, venezuela_te_busca, reconexion)
# re-host each other's photos. A face match between a re-hosted copy of the SAME
# picture and its origin scores near-1.0 but is NOT independent corroboration —
# it is one photo counted twice. _PHASH_NEAR_DUP bits (of 64) is the Hamming
# distance below which two photos are treated as the same underlying image.
_PHASH_NEAR_DUP: int = 8

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


def _dhash(img: np.ndarray, hash_size: int = 8) -> str | None:
    """Cheap perceptual hash (difference hash) so a re-hosted copy of the same
    photo (recompressed/resized by a different scraper) can be recognized even
    though its bytes differ. cv2/numpy only — no new dependency."""
    try:
        small = cv2.resize(img, (hash_size + 1, hash_size), interpolation=cv2.INTER_AREA)
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY) if small.ndim == 3 else small
        bits = (gray[:, 1:] > gray[:, :-1]).flatten()
        val = 0
        for b in bits:
            val = (val << 1) | int(b)
        return format(val, "016x")
    except Exception as exc:  # noqa: BLE001
        logger.warning("_dhash failed: %s", exc)
        return None


def _hamming_hex(h1: str | None, h2: str | None) -> int:
    if not h1 or not h2:
        return 999
    try:
        return bin(int(h1, 16) ^ int(h2, 16)).count("1")
    except ValueError:
        return 999


# venezuelatebusca's own re-host folder names don't match our internal scraper
# source keys 1:1 (e.g. our 'venezreporta' vs their '/venezuela_reporta/').
# Verified 2026-07-01 by inspecting real re-hosted URLs.
_SOURCE_PATH_ALIASES: dict[str, tuple[str, ...]] = {
    "venezreporta": ("venezreporta", "venezuela_reporta"),
}


def _source_path_tokens(source: str | None) -> tuple[str, ...]:
    if not source:
        return ()
    return _SOURCE_PATH_ALIASES.get(source, (source,))


def _same_photo_suspected(
    a_source_url: str | None, a_photo: str | None, a_source: str | None,
    b_source_url: str | None, b_photo: str | None, b_source: str | None,
) -> bool:
    """Fallback for photos ingested before phash existed (can't re-hash without
    re-downloading every old image): catches known cross-aggregator re-hosting
    patterns, verified 2026-07-01 at 100% precision on a sample —
    venezreporta filenames embed the reconexion person id; venezuelatebusca
    paths tag the upstream scraper folder (e.g. '/venezuela_reporta/')."""
    if not a_photo or not b_photo:
        return False
    for src_url, other_photo in ((a_source_url, b_photo), (b_source_url, a_photo)):
        if src_url and ":" in src_url:
            token = src_url.split(":", 1)[1]
            if len(token) >= 8 and token in other_photo:
                return True
    for src_name, other_photo in ((a_source, b_photo), (b_source, a_photo)):
        for tok in _source_path_tokens(src_name):
            if f"/{tok}/" in other_photo:
                return True
    return False


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

    return await embed_photo_from_bytes(image_bytes, face_model)


async def embed_photo_from_bytes(image_bytes: bytes, face_model: Any) -> dict | None:
    """Decode image bytes, run InsightFace, return the best face (embedding/det_score/
    bbox) or None. The bytes-in counterpart of embed_photo_from_url, used by channels
    (e.g. Telegram) that download the photo themselves — keeps the image in memory so
    no token-bearing URL is persisted and no face lands in a public bucket."""
    if not image_bytes:
        return None
    try:
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    except Exception as exc:
        logger.warning("embed_photo_from_bytes: decode error: %s", exc)
        return None

    if img is None:
        logger.warning("embed_photo_from_bytes: cv2 returned None")
        return None

    # C5: asyncio.to_thread for blocking ML call
    try:
        faces = await asyncio.to_thread(face_model.get, img)
    except Exception as exc:
        logger.warning("embed_photo_from_bytes: face_model.get raised %s", exc)
        return None

    if not faces:
        logger.info("embed_photo_from_bytes: no face detected")
        return None

    best = max(faces, key=lambda f: float(f.det_score))
    if best.embedding is None:
        logger.warning("embed_photo_from_bytes: face detected but embedding is None")
        return None

    return {
        "embedding": best.embedding.tolist(),
        "det_score": float(best.det_score),
        "bbox": best.bbox.tolist(),
        "phash": _dhash(img),
    }


async def process_photo_for_report(
    report_id: str,
    photo_url: str,
    app: Any,
    image_bytes: bytes | None = None,
) -> list[str]:
    """
    Extract a face embedding from photo_url and persist it in Supabase.

    Steps:
    1. Run embed_photo_from_url.
    2. If no face: PATCH photos row with quality_ok=False, return [].
    3. If face found: PATCH photos row with face_embedding, det_score,
       face_bbox, quality_ok=True.
    4. Trigger _search_face_matches for the opposite report kind.

    Returns match_ids inserted, best score first, or [] if no match was found.
    The top-scored one is not always disclosable (e.g. it may be another
    family's private report) — callers should try each in order until one
    clears disclosure, not stop at the first.
    """
    sb_url: str = app.state.supabase_url.rstrip("/")
    sb_key: str = app.state.supabase_service_key

    # image_bytes provided (e.g. Telegram) -> embed in memory; else download the URL.
    # photo_url is still used as the photos.storage_path key (for bytes channels it is
    # a stable non-URL token like "telegram:<file_unique_id>").
    if image_bytes is not None:
        result = await embed_photo_from_bytes(image_bytes, app.state.face_model)
    else:
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
            return []

        # Fetch report kind/source to determine opposite kind for matching and
        # to feed the same-photo (re-hosting) heuristic in _search_face_matches.
        try:
            rows = await _sb_get(
                client,
                f"{sb_url}/rest/v1/reports",
                sb_key,
                {"id": f"eq.{report_id}", "select": "id,kind,source,source_url"},
            )
        except Exception as exc:
            logger.error(
                "process_photo_for_report: could not fetch report %s: %s", report_id, exc
            )
            return []

        if not rows:
            logger.error("process_photo_for_report: report %s not found", report_id)
            return []

        source_kind: str = rows[0].get("kind", "") or "missing"
        source_report_source: str = rows[0].get("source") or ""
        source_report_source_url: str | None = rows[0].get("source_url")

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
                    "phash": result.get("phash"),
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
            return []

    # Search BOTH kinds: the same face is worth surfacing whether the person is
    # listed as missing (another searcher) or found (located). A missing↔missing
    # hit connects two families looking for the same person.
    match_ids: list[str] = []
    for target_kind in ("missing", "found"):
        ids = await _search_face_matches(
            report_id, result["embedding"], target_kind, source_kind, sb_url, sb_key,
            source_report_source=source_report_source,
            source_report_source_url=source_report_source_url,
            source_photo_url=photo_url,
            source_phash=result.get("phash"),
        )
        match_ids.extend(ids)
    return match_ids


async def _search_face_matches(
    source_report_id: str,
    face_embedding: list,
    target_kind: str,
    source_kind: str,
    sb_url: str,
    sb_key: str,
    source_report_source: str = "",
    source_report_source_url: str | None = None,
    source_photo_url: str = "",
    source_phash: str | None = None,
) -> list[str]:
    """
    Call the match_reports_by_face Supabase RPC, score each candidate,
    and insert qualifying rows into the matches table.

    Combined score formula (text signal not yet available at photo-ingest time):
        combined_score = FACE_WEIGHT * face_score

    A match row is inserted only when combined_score >= COMBINED_THRESHOLD.
    All inserted rows get status='pending' (human review required).

    Each row is flagged same_photo_suspected=True when the candidate's photo
    looks like a re-hosted copy of the source photo (phash near-duplicate, or
    a known cross-aggregator re-hosting pattern) — that is NOT independent
    corroboration and should be excluded from "confirmed" review (see
    migration 018 / _same_photo_suspected).

    Returns the ids of all match rows inserted, in the RPC's similarity-desc
    order (best score first), or [].
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
            return []

        qualifying: list[tuple[str, float]] = []
        for candidate in candidates:
            candidate_report_id: str = candidate.get("report_id", "")
            if not candidate_report_id or candidate_report_id == source_report_id:
                continue
            face_score = float(candidate.get("similarity", 0.0))
            # At photo-ingest time there is no text signal yet.
            # Gate on face similarity alone; combined_score = face_score.
            if face_score < FACE_MATCH_THRESHOLD:
                continue
            qualifying.append((candidate_report_id, face_score))

        if not qualifying:
            return []

        # Batch-fetch candidate source/photo info once (not per-candidate) for
        # the same-photo check below.
        cand_info: dict[str, dict] = {}
        try:
            ids_csv = ",".join(cid for cid, _ in qualifying)
            info_rows = await _sb_get(
                client, f"{sb_url}/rest/v1/reports", sb_key,
                {"id": f"in.({ids_csv})",
                 "select": "id,source,source_url,photos(storage_path,phash)"},
            )
            for r in info_rows:
                photos = r.get("photos") or []
                cand_info[r["id"]] = {
                    "source": r.get("source"),
                    "source_url": r.get("source_url"),
                    "storage_path": photos[0].get("storage_path") if photos else None,
                    "phash": photos[0].get("phash") if photos else None,
                }
        except Exception as exc:  # noqa: BLE001 - best-effort, never blocks the match
            logger.warning("_search_face_matches: candidate info fetch failed: %s", exc)

        match_ids: list[str] = []
        for candidate_report_id, face_score in qualifying:
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

            info = cand_info.get(candidate_report_id, {})
            suspected = _hamming_hex(source_phash, info.get("phash")) <= _PHASH_NEAR_DUP
            if not suspected:
                suspected = _same_photo_suspected(
                    source_report_source_url, source_photo_url, source_report_source,
                    info.get("source_url"), info.get("storage_path"), info.get("source"),
                )

            match_id = str(uuid.uuid4())
            row: dict = {
                "id": match_id,
                "missing_id": missing_id,
                "found_id": found_id,
                "face_score": round(face_score, 4),
                "text_score": 0.0,
                "combined_score": round(combined_score, 4),
                "status": "pending",
                "same_photo_suspected": suspected,
            }

            try:
                await _sb_insert(client, sb_url, sb_key, "matches", row)
                logger.info(
                    "Match inserted: id=%s missing=%s found=%s face=%.3f combined=%.3f "
                    "same_photo_suspected=%s",
                    match_id, missing_id, found_id, face_score, combined_score, suspected,
                )
                match_ids.append(match_id)
            except Exception as exc:
                logger.warning(
                    "_search_face_matches: insert failed for match candidate %s "
                    "(source=%s): %s",
                    candidate_report_id, source_report_id, exc,
                )

        return match_ids
