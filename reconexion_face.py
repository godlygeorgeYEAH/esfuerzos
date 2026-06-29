"""
reconexion_face.py - Facial recognition via the Reconexión /identificar endpoint.

A SECOND face engine, complementary to the local InsightFace pipeline: sends a photo
to reconexión's curated registry and turns strong matches into pending `matches` rows
for human review. The matched persona is looked up (or upserted) as a `reconexion`
report so both sides of the match are rows in our `reports` table.

NON-NEGOTIABLE minor governance: when /identificar returns needsReview=true (a minor
query) it gives NO candidates — we store nothing and route to human review. Matches
are ALWAYS status='pending'; never auto-confirmed.

Two entry points:
  identify_and_store(report_id, photo_url, app, is_minor)  - real-time (photo intake)
  run_reconexion_face_backfill(app)                        - scheduled sweep over our
        missing-with-photo corpus vs the reconexión registry (rate-limited)
"""
from __future__ import annotations

import ipaddress
import logging
import urllib.parse
import uuid
from typing import Any

import httpx

import reconexion_client as rc
from config import get_settings
from face_pipeline import _sb_get, _sb_headers, _sb_insert
from scrapers.reconexion_api import normalize_persona

logger = logging.getLogger("reconexion_face")
settings = get_settings()

_BACKFILL_BATCH = 15          # reports per backfill run (API limit is generous, stay gentle)
_UA = "Mozilla/5.0 (compatible; ReuneVE/1.0)"


def _url_allowed(url: str) -> bool:
    """Allow https public hosts + the trusted internal WAHA host (plain http on the
    docker net). Block private/loopback for external hosts (SSRF)."""
    try:
        p = urllib.parse.urlparse(url)
    except Exception:
        return False
    host = (p.hostname or "").lower()
    if not host:
        return False
    waha_host = (urllib.parse.urlparse(settings.waha_url).hostname or "").lower()
    if host == waha_host:
        return True
    if p.scheme != "https":
        return False
    try:
        addr = ipaddress.ip_address(host)
        if addr.is_private or addr.is_loopback or addr.is_link_local:
            return False
    except ValueError:
        pass  # domain, allow
    return True


async def _download(url: str) -> tuple[bytes | None, str]:
    if not url or not _url_allowed(url):
        return None, ""
    headers = {"User-Agent": _UA}
    waha_host = (urllib.parse.urlparse(settings.waha_url).hostname or "").lower()
    if (urllib.parse.urlparse(url).hostname or "").lower() == waha_host and settings.waha_api_key:
        headers["X-Api-Key"] = settings.waha_api_key
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as cl:
            r = await cl.get(url, headers=headers)
            r.raise_for_status()
            return r.content, r.headers.get("content-type", "image/jpeg")
    except Exception as exc:  # noqa: BLE001
        logger.warning("reconexion_face download failed %s: %s", url, exc)
        return None, ""


async def _ensure_persona_report(client: httpx.AsyncClient, sb_url: str, sb_key: str,
                                 persona: dict) -> str | None:
    """Return our report id for a reconexión persona, upserting it if absent."""
    pid = persona.get("id")
    if not pid:
        return None
    surl = f"reconexion:{pid}"
    rows = await _sb_get(client, f"{sb_url}/rest/v1/reports", sb_key,
                         {"source": "eq.reconexion", "source_url": f"eq.{surl}",
                          "select": "id", "limit": "1"})
    if rows:
        return rows[0]["id"]
    norm = normalize_persona(persona)
    if not norm:
        return None
    resp = await client.post(f"{sb_url}/rest/v1/reports",
                             headers=_sb_headers(sb_key),
                             params={"on_conflict": "source,source_url"},
                             json=norm)
    if resp.status_code in (200, 201):
        body = resp.json()
        return body[0]["id"] if body else None
    return None


async def _match_exists(client: httpx.AsyncClient, sb_url: str, sb_key: str,
                        a: str, b: str) -> bool:
    rows = await _sb_get(client, f"{sb_url}/rest/v1/matches", sb_key,
                         {"missing_id": f"eq.{a}", "found_id": f"eq.{b}",
                          "select": "id", "limit": "1"})
    return bool(rows)


async def identify_and_store(report_id: str, photo_url: str, app: Any,
                             is_minor: bool = False, image_bytes: bytes | None = None,
                             content_type: str = "image/jpeg") -> int:
    """Run /identificar for one report's photo; store strong matches as pending rows.
    Pass image_bytes to skip the download (e.g. Telegram already has the bytes).
    Returns the number of matches created. Best-effort; never raises."""
    if not rc.enabled():
        return 0
    sb_url = app.state.supabase_url.rstrip("/")
    sb_key = app.state.supabase_service_key
    try:
        if image_bytes is not None:
            img, ctype = image_bytes, content_type
        else:
            img, ctype = await _download(photo_url)
        if not img:
            return 0
        res = await rc.identificar(img, content_type=ctype, es_menor=is_minor)
        if not res:
            return 0
        if res.get("needsReview"):
            logger.info("reconexion_face: needsReview (minor governance) for report %s — "
                        "no candidates stored, route to human", report_id)
            return 0
        results = [r for r in (res.get("results") or []) if r.get("strongMatch")]
        if not results:
            return 0
        created = 0
        async with httpx.AsyncClient(timeout=30) as client:
            for r in results:
                persona = r.get("persona") or {}
                found_id = await _ensure_persona_report(client, sb_url, sb_key, persona)
                if not found_id or found_id == report_id:
                    continue
                if await _match_exists(client, sb_url, sb_key, report_id, found_id):
                    continue
                row = {
                    "id": str(uuid.uuid4()),
                    "missing_id": report_id,
                    "found_id": found_id,
                    "face_score": round(float(r.get("score") or 0), 4),
                    "text_score": 0.0,
                    "combined_score": round(float(r.get("score") or 0), 4),
                    "status": "pending",
                }
                try:
                    await _sb_insert(client, sb_url, sb_key, "matches", row)
                    created += 1
                    logger.info("reconexion_face match: report %s ~ %s score=%.3f",
                                report_id, found_id, row["face_score"])
                except Exception as exc:  # noqa: BLE001
                    logger.warning("reconexion_face insert failed: %s", exc)
        return created
    except Exception as exc:  # noqa: BLE001
        logger.warning("reconexion_face identify_and_store error (report %s): %s", report_id, exc)
        return 0


async def run_reconexion_face_backfill(app: Any) -> dict:
    """Scheduled: cross our missing-with-photo reports against the reconexión face
    registry. Marks each processed report (raw_data.reconexion_checked=true) so it is
    queried once. Rate-limited via small batches."""
    if not rc.enabled():
        return {"skipped": "no reconexion_api_key"}
    sb_url = app.state.supabase_url.rstrip("/")
    sb_key = app.state.supabase_service_key
    processed = 0
    matched = 0
    async with httpx.AsyncClient(timeout=30) as client:
        # missing reports with a foto_url not yet checked against reconexión
        rows = await _sb_get(client, f"{sb_url}/rest/v1/reports", sb_key, {
            "kind": "eq.missing",
            "raw_data->>foto_url": "not.is.null",
            "raw_data->>reconexion_checked": "is.null",
            "source": "neq.reconexion",  # don't match the registry against itself
            "select": "id,raw_data",
            "order": "created_at.desc",
            "limit": str(_BACKFILL_BATCH),
        })
    for r in rows:
        rid = r["id"]
        foto = (r.get("raw_data") or {}).get("foto_url")
        if foto:
            matched += await identify_and_store(rid, foto, app, is_minor=False)
        # mark as checked (merge flag into raw_data) regardless, so we don't re-query
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                rd = dict(r.get("raw_data") or {})
                rd["reconexion_checked"] = True
                await client.patch(f"{sb_url}/rest/v1/reports",
                                   headers=_sb_headers(sb_key, "return=minimal"),
                                   params={"id": f"eq.{rid}"},
                                   json={"raw_data": rd})
        except Exception as exc:  # noqa: BLE001
            logger.warning("reconexion_face backfill mark failed %s: %s", rid, exc)
        processed += 1
    logger.info("reconexion_face backfill: processed=%d matched=%d", processed, matched)
    return {"processed": processed, "matched": matched}
