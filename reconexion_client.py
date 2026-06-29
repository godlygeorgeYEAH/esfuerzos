"""
reconexion_client.py - Client for the Reconexión / theempire integrator API.

Read-only API exposing depurated public records of missing/located persons, the
centros (hospitals/shelters), their rosters (listas), and a facial-recognition
endpoint (/identificar). Auth via the X-Api-Key header; cursor pagination.

IMPORTANT: the API sits behind CloudFront which fingerprints the TLS handshake
(JA3) and 403s plain Python clients (httpx/aiohttp) regardless of headers. We use
curl_cffi with Chrome impersonation, which presents a real browser TLS fingerprint
and passes (verified live 2026-06-29). Do not swap this back to httpx.

Spec: GET /personas (estado sin-contacto|localizado), /personas/{id},
/centros (tipo hospital|centro_acopio), /centros/{id}, /listas, /listas/{id},
POST /identificar {foto: data-url base64, esMenor: bool}. Rate limit ~1000/60s.
"""
from __future__ import annotations

import asyncio
import base64
import logging

from curl_cffi.requests import AsyncSession

from config import get_settings

logger = logging.getLogger("reconexion")
settings = get_settings()

_IMPERSONATE = "chrome124"
_RETRY_CAP = 8.0  # seconds; cap the 429 backoff


def enabled() -> bool:
    """True when an API key is configured (the source/feature is opt-in)."""
    return bool(settings.reconexion_api_key)


def _headers(extra: dict | None = None) -> dict:
    h = {
        "X-Api-Key": settings.reconexion_api_key,
        "Accept": "application/json",
    }
    if extra:
        h.update(extra)
    return h


def _retry_after(resp) -> float:
    """Seconds to wait before retrying a 429, from rate-limit headers, capped."""
    for k in ("retry-after", "ratelimit-reset", "x-ratelimit-reset"):
        v = resp.headers.get(k)
        if v:
            try:
                return min(float(v), _RETRY_CAP)
            except ValueError:
                pass
    return 2.0


async def _request(method: str, path: str, *, params: dict | None = None,
                   json_body: dict | None = None, timeout: float = 25.0):
    """One request through a Chrome-impersonating session, with a single 429 retry.
    Returns the parsed JSON dict, or None on any failure (logged)."""
    if not enabled():
        return None
    url = settings.reconexion_base_url.rstrip("/") + path
    for attempt in range(2):
        try:
            async with AsyncSession(impersonate=_IMPERSONATE, timeout=timeout) as s:
                if method == "GET":
                    resp = await s.get(url, headers=_headers(), params=params)
                else:
                    resp = await s.post(url, headers=_headers({"Content-Type": "application/json"}),
                                        json=json_body)
            if resp.status_code == 429 and attempt == 0:
                wait = _retry_after(resp)
                logger.warning("reconexion 429 on %s, waiting %.1fs", path, wait)
                await asyncio.sleep(wait)
                continue
            if resp.status_code != 200:
                logger.warning("reconexion %s %s -> %s: %s", method, path,
                               resp.status_code, resp.text[:160])
                return None
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("reconexion %s %s failed: %s", method, path, exc)
            return None
    return None


async def list_personas(cursor: str | None = None, estado: str | None = None,
                        limit: int = 100) -> tuple[list[dict], str | None]:
    """One page of persons. Returns (data, next_cursor). estado: sin-contacto|localizado."""
    params: dict = {"limit": str(limit)}
    if cursor:
        params["cursor"] = cursor
    if estado:
        params["estado"] = estado
    d = await _request("GET", "/personas", params=params)
    if not d:
        return [], None
    pag = d.get("pagination") or {}
    return d.get("data") or [], (pag.get("nextCursor") if pag.get("hasMore") else None)


async def get_persona(persona_id: str) -> dict | None:
    return await _request("GET", f"/personas/{persona_id}")


async def list_centros(cursor: str | None = None, tipo: str | None = None,
                       limit: int = 100) -> tuple[list[dict], str | None]:
    """One page of centros. tipo: hospital|centro_acopio."""
    params: dict = {"limit": str(limit)}
    if cursor:
        params["cursor"] = cursor
    if tipo:
        params["tipo"] = tipo
    d = await _request("GET", "/centros", params=params)
    if not d:
        return [], None
    pag = d.get("pagination") or {}
    return d.get("data") or [], (pag.get("nextCursor") if pag.get("hasMore") else None)


async def list_listas(cursor: str | None = None, limit: int = 100) -> tuple[list[dict], str | None]:
    params: dict = {"limit": str(limit)}
    if cursor:
        params["cursor"] = cursor
    d = await _request("GET", "/listas", params=params)
    if not d:
        return [], None
    pag = d.get("pagination") or {}
    return d.get("data") or [], (pag.get("nextCursor") if pag.get("hasMore") else None)


async def get_lista(lista_id: str) -> dict | None:
    """ListaDetail: {lista, centro, entradas[]}."""
    return await _request("GET", f"/listas/{lista_id}")


async def identificar(photo_bytes: bytes, content_type: str = "image/jpeg",
                      es_menor: bool = False) -> dict | None:
    """Facial recognition against the reconexión registry. Returns the raw result:
    {results:[{faceId,score,strongMatch,persona}], needsReview, strongThreshold,
    queryFaces, ...} or None on failure.

    MINOR GOVERNANCE: when es_menor is True (or the registry detects a minor) the
    API returns needsReview=true with NO candidates — the caller MUST route to human
    review and never disclose candidates. Never auto-confirm a match from here.
    """
    if not photo_bytes:
        return None
    b64 = base64.b64encode(photo_bytes).decode()
    data_url = f"data:{content_type or 'image/jpeg'};base64,{b64}"
    return await _request("POST", "/identificar",
                          json_body={"foto": data_url, "esMenor": bool(es_menor)},
                          timeout=45.0)
