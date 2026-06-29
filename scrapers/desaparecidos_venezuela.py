"""
scrapers/desaparecidos_venezuela.py -- desaparecidosvenezuela.com (SISMO 2026).

Source:  https://www.desaparecidosvenezuela.com
API:     GET /api/personas?skip=N&take=20  (public JSON, no auth)
         The server hard-caps `take` at 20 regardless of what is sent, so we
         paginate with skip += 20 and stop on the first empty page (same pattern
         as pacientes_terremoto). Default order is newest-first by updatedAt.

Data: community reports of missing / located people after the June 2026 quake.
  estado = "BUSCADO"       -> kind = "missing"
  estado = "SANO_SALVO"    -> kind = "found"   (self-reported safe)
  estado = "INFO_RECIBIDA" -> kind = "found"   (community confirmed located)
Records with oculto=true are platform-hidden and skipped.

Photos: each person exposes GET /api/personas/{id}/foto (binary). We store the
absolute URL in raw_data.foto_url so face_backfill embeds it. No cédula in this source.

source_url = the canonical person page https://www.desaparecidosvenezuela.com/p/{id}
(a real, openable link; unique per record for the (source, source_url) upsert).

Register in scraper_orchestrator._make_scrapers() to enable.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from .base import BaseVEScraper

logger = logging.getLogger(__name__)

_SOURCE_NAME = "desaparecidos_venezuela"
_BASE = os.environ.get("DESAPARECIDOSVE_BASE", "https://www.desaparecidosvenezuela.com")
_API = f"{_BASE}/api/personas"
_PAGE = 20                      # server cap on `take`
_MAX_PAGES = 400               # safety bound (~8k records)
_UA = {"User-Agent": "Mozilla/5.0 (compatible; ReuneVE/1.0)", "Accept": "application/json"}

_OWN_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_OWN_SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

_FOUND = {"SANO_SALVO", "INFO_RECIBIDA"}


def _normalize(raw: dict) -> dict | None:
    """Map an /api/personas row to the reports schema. None if hidden or nameless."""
    if not isinstance(raw, dict) or raw.get("oculto") is True:
        return None
    name = (raw.get("nombre") or "").strip()
    if not name or len(name) < 3:
        return None
    pid = raw.get("id")
    if not pid:
        return None
    estado = (raw.get("estado") or "BUSCADO").upper()
    kind = "found" if estado in _FOUND else "missing"
    age = raw.get("edad")
    try:
        age = int(age) if age not in (None, "") else None
        if age is not None and not (0 < age < 120):
            age = None
    except (TypeError, ValueError):
        age = None
    marks = (raw.get("descripcion") or "").strip()
    # Append the latest community update (e.g. "Sí ya apareció") for context.
    upds = raw.get("actualizaciones") or []
    if upds and isinstance(upds, list):
        msg = (upds[-1].get("mensaje") or "").strip() if isinstance(upds[-1], dict) else ""
        if msg:
            marks = (marks + " | " if marks else "") + f"Actualización: {msg}"
    marks = marks[:480] or None
    foto = raw.get("fotoUrl")
    foto_url = (_BASE + foto) if isinstance(foto, str) and foto.startswith("/") else foto
    raw_clean = {k: v for k, v in raw.items() if k not in ("actualizaciones",)}
    raw_clean["foto_url"] = foto_url
    return {
        "kind": kind,
        "full_name": name,
        "age": age,
        "last_seen_location": (raw.get("zona") or None),
        "distinguishing_marks": marks,
        "clothing": None,
        "source": _SOURCE_NAME,
        "source_url": f"{_BASE}/p/{pid}",
        "raw_data": raw_clean,
    }


class DesaparecidosVenezuelaScraper(BaseVEScraper):
    source_name = _SOURCE_NAME

    async def _fetch_page(self, skip: int) -> list[dict]:
        async with httpx.AsyncClient(timeout=30, headers=_UA, follow_redirects=True) as cl:
            r = await cl.get(_API, params={"skip": skip, "take": _PAGE})
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else (data.get("data") or [])

    async def _upsert(self, rows: list[dict]) -> int:
        if not _OWN_SUPABASE_URL or not _OWN_SUPABASE_KEY:
            raise RuntimeError("SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set")
        if not rows:
            return 0
        done = 0
        async with httpx.AsyncClient(timeout=30) as cl:
            for i in range(0, len(rows), 200):
                chunk = rows[i:i + 200]
                resp = await cl.post(
                    f"{_OWN_SUPABASE_URL.rstrip('/')}/rest/v1/reports",
                    headers=self._sb_headers(),
                    params={"on_conflict": "source,source_url"},
                    json=chunk)
                if resp.status_code in (200, 201, 204):
                    done += len(chunk)
                else:
                    logger.warning("%s upsert %d: %s", _SOURCE_NAME, resp.status_code, resp.text[:160])
        return done

    async def poll_recent(self) -> int:
        count = 0
        error: str | None = None
        try:
            rows = [r for r in (_normalize(x) for x in await self._fetch_page(0)) if r]
            count = await self._upsert(rows)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.error("%s poll_recent error: %s", _SOURCE_NAME, exc)
        await self.log_run(_SOURCE_NAME, "poll_recent", count, 0, error)
        return count

    async def full_sweep(self) -> int:
        total = 0
        error: str | None = None
        try:
            skip = 0
            for _ in range(_MAX_PAGES):
                page = await self._fetch_page(skip)
                if not page:
                    break
                rows = [r for r in (_normalize(x) for x in page) if r]
                total += await self._upsert(rows)
                if len(page) < _PAGE:
                    break
                skip += _PAGE
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.error("%s full_sweep error: %s", _SOURCE_NAME, exc)
        logger.info("%s full_sweep: upserted %d", _SOURCE_NAME, total)
        await self.log_run(_SOURCE_NAME, "full_sweep", total, 0, error)
        return total
