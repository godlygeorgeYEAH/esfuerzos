"""
scrapers/localizave.py -- localizave.com (SISMO 2026 hospital patients, WITH cédula).

Source:  https://localizave.com
API:     GET /api/pacientes?page=N&page_size=100  (public JSON, no auth)
         Response: {"data": [...], "pagination": {page, page_size, total, total_pages, has_more}}

Data: people physically located and receiving care in named hospitals -> kind='found'
for every record. Crucially carries CÉDULA, written into distinguishing_marks as
"CI: <digits>" so run_cedula_exact_match connects family searches to these by exact ID
(the strongest match). Digitized from photographed hospital lists.

Field mapping:
  nombre_completo  -> full_name
  edad             -> age (nullable)
  cedula (V/E+...) -> distinguishing_marks "CI: <digits>"
  ubicacion_actual -> last_seen_location (hospital name)
  estado_salud     -> person_state (deceased if fallecido/muerto, else alive)
  estado/municipio/parroquia -> appended to location when present
  id (int)         -> source_url "localizave:{id}" (stable dedup key)
The base64 `foto` field is intentionally NOT stored (would bloat the DB; many are null).

Register in scraper_orchestrator._make_scrapers() to enable.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx

from .base import BaseVEScraper

logger = logging.getLogger(__name__)

_SOURCE_NAME = "localizave"
_BASE = os.environ.get("LOCALIZAVE_BASE", "https://localizave.com")
_API = f"{_BASE}/api/pacientes"
_PAGE = 100
_MAX_PAGES = 500
_UA = {"User-Agent": "Mozilla/5.0 (compatible; ReuneVE/1.0)", "Accept": "application/json"}

_OWN_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_OWN_SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")


def _normalize(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    name = (raw.get("nombre_completo") or "").strip()
    if not name or len(name) < 3:
        return None
    pid = raw.get("id")
    if pid is None:
        return None
    age = raw.get("edad")
    try:
        age = int(age) if age not in (None, "") else None
        if age is not None and not (0 < age < 120):
            age = None
    except (TypeError, ValueError):
        age = None
    # Location: hospital + admin area when present.
    loc_parts = [raw.get("ubicacion_actual"), raw.get("municipio"), raw.get("estado")]
    location = ", ".join(p for p in loc_parts if p) or None
    # Cédula -> CI: digits (feeds exact match). Drop the V/E prefix, keep digits.
    cedula = re.sub(r"\D", "", str(raw.get("cedula") or ""))
    marks_parts = []
    if 5 <= len(cedula) <= 10:
        marks_parts.append(f"CI: {cedula}")
    salud = (raw.get("estado_salud") or "").strip()
    if salud and salud.lower() != "desconocido":
        marks_parts.append(f"Estado: {salud}")
    marks = " | ".join(marks_parts) or None
    salud_d = salud.lower()
    if any(t in salud_d for t in ("fallec", "muert", "occiso", "difunt")):
        person_state = "deceased"
    elif any(t in salud_d for t in ("herid", "grave", "critic")):
        person_state = "injured"
    else:
        person_state = "alive"          # listed in a hospital = located alive
    return {
        "kind": "found",
        "full_name": name,
        "age": age,
        "last_seen_location": location,
        "distinguishing_marks": marks,
        "clothing": None,
        "person_state": person_state,
        "source": _SOURCE_NAME,
        "source_url": f"localizave:{pid}",
        "raw_data": {"id": pid, "cedula": raw.get("cedula"),
                     "ubicacion_actual": raw.get("ubicacion_actual"),
                     "estado_salud": salud or None,
                     "telefono": raw.get("telefono"),
                     "fecha_registro": raw.get("fecha_registro")},
    }


class LocalizaveScraper(BaseVEScraper):
    source_name = _SOURCE_NAME

    async def _fetch_page(self, page: int) -> tuple[list[dict], bool]:
        async with httpx.AsyncClient(timeout=30, headers=_UA, follow_redirects=True) as cl:
            r = await cl.get(_API, params={"page": page, "page_size": _PAGE})
            r.raise_for_status()
            d = r.json()
        data = d.get("data") if isinstance(d, dict) else (d if isinstance(d, list) else [])
        pag = d.get("pagination") if isinstance(d, dict) else {}
        has_more = bool(pag.get("has_more")) if isinstance(pag, dict) else (len(data) >= _PAGE)
        return (data or []), has_more

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
            page, _ = await self._fetch_page(1)
            rows = [r for r in (_normalize(x) for x in page) if r]
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
            page_n = 1
            for _ in range(_MAX_PAGES):
                page, has_more = await self._fetch_page(page_n)
                if not page:
                    break
                rows = [r for r in (_normalize(x) for x in page) if r]
                total += await self._upsert(rows)
                if not has_more:
                    break
                page_n += 1
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.error("%s full_sweep error: %s", _SOURCE_NAME, exc)
        logger.info("%s full_sweep: upserted %d", _SOURCE_NAME, total)
        await self.log_run(_SOURCE_NAME, "full_sweep", total, 0, error)
        return total
