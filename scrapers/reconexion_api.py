"""
scrapers/reconexion_api.py -- Reconexión / theempire integrator API (read-only).

Replaces the old scrapers/reconexion.py, which 403s: CloudFront fingerprints TLS
(JA3) and blocks plain Python HTTP. We fetch via reconexion_client (curl_cffi with
Chrome impersonation) and upsert to OUR Supabase over httpx (no CloudFront there).

Ingests:
  /personas -> reports. estado 'sin-contacto'->kind=missing, 'localizado'->found.
              cedula -> distinguishing_marks "CI: <digits>" (feeds run_cedula_exact_match);
              edad->age; ubicacion.texto->last_seen_location; foto->raw_data.foto_url
              (feeds face_backfill). source="reconexion", source_url="reconexion:<id>".
  /listas   -> reports (found). Names registered in centros (hospitals/shelters) =
              located. source="reconexion_listas", source_url="reconexion_lista:<id>".

person_state stays 'unknown' (estado is a contact-state, not a medical one).
Standalone /centros ingestion is deferred; the centro is embedded per persona.
Enabled only when settings.reconexion_api_key is set (registered in the orchestrator).
"""
from __future__ import annotations

import logging
import os
import re

import httpx

import reconexion_client as rc
from .base import BaseVEScraper

logger = logging.getLogger(__name__)

_SOURCE = "reconexion"
_SOURCE_LISTAS = "reconexion_listas"
_MAX_PAGES = 1000  # cursor pages safety bound

_OWN_SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
_OWN_SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")


def _age(v) -> int | None:
    try:
        a = int(str(v).split()[0]) if v not in (None, "") else None
        return a if (a is None or 0 < a < 120) else None
    except (TypeError, ValueError):
        return None


def normalize_persona(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None
    name = (raw.get("nombre") or "").strip()
    pid = raw.get("id")
    if not name or len(name) < 3 or not pid:
        return None
    estado = (raw.get("estado") or "").lower()
    kind = "found" if "localizado" in estado else "missing"
    cedula = re.sub(r"\D", "", str(raw.get("cedula") or ""))
    marks_parts = []
    if 5 <= len(cedula) <= 10:
        marks_parts.append(f"CI: {cedula}")
    desc = (raw.get("descripcion") or "").strip()
    if desc:
        marks_parts.append(desc)
    centro = raw.get("centro") or None
    if isinstance(centro, dict) and centro.get("nombre"):
        marks_parts.append(f"Centro: {centro['nombre']}")
    marks = " | ".join(marks_parts) or None
    ubic = raw.get("ubicacion") or {}
    location = (ubic.get("texto") if isinstance(ubic, dict) else None) or None
    foto = (raw.get("foto") or "").strip() or None
    return {
        "kind": kind,
        "full_name": name,
        "age": _age(raw.get("edad")),
        "last_seen_location": location,
        "distinguishing_marks": marks,
        "clothing": None,
        "person_state": "unknown",
        "source": _SOURCE,
        "source_url": f"reconexion:{pid}",
        # PII (contacto.telefono) intentionally dropped; keep foto for face backfill.
        "raw_data": {"id": pid, "estado": estado or None, "foto_url": foto,
                     "centro": (centro.get("nombre") if isinstance(centro, dict) else None)},
    }


def normalize_entrada(entrada: dict, centro: dict | None) -> dict | None:
    if not isinstance(entrada, dict):
        return None
    name = (entrada.get("nombre") or "").strip()
    eid = entrada.get("id")
    if not name or len(name) < 3 or not eid:
        return None
    centro_name = (centro or {}).get("nombre") if isinstance(centro, dict) else None
    return {
        "kind": "found",
        "full_name": name,
        "age": _age(entrada.get("edad")),
        "last_seen_location": centro_name,
        "distinguishing_marks": (f"Centro: {centro_name}" if centro_name else None),
        "clothing": None,
        "person_state": "unknown",
        "source": _SOURCE_LISTAS,
        "source_url": f"reconexion_lista:{eid}",
        "raw_data": {"id": eid, "centro": centro_name, "estado": entrada.get("estado")},
    }


class ReconexionAPIScraper(BaseVEScraper):
    source_name = _SOURCE

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
                    logger.warning("%s upsert %d: %s", _SOURCE, resp.status_code, resp.text[:160])
        return done

    async def poll_recent(self) -> int:
        """First page of personas (newest by updatedAt desc)."""
        count = 0
        error: str | None = None
        try:
            data, _ = await rc.list_personas(limit=100)
            rows = [r for r in (normalize_persona(x) for x in data) if r]
            count = await self._upsert(rows)
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.error("%s poll_recent error: %s", _SOURCE, exc)
        await self.log_run(_SOURCE, "poll_recent", count, 0, error)
        return count

    async def full_sweep(self) -> int:
        total = 0
        error: str | None = None
        try:
            # personas (cursor)
            cursor = None
            for _ in range(_MAX_PAGES):
                data, cursor = await rc.list_personas(cursor=cursor, limit=200)
                if not data:
                    break
                rows = [r for r in (normalize_persona(x) for x in data) if r]
                total += await self._upsert(rows)
                if not cursor:
                    break
            # listas (cursor) -> entradas via detail
            lcursor = None
            for _ in range(_MAX_PAGES):
                listas, lcursor = await rc.list_listas(cursor=lcursor, limit=100)
                if not listas:
                    break
                for lst in listas:
                    detail = await rc.get_lista(lst.get("id")) if lst.get("id") else None
                    if not detail:
                        continue
                    centro = detail.get("centro")
                    entradas = detail.get("entradas") or []
                    rows = [r for r in (normalize_entrada(e, centro) for e in entradas) if r]
                    total += await self._upsert(rows)
                if not lcursor:
                    break
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.error("%s full_sweep error: %s", _SOURCE, exc)
        logger.info("%s full_sweep: upserted %d", _SOURCE, total)
        await self.log_run(_SOURCE, "full_sweep", total, 0, error)
        return total
