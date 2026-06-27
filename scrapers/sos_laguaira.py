"""
scrapers/sos_laguaira.py -- Periodic scraper for SOS La Guaira.

Source: https://soslaguaira.lat/ (API: https://api.soslaguaira.lat/api/personas)
Kind: both (missing and found)

SOS La Guaira is a reunification and rescue platform focused exclusively on the
La Guaira / Vargas region, which suffered the highest building-collapse rate in
the 2026 earthquake. The site is a Vite/React SPA; data lives in a custom REST
API at api.soslaguaira.lat.

API reference (discovered via JS bundle network inspection):
  GET https://api.soslaguaira.lat/api/personas
  Response: {"success": true, "data": [...], "message": "Listado de personas."}
  No authentication required.
  CORS: unrestricted.

Pagination: the API returns all records in a single response and ignores
page/limit query params. fetch_page(1) fetches the full set; fetch_page(n>1)
returns [] to satisfy BaseScraper.run_full()'s termination contract.
No server-side record cap was observed but the dataset is growing -- if the API
adds pagination in the future, the fetch_page implementation below must be updated.

Schema fields (observed):
  id                 -- integer, unique per record (dedup key)
  tipo               -- submission channel: "busco" | "reporto" | "pub" | "wa"
  nombre             -- full name (UTF-8, may include multiple persons)
  edad               -- integer age or null
  descripcion        -- free-text notes (maps to distinguishing_marks)
  foto_url           -- photo URL or null
  estado             -- status enum (drives kind; see _KIND_FROM_ESTADO below)
  lat / lng          -- GPS coordinates
  direccion          -- street address (incident location, not reporter PII)
  edificio           -- building name/number
  piso               -- floor number
  contacto_nombre    -- reporter name (PII -- excluded from raw_data)
  contacto_telefono  -- reporter phone (PII -- excluded from raw_data)
  cedula             -- national ID (PII -- excluded from raw_data)
  created_at         -- ISO-8601 timestamp

Kind logic:
  estado is the primary signal; tipo is used only for ambiguous estados.
  fallecido -> kind=found, status is written to distinguishing_marks (not a
  boolean field -- per system constraint 3).

Dedup key: source_url = "sos_laguaira:{id}"

Upsert resolution: ignore-duplicates (constraint 6).
  Limitation: if the API updates records in place (e.g. desaparecido ->
  rescatado), the status change will NOT propagate to existing rows. For a
  fully mutable source, switch to merge-duplicates and document the deviation
  from constraint 6 (see red_solidaria_venezuela.py for that pattern). The
  site was 1 day old at scraper creation; append-only behavior was assumed.

Orchestrator registration (add to scraper_orchestrator.py _make_scrapers()):
    from scrapers.sos_laguaira import SosLaGuairaScraper
    scrapers["sos_laguaira"] = SosLaGuairaScraper(sb_url, sb_key)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import httpx

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_SOURCE_NAME = "sos_laguaira"
_API_URL = "https://api.soslaguaira.lat/api/personas"
_DEFAULT_LOCATION = "La Guaira, Vargas, Venezuela"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ReuneVE-Scraper/1.0; +https://reune.ve/acerca)",
    "Accept": "application/json",
}

# ---------------------------------------------------------------------------
# Estado -> kind mapping
# ---------------------------------------------------------------------------

# Estados that unambiguously indicate the person has been located/confirmed.
_FOUND_ESTADOS = frozenset({
    "a_salvo",
    "evacuado_ok",
    "rescatado",
    "visto_con_vida",
    "herido",
    "fallecido",
})

# Estados that unambiguously indicate the person is still missing / unlocated.
_MISSING_ESTADOS = frozenset({
    "desaparecido",
})

# Ambiguous estados (person location partially known but situation unresolved).
# Resolved via tipo, same as truly-unknown estados, but kept as a named set so
# future maintainers can easily split the handling if needed.
_AMBIGUOUS_ESTADOS = frozenset({
    "atrapado",
    "atrapados",
    "en_rescate",
    "colapsado",
    "dano_grave",
})

# Human-readable estado labels for distinguishing_marks.
_ESTADO_LABEL: dict[str, str] = {
    "a_salvo": "A salvo",
    "atrapado": "Atrapado",
    "atrapados": "Atrapados",
    "colapsado": "Edificio colapsado",
    "dano_grave": "Dano grave",
    "desaparecido": "Desaparecido",
    "en_rescate": "En rescate",
    "evacuado_ok": "Evacuado, a salvo",
    "fallecido": "Fallecido",
    "herido": "Herido",
    "rescatado": "Rescatado",
    "visto_con_vida": "Visto con vida",
}


def _infer_kind(tipo: str, estado: str) -> str:
    """
    Derive kind from estado (primary) with tipo as fallback for ambiguous cases.

    Rules:
    - Known found estado    -> "found"
    - Known missing estado  -> "missing"
    - Ambiguous estado      -> "missing" if tipo == "busco", else "found"
    - Unknown estado        -> "missing" if tipo == "busco", else "found"
    """
    estado_norm = (estado or "").lower().strip()

    if estado_norm in _FOUND_ESTADOS:
        return "found"
    if estado_norm in _MISSING_ESTADOS:
        return "missing"

    # Ambiguous estados (atrapado, en_rescate, colapsado, etc.): person situation
    # is unresolved; fall back to tipo as the intent signal.
    tipo_norm = (tipo or "").lower().strip()
    if estado_norm in _AMBIGUOUS_ESTADOS:
        return "missing" if tipo_norm == "busco" else "found"

    # Truly unknown estado (new value added by API): same tipo-based fallback.
    return "missing" if tipo_norm == "busco" else "found"


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


class SosLaGuairaScraper(BaseScraper):
    """
    Periodic scraper for SOS La Guaira.

    Fetches all persons reported on the SOS La Guaira platform via its REST API.
    Covers both missing persons (tipo=busco) and located/confirmed persons
    (tipo=reporto). Geographic scope is La Guaira / Vargas only.

    Inherits run_poll() and run_full() from BaseScraper.
    upsert_report() is overridden to enforce ignore-duplicates (constraint 6).
    fetch_page() uses httpx directly (project-wide httpx preference).
    """

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__(_SOURCE_NAME, supabase_url, supabase_key)

    # ------------------------------------------------------------------
    # Constraint 6 override: ignore-duplicates for all inserts.
    # See module docstring for the tradeoff vs merge-duplicates.
    # ------------------------------------------------------------------

    async def upsert_report(self, data: dict) -> bool:
        """
        POST a single report to Supabase with resolution=ignore-duplicates.
        Deduplication key: (source, source_url).
        Returns True on success, False on HTTP error or network failure.
        """
        try:
            async with httpx.AsyncClient(timeout=15) as cl:
                resp = await cl.post(
                    f"{self._supabase_url}/rest/v1/reports",
                    headers=self._sb_headers(
                        "resolution=ignore-duplicates,return=minimal"
                    ),
                    params={"on_conflict": "source,source_url"},
                    json=[data],
                )
                if resp.status_code in (200, 201):
                    return True
                logger.warning(
                    "[%s] upsert_report HTTP %d: %s",
                    _SOURCE_NAME,
                    resp.status_code,
                    resp.text[:150],
                )
                return False
        except httpx.HTTPError as exc:
            logger.warning("[%s] upsert_report network error: %s", _SOURCE_NAME, exc)
            return False
        except Exception as exc:
            logger.warning("[%s] upsert_report unexpected error: %s", _SOURCE_NAME, exc)
            return False

    # ------------------------------------------------------------------
    # BaseScraper abstract interface
    # ------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """
        Fetch all personas from the SOS La Guaira API.

        The API returns the full dataset in a single unfiltered response.
        Page 1 returns all records; page > 1 returns [] to terminate
        BaseScraper.run_full()'s pagination loop.

        Uses httpx with 3 retry attempts (exponential backoff 1s, 2s, 4s).
        """
        if page > 1:
            return []

        last_exc: Optional[Exception] = None
        for attempt in range(3):
            wait = 2 ** attempt  # 1s, 2s, 4s
            try:
                async with httpx.AsyncClient(
                    timeout=20.0, headers=_HEADERS
                ) as client:
                    resp = await client.get(_API_URL)
                    resp.raise_for_status()
                    # resp.json() decodes using the response charset (UTF-8),
                    # preserving accented characters correctly.
                    payload = resp.json()

                if not isinstance(payload, dict) or not payload.get("success"):
                    logger.warning(
                        "[%s] Unexpected API response shape: %s",
                        _SOURCE_NAME,
                        str(payload)[:200],
                    )
                    return []

                records: list[dict[str, Any]] = payload.get("data") or []
                logger.debug("[%s] fetch_page(1): %d records", _SOURCE_NAME, len(records))
                return records

            except httpx.HTTPStatusError as exc:
                last_exc = exc
                logger.warning(
                    "[%s] fetch_page attempt %d HTTP %d -- retry in %ds",
                    _SOURCE_NAME,
                    attempt + 1,
                    exc.response.status_code,
                    wait,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "[%s] fetch_page attempt %d failed: %s -- retry in %ds",
                    _SOURCE_NAME,
                    attempt + 1,
                    exc,
                    wait,
                )

            if attempt < 2:
                await asyncio.sleep(wait)

        logger.error(
            "[%s] fetch_page failed after 3 attempts: %s", _SOURCE_NAME, last_exc
        )
        return []

    def normalize(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        """
        Map a SOS La Guaira API record to the 'reports' table schema.

        Field mapping:
          nombre      -> full_name
          edad        -> age
          descripcion -> base for distinguishing_marks
          estado      -> prepended to marks (takes priority to survive truncation);
                        drives kind via _infer_kind()
          tipo        -> fallback for kind when estado is ambiguous
          direccion   -> first component of last_seen_location (not stored in raw_data)
          edificio    -> second component of last_seen_location
          piso        -> appended to location when present
          lat/lng     -> appended to location as coordinates when no address
          id          -> suffix of source_url (dedup key)
          foto_url    -> photo_url field

        PII excluded from raw_data:
          cedula, contacto_nombre, contacto_telefono
          (direccion is also excluded from raw_data; strip_pii drops it as well)

        Deceased persons (estado=fallecido):
          kind='found', "Fallecido" written to distinguishing_marks.
          No boolean field used (constraint 3).

        distinguishing_marks layout: estado label is prepended before descripcion
        so that truncation from the end never silently drops the status tag
        (constraint 2).
        """
        try:
            record_id = raw.get("id")
            nombre = (raw.get("nombre") or "").strip()
            if not nombre:
                logger.debug("[%s] skipping record id=%s: empty nombre", _SOURCE_NAME, record_id)
                return None
            if record_id is None:
                logger.debug("[%s] skipping record with no id: nombre=%r", _SOURCE_NAME, nombre)
                return None

            tipo: str = (raw.get("tipo") or "").strip()
            estado: str = (raw.get("estado") or "").strip()
            kind: str = _infer_kind(tipo, estado)

            # --- age ---
            age_val = raw.get("edad")
            age: Optional[int] = None
            if age_val is not None:
                try:
                    candidate = int(age_val)
                    if 0 < candidate < 130:
                        age = candidate
                except (TypeError, ValueError):
                    pass

            # --- last_seen_location ---
            # direccion is incident location (where person was last seen / reported),
            # not the reporter's home address -- safe to include in location field.
            # Not stored in raw_data to limit surface area (strip_pii also catches it).
            direccion: str = (raw.get("direccion") or "").strip()
            edificio: str = (raw.get("edificio") or "").strip()
            piso_raw = raw.get("piso")
            piso_str: str = str(piso_raw).strip() if piso_raw is not None else ""

            location_parts: list[str] = []
            if edificio:
                if piso_str and piso_str not in ("None", "null", "0"):
                    location_parts.append(f"{edificio}, Piso {piso_str}")
                else:
                    location_parts.append(edificio)
            if direccion:
                location_parts.append(direccion)

            if location_parts:
                location = ", ".join(location_parts) + f", {_DEFAULT_LOCATION}"
            else:
                # No address fields -- fall back to GPS if available, else region default.
                lat = raw.get("lat")
                lng = raw.get("lng")
                if lat is not None and lng is not None:
                    try:
                        location = (
                            f"{_DEFAULT_LOCATION} "
                            f"({float(lat):.5f}, {float(lng):.5f})"
                        )
                    except (TypeError, ValueError):
                        location = _DEFAULT_LOCATION
                else:
                    location = _DEFAULT_LOCATION

            # --- distinguishing_marks ---
            # Estado label is prepended before descripcion so that the 500-char
            # truncation (applied from the right) never silently drops the status
            # tag -- especially critical for estado=fallecido (constraint 2/3).
            descripcion: str = (raw.get("descripcion") or "").strip()
            estado_label: str = _ESTADO_LABEL.get(estado.lower(), "")

            marks_parts: list[str] = []
            if estado_label and estado_label.lower() not in descripcion.lower():
                marks_parts.append(f"Estado: {estado_label}")
            if descripcion:
                marks_parts.append(descripcion)

            marks: Optional[str] = " | ".join(marks_parts) if marks_parts else None
            if marks and len(marks) > 500:
                marks = marks[:497] + "..."

            # --- photo_url ---
            foto_url = (raw.get("foto_url") or "").strip() or None

            # --- raw_data (no PII: cedula, contacto_nombre, contacto_telefono excluded) ---
            # direccion is also excluded (strip_pii removes it; we keep it only in location).
            raw_data_base = {
                "id": record_id,
                "tipo": tipo,
                "estado": estado,
                "lat": raw.get("lat"),
                "lng": raw.get("lng"),
                "edificio": edificio or None,
                "piso": piso_str or None,
                "created_at": raw.get("created_at"),
            }
            # Remove None values to keep raw_data clean, then strip_pii for safety.
            raw_data: dict[str, Any] = strip_pii(
                {k: v for k, v in raw_data_base.items() if v is not None}
            )

            return {
                "kind": kind,
                "full_name": nombre,
                "age": age,
                "last_seen_location": location,
                "distinguishing_marks": marks,
                "clothing": None,
                "photo_url": foto_url,
                "source": _SOURCE_NAME,
                "source_url": f"sos_laguaira:{record_id}",
                "raw_data": raw_data,
            }

        except Exception as exc:
            logger.warning(
                "[%s] normalize error for record id=%s: %s",
                _SOURCE_NAME,
                raw.get("id"),
                exc,
            )
            return None
