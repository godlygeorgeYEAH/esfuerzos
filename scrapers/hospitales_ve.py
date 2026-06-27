"""
api/scrapers/hospitales_ve.py -- Scraper and search source for hospitalesenvenezuela.com

Source: Crowdsourced hospital patient registry built for the Venezuela M7.2/M7.5
earthquake (Jun 24 2026). ~18,856 patients across 150 hospitals at time of design.

Two classes are exported:

  HospitalesVEScraper(BaseVEScraper)
    Periodic ingestion into OUR 'reports' table.
    Constructor takes explicit (supabase_url, supabase_key) for OUR Supabase.
    External credentials are read from environment variables:
      HOSPITALES_EXT_URL  -- defaults to the hardcoded project URL below
      HOSPITALES_EXT_KEY  -- anon key; source is disabled if empty
    fetch_page(page)      -- paginate /rest/v1/pacientes, 100 records per page;
                            falls back to /personas and /patients on 404/403
    normalize(raw)        -- map raw record to our 'reports' schema
    search(query)         -- POST /rpc/buscar_paciente, returns raw dicts
    poll_recent()         -- page 1 (100 most recent) -> upsert
    full_sweep()          -- paginate until empty page

  HospitalesVESource(BaseSearchSource)
    Per-query live search registered with BaseSearchSource._registry.
    Fires on every WhatsApp report intake via the search orchestrator.
    Uses the same external Supabase project.
    Kept separate from HospitalesVEScraper so it can be instantiated with
    no args (required by BaseSearchSource.build_sources()).

Field mapping (normalize):
  kind              = "found" (hospital admission = found/located)
  full_name         = nombre | name | "Desconocido"
  age               = edad | age
  last_seen_location = hospital | ubicacion | ciudad
  source            = "hospitales_ve"
  source_url        = "hospitales_ve:{id}"

Deduplication: conflicts on (source, source_url) in our 'reports' table are
merged (Supabase upsert). Bot message must show all hits because one person
can appear in multiple hospital registries.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx

from .base import (
    BaseSearchSource,
    BaseVEScraper,
    SearchQuery,
    SearchResult,
    age_match_score,
    composite_score,
    location_match_score,
    name_similarity,
    name_variants,
    strip_pii,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# External Supabase project for hospitalesenvenezuela.com
# The anon key is publicly embedded in the site HTML by design (emergency data).
# ---------------------------------------------------------------------------
_EXT_URL_DEFAULT = "https://ozuxfepfkvnxkywdsqxy.supabase.co"
_EXT_KEY_DEFAULT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im96dXhmZXBma3ZueGt5d2RzcXh5Iiwicm9sZSI6Im"
    "Fub24iLCJpYXQiOjE3ODI0MjI5NTEsImV4cCI6MjA5Nzk5ODk1MX0"
    ".YhW0GalGkQZdO2NJTg_01C5XhdMmJ6RbNSNXXC0xG4o"
)

_SOURCE_NAME = "hospitales_ve"
_PAGE_SIZE = 100
# Table names to try in order when the primary table name is unknown/missing.
_TABLE_CANDIDATES = ["pacientes", "personas", "patients"]

# Regex for parsing age and neighborhood out of the free-text 'detalle' field.
# Handles: '7 anos - Caribe', '74 anos Petare', '55 | Politrauma', '62 - Trauma'
_AGE_RE = re.compile(r"(\d{1,3})\s*a[nn]o", re.IGNORECASE)
_AGE_PLAIN_RE = re.compile(r"^(\d{1,3})\s*[|\-]")
_LOCATION_RE = re.compile(r"[\-|]\s*(.+)$")


# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------

def _parse_age_from_detalle(detalle: str | None) -> int | None:
    if not detalle:
        return None
    m = _AGE_RE.search(detalle)
    if m:
        age = int(m.group(1))
        return age if 0 < age < 120 else None
    m2 = _AGE_PLAIN_RE.match(detalle.strip())
    if m2:
        age = int(m2.group(1))
        return age if 0 < age < 120 else None
    return None


def _parse_neighborhood_from_detalle(detalle: str | None) -> str | None:
    if not detalle:
        return None
    m = _LOCATION_RE.search(detalle)
    return m.group(1).strip() if m else None


# ---------------------------------------------------------------------------
# HospitalesVEScraper -- periodic ingestion (BaseVEScraper)
# ---------------------------------------------------------------------------

class HospitalesVEScraper(BaseVEScraper):
    """
    Periodic scraper for hospitalesenvenezuela.com.

    Reads external patient data via Supabase REST, normalizes it, and upserts
    into OUR 'reports' table.

    Usage:
        scraper = HospitalesVEScraper(supabase_url, supabase_service_key)
        await scraper.poll_recent()   # lightweight, 100 records
        await scraper.full_sweep()    # full paginated crawl
        results = await scraper.search("Jose Rodriguez")  # raw dicts
    """

    source_name = _SOURCE_NAME

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        """
        supabase_url  -- OUR Supabase project URL (used for upsert/log_run)
        supabase_key  -- OUR Supabase service role key
        External (hospitalesenvenezuela.com) credentials are read from env:
          HOSPITALES_EXT_URL  -- defaults to the hardcoded project URL
          HOSPITALES_EXT_KEY  -- anon key; if empty, fetch_page returns []
        """
        self._supabase_url = supabase_url
        self._supabase_key = supabase_key
        self._ext_url: str = os.environ.get("HOSPITALES_EXT_URL", _EXT_URL_DEFAULT)
        self._ext_key: str = os.environ.get("HOSPITALES_EXT_KEY", "")

    # -----------------------------------------------------------------------
    # HTTP header helpers
    # -----------------------------------------------------------------------

    def _sb_headers(
        self, prefer: str = "resolution=merge-duplicates,return=minimal"
    ) -> dict[str, str]:
        """Headers for OUR Supabase (uses instance credentials)."""
        return {
            "apikey": self._supabase_key,
            "Authorization": f"Bearer {self._supabase_key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }

    def _ext_headers(self) -> dict[str, str]:
        """Headers for the external hospitalesenvenezuela.com Supabase."""
        return {
            "apikey": self._ext_key,
            "Authorization": f"Bearer {self._ext_key}",
            "Content-Type": "application/json",
        }

    # -----------------------------------------------------------------------
    # upsert_report / log_run overrides to use instance URL
    # BaseVEScraper uses module-level _SUPABASE_URL; we need instance variables.
    # -----------------------------------------------------------------------

    async def upsert_report(self, data: dict[str, Any]) -> None:
        """
        Write a normalized record to OUR 'reports' table.
        Conflicts on (source, source_url) are merged (update existing row).
        """
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{self._supabase_url}/rest/v1/reports",
                headers=self._sb_headers(),
                params={"on_conflict": "source,source_url"},
                json=[data],
            )
            resp.raise_for_status()

    async def log_run(
        self,
        source: str,
        run_type: str,
        rows_inserted: int,
        rows_updated: int,
        error: str | None = None,
    ) -> None:
        """Write a run log entry to OUR 'scraper_runs' table."""
        row = {
            "source": source,
            "run_type": run_type,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "error": error,
        }
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.post(
                    f"{self._supabase_url}/rest/v1/scraper_runs",
                    headers=self._sb_headers("return=minimal"),
                    json=[row],
                )
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("HospitalesVEScraper.log_run failed: %s", exc)

    # -----------------------------------------------------------------------
    # Core primitives
    # -----------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """
        Fetch one page of 100 records from the external Supabase project.

        page=1 returns offset 0-99, page=2 returns 100-199, etc.

        Strategy:
          1. If self._ext_key is empty: log warning and return [].
          2. Try GET /rest/v1/pacientes?select=*&limit=100&offset={(page-1)*100}.
          3. On 404 or 403: try /rest/v1/personas, then /rest/v1/patients.
          4. On any other non-200: log warning and return [].

        Returns list of raw record dicts, or [] on failure.
        """
        if not self._ext_key:
            logger.warning(
                "HospitalesVEScraper.fetch_page: HOSPITALES_EXT_KEY is not set; "
                "cannot fetch external data"
            )
            return []

        offset = (page - 1) * _PAGE_SIZE
        headers = self._ext_headers()
        params = {
            "select": "*",
            "limit": str(_PAGE_SIZE),
            "offset": str(offset),
        }

        async with httpx.AsyncClient(timeout=30) as client:
            for table in _TABLE_CANDIDATES:
                url = f"{self._ext_url}/rest/v1/{table}"
                try:
                    resp = await client.get(url, headers=headers, params=params)
                    if resp.status_code == 200:
                        records = resp.json()
                        return records if isinstance(records, list) else []
                    if resp.status_code in (403, 404):
                        logger.debug(
                            "HospitalesVEScraper.fetch_page: table '%s' returned HTTP %d, "
                            "trying next candidate",
                            table,
                            resp.status_code,
                        )
                        continue
                    logger.warning(
                        "HospitalesVEScraper.fetch_page: unexpected HTTP %d for table '%s'",
                        resp.status_code,
                        table,
                    )
                    return []
                except httpx.TimeoutException:
                    logger.warning(
                        "HospitalesVEScraper.fetch_page: timeout on table '%s'", table
                    )
                    return []
                except Exception as exc:
                    logger.warning(
                        "HospitalesVEScraper.fetch_page: error on table '%s': %s", table, exc
                    )
                    return []

        # All table candidates exhausted.
        logger.warning("HospitalesVEScraper.fetch_page: all table candidates failed")
        return []

    def normalize(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Map a raw external record to our 'reports' table schema.

        Field mapping:
          kind              = "found" (present in hospital = located)
          full_name         = nombre | name | "Desconocido"
          age               = edad | age (int or None)
          last_seen_location = hospital | ubicacion | ciudad
          source            = "hospitales_ve"
          source_url        = "hospitales_ve:{id}"  (conflict key for upsert)
        """
        record_id = raw.get("id", "")
        full_name = (
            raw.get("nombre")
            or raw.get("name")
            or "Desconocido"
        )
        age = raw.get("edad") or raw.get("age")
        last_seen_location = (
            raw.get("hospital")
            or raw.get("ubicacion")
            or raw.get("ciudad")
        )
        return {
            "kind": "found",
            "full_name": full_name,
            "age": age,
            "last_seen_location": last_seen_location,
            "distinguishing_marks": None,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"hospitales_ve:{record_id}",
            "raw_data": strip_pii(raw),
        }

    async def search(self, query: str) -> list[dict[str, Any]]:
        """
        POST /rest/v1/rpc/buscar_paciente with {"nombre": query}.
        Returns raw record dicts from the external Supabase project.
        Returns [] if ext_key is empty or on HTTP/network error.
        """
        if not self._ext_key:
            logger.warning(
                "HospitalesVEScraper.search: HOSPITALES_EXT_KEY is not set"
            )
            return []

        url = f"{self._ext_url}/rest/v1/rpc/buscar_paciente"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    url,
                    headers=self._ext_headers(),
                    json={"nombre": query},
                )
                if resp.status_code == 200:
                    records = resp.json()
                    return records if isinstance(records, list) else []
                logger.warning(
                    "HospitalesVEScraper.search: HTTP %d for query '%s'",
                    resp.status_code,
                    query,
                )
        except httpx.TimeoutException:
            logger.warning(
                "HospitalesVEScraper.search: timeout for query '%s'", query
            )
        except Exception as exc:
            logger.error("HospitalesVEScraper.search failed: %s", exc)

        return []

    # -----------------------------------------------------------------------
    # BaseVEScraper contract
    # -----------------------------------------------------------------------

    async def poll_recent(self) -> int:
        """
        Fetch the most recent 100 records (page 1) and upsert into our table.
        Runs every ~5 minutes. Returns count of rows upserted.
        """
        rows_upserted = 0
        error_msg: str | None = None
        try:
            records = await self.fetch_page(1)
            for raw in records:
                name = raw.get("nombre") or raw.get("name")
                if not name or not str(name).strip():
                    continue
                await self.upsert_report(self.normalize(raw))
                rows_upserted += 1
        except Exception as exc:
            error_msg = str(exc)
            logger.error("HospitalesVEScraper.poll_recent failed: %s", exc)
        finally:
            await self.log_run(_SOURCE_NAME, "poll_recent", rows_upserted, 0, error_msg)
        logger.info("HospitalesVEScraper.poll_recent: upserted %d rows", rows_upserted)
        return rows_upserted

    async def full_sweep(self) -> int:
        """
        Full paginated crawl of the external source.
        Stops when a page returns fewer than 100 records.
        Returns total count of rows upserted.
        """
        rows_upserted = 0
        page = 1
        error_msg: str | None = None
        try:
            while True:
                records = await self.fetch_page(page)
                if not records:
                    break
                for raw in records:
                    name = raw.get("nombre") or raw.get("name")
                    if not name or not str(name).strip():
                        continue
                    await self.upsert_report(self.normalize(raw))
                    rows_upserted += 1
                logger.debug(
                    "HospitalesVEScraper.full_sweep: page %d, %d records, total=%d",
                    page,
                    len(records),
                    rows_upserted,
                )
                if len(records) < _PAGE_SIZE:
                    break
                page += 1
        except Exception as exc:
            error_msg = str(exc)
            logger.error(
                "HospitalesVEScraper.full_sweep failed on page %d: %s", page, exc
            )
        finally:
            await self.log_run(_SOURCE_NAME, "full_sweep", rows_upserted, 0, error_msg)
        logger.info(
            "HospitalesVEScraper.full_sweep: upserted %d rows across %d pages",
            rows_upserted,
            page,
        )
        return rows_upserted


# ---------------------------------------------------------------------------
# HospitalesVESource -- per-query search source (BaseSearchSource)
# Registered automatically via __init_subclass__ when this module is imported.
# Instantiated with no args by BaseSearchSource.build_sources().
# ---------------------------------------------------------------------------

class HospitalesVESource(BaseSearchSource):
    """
    Per-query search against hospitalesenvenezuela.com.
    Fires on every incoming WhatsApp reunion report via the search orchestrator.

    RPC: POST /rest/v1/rpc/buscar_paciente {"p_term": "<name>"}
    Tries up to 4 name variants (full name, reversed, surname, given name)
    and stops on the first non-empty response.

    Deduplication: by (nombre.lower, location.lower[:60]).
    Bot message must show all location hits; a person can appear at multiple
    hospitals due to crowdsourced duplicate entries.
    """

    source_name = _SOURCE_NAME
    timeout_seconds = 7.0

    def __init__(self) -> None:
        # Use env var if set; fall back to the public anon key embedded in HTML.
        self._ext_url = os.environ.get("HOSPITALES_EXT_URL", _EXT_URL_DEFAULT)
        self._ext_key = os.environ.get("HOSPITALES_EXT_KEY", _EXT_KEY_DEFAULT)

    def _ext_headers(self) -> dict[str, str]:
        return {
            "apikey": self._ext_key,
            "Authorization": f"Bearer {self._ext_key}",
            "Content-Type": "application/json",
        }

    async def search_person(self, query: SearchQuery) -> list[SearchResult]:
        results: list[SearchResult] = []
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                for variant in name_variants(query.full_name):
                    records = await self._call_rpc(client, variant)
                    if records is None:
                        # HTTP error on this variant; try next.
                        continue
                    if not records:
                        # Valid response, zero results; try next variant.
                        continue

                    for rec in records:
                        nombre = (rec.get("nombre") or "").strip()
                        if not nombre:
                            continue
                        detalle = rec.get("detalle")
                        centro = (rec.get("centro") or "").strip()
                        ciudad = (rec.get("ciudad") or "").strip()

                        result_age = _parse_age_from_detalle(detalle)
                        neighborhood = _parse_neighborhood_from_detalle(detalle)

                        loc_parts = [p for p in [neighborhood, centro, ciudad] if p]
                        location = " | ".join(loc_parts) if loc_parts else None

                        ns = name_similarity(query.full_name, nombre)
                        age_s = age_match_score(query.age, result_age)
                        loc_s = location_match_score(query.last_seen_location, location)
                        score = composite_score(ns, age_s, loc_s)

                        results.append(SearchResult(
                            source=self.source_name,
                            full_name=nombre,
                            score=round(score, 3),
                            name_similarity=round(ns, 3),
                            location=location,
                            age=result_age,
                            detail=detalle,
                            contact=rec.get("telefono"),
                            source_url="https://hospitalesenvenezuela.com",
                            kind="hospital_patient",
                            raw=rec,
                        ))

                    # Got results for this variant; stop.
                    break

        except Exception as exc:
            logger.error("HospitalesVESource.search_person failed: %s", exc)

        return self._dedup_and_sort(results)

    async def _call_rpc(
        self,
        client: httpx.AsyncClient,
        term: str,
    ) -> list[dict] | None:
        """
        POST /rest/v1/rpc/buscar_paciente with {p_term: term}.
        Returns list of records on 200, None on HTTP error.
        """
        try:
            resp = await client.post(
                f"{self._ext_url}/rest/v1/rpc/buscar_paciente",
                headers=self._ext_headers(),
                json={"p_term": term},
            )
            if resp.status_code == 200:
                return resp.json() or []
            logger.warning(
                "HospitalesVESource._call_rpc: HTTP %s for term '%s'",
                resp.status_code,
                term,
            )
            return None
        except httpx.TimeoutException:
            logger.warning(
                "HospitalesVESource._call_rpc: timeout for term '%s'", term
            )
            return None

    @staticmethod
    def _dedup_and_sort(results: list[SearchResult]) -> list[SearchResult]:
        """
        Sort by score desc. Deduplicate by (nombre.lower, location.lower[:60]).
        Duplicates are common (same patient registered by multiple volunteers).
        """
        seen: set[tuple[str, str]] = set()
        deduped: list[SearchResult] = []
        for r in sorted(results, key=lambda x: x.score, reverse=True):
            key = (r.full_name.lower(), (r.location or "")[:60].lower())
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        return deduped
