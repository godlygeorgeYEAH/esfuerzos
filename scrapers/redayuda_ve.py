"""
api/scrapers/redayuda_ve.py -- Periodic scraper + search source for redayuda_ve.

RedAyudaVenezuela.com is a Supabase-backed crisis platform built for
the Venezuela earthquake (Jun 2026). Its anon key is publicly embedded
in the client JS bundle (standard Supabase public-read pattern), so we
query its Supabase project via REST directly -- no HTML parsing needed.

Data model on their end (3 sub-tables ingested here):
  missing_persons   -- reported missing, primary person registry
  found_persons     -- people confirmed located
  hospital_reports  -- patients registered at hospitals

Search:
  search_people RPC -- cross-table search, params: {"query": <name>}

This class does two jobs:

  1. BaseVEScraper -- periodic ingestion into OUR 'reports' table.
       fetch_page(page) fetches all 3 sub-tables on page 1, empty on
       subsequent pages (data is small enough for a single bulk pull).
       normalize(raw) maps each tagged row to the 'reports' schema.
       poll_recent() and full_sweep() both delegate to fetch_page + normalize.

  2. BaseSearchSource -- per-query live search on each WhatsApp report intake.
       search_person() calls rpc/search_people with name variants.
       Falls back to internal ILIKE on our 'reports' table when the RPC
       returns empty or is unavailable.

Configuration:
  REDAYUDA_ANON_KEY -- anon (public read) key for the RedAyuda Supabase project.
                       Read from environment. Falls back to empty string if unset
                       (all API calls will then fail gracefully with 401).

Note: BeautifulSoup4 is not used here. The site exposes all data as JSON
via the Supabase REST interface.
"""
from __future__ import annotations

import logging
import os
import urllib.parse
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
# Our own Supabase instance (writes go here via BaseVEScraper.upsert_report)
# ---------------------------------------------------------------------------
_SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# ---------------------------------------------------------------------------
# RedAyudaVenezuela.com Supabase project (public anon key, read-only access)
# ---------------------------------------------------------------------------
_RDA_SUPABASE_URL = "https://cpavwkdonvkvrwygfzfo.supabase.co"
_RDA_ANON_KEY: str = os.environ.get("REDAYUDA_ANON_KEY", "")

_SOURCE_NAME = "redayuda_ve"

# Minimum fuzzy name score to include a result in search output.
_MIN_NAME_SCORE = 0.45

# Page 1 is the only page that returns data (fetch-all-at-once strategy).
_ONLY_PAGE = 1

# REST select columns per sub-table.
_SELECT_MISSING = "id,nombre,edad,ubicacion,estado,ciudad,descripcion,contacto,foto_url,created_at"
_SELECT_FOUND = "id,nombre,edad,ubicacion,estado,ciudad,descripcion,contacto,foto_url,created_at"
_SELECT_HOSPITAL = "id,nombre,edad,ubicacion,estado,ciudad,descripcion,contacto,foto_url,created_at"


class RedAyudaVEScraper(BaseVEScraper, BaseSearchSource):
    """
    Periodic scraper and per-query search source for redayudavenezuela.com.

    Dual inheritance:
      BaseVEScraper      -- scheduled ingestion into our 'reports' table
      BaseSearchSource   -- on-demand search fired per WhatsApp report intake

    Core interface:
      fetch_page(page)   -- returns tagged raw rows from all 3 sub-tables
      normalize(raw)     -- maps one tagged row to the 'reports' table schema
    """

    source_name = _SOURCE_NAME
    timeout_seconds = 8.0

    # -----------------------------------------------------------------------
    # HTTP helpers
    # -----------------------------------------------------------------------

    def _rda_headers(self) -> dict[str, str]:
        """Headers for the RedAyuda Supabase REST API (anon, read-only)."""
        key = _RDA_ANON_KEY
        return {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    # -----------------------------------------------------------------------
    # fetch_page / normalize -- core pagination + normalization abstraction
    # -----------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """
        Fetch raw records from the RedAyuda API.

        Strategy: fetch all 3 sub-tables on page 1 in a single batch.
        Return empty list for all subsequent pages (data is small enough
        that a single pull covers the full dataset without pagination).

        Each returned row is tagged with '_table_source' so that normalize()
        can route field mappings correctly:
          "missing"  -- from missing_persons
          "found"    -- from found_persons
          "hospital" -- from hospital_reports

        Returns an empty list on page != 1 or on any HTTP error.
        """
        if page != _ONLY_PAGE:
            return []

        results: list[dict[str, Any]] = []

        sub_tables: list[tuple[str, str, str]] = [
            ("missing_persons",  _SELECT_MISSING,  "missing"),
            ("found_persons",    _SELECT_FOUND,    "found"),
            ("hospital_reports", _SELECT_HOSPITAL, "hospital"),
        ]

        async with httpx.AsyncClient(timeout=30) as client:
            for table, select_cols, table_tag in sub_tables:
                url = f"{_RDA_SUPABASE_URL}/rest/v1/{table}"
                params = {"select": select_cols}
                try:
                    resp = await client.get(
                        url,
                        headers=self._rda_headers(),
                        params=params,
                    )
                    if resp.status_code != 200:
                        logger.warning(
                            "RedAyudaVEScraper.fetch_page: %s returned %d",
                            table,
                            resp.status_code,
                        )
                        continue
                    rows = resp.json() or []
                    for row in rows:
                        row["_table_source"] = table_tag
                    results.extend(rows)
                    logger.debug(
                        "RedAyudaVEScraper.fetch_page: %s -> %d rows",
                        table,
                        len(rows),
                    )
                except Exception as exc:
                    logger.warning(
                        "RedAyudaVEScraper.fetch_page: error fetching %s: %s",
                        table,
                        exc,
                    )

        return results

    @staticmethod
    def normalize(raw: dict[str, Any]) -> dict[str, Any]:
        """
        Map a tagged raw row from any sub-table to our 'reports' table schema.

        Routing by _table_source:
          "missing"  -> kind=missing, source_url=redayuda_missing:<id>
          "found"    -> kind=found,   source_url=redayuda_found:<id>
          "hospital" -> kind=found,   source_url=redayuda_hospital:<id>

        Field mapping (RedAyuda uses Spanish column names):
          nombre     -> full_name
          edad       -> age (int or None)
          ubicacion / estado / ciudad -> last_seen_location (first non-empty wins)
          descripcion -> distinguishing_marks
          contacto   -> contact (not stored in 'reports' schema but kept in raw_data)
          foto_url   -> not in 'reports' schema; kept in raw_data
        """
        table_source = raw.get("_table_source", "missing")
        record_id = raw.get("id") or ""

        if table_source == "missing":
            kind = "missing"
            source_url = f"redayuda_missing:{record_id}"
        elif table_source == "found":
            kind = "found"
            source_url = f"redayuda_found:{record_id}"
        else:
            # "hospital" and any unknown tag
            kind = "found"
            source_url = f"redayuda_hospital:{record_id}"

        full_name = (raw.get("nombre") or "").strip() or "Desconocido"

        last_seen_location = (
            raw.get("ubicacion")
            or raw.get("estado")
            or raw.get("ciudad")
            or None
        )
        if last_seen_location:
            last_seen_location = last_seen_location.strip() or None

        age_raw = raw.get("edad")
        try:
            age = int(age_raw) if age_raw is not None else None
        except (TypeError, ValueError):
            age = None

        return {
            "kind": kind,
            "full_name": full_name,
            "age": age,
            "last_seen_location": last_seen_location,
            "distinguishing_marks": (raw.get("descripcion") or "").strip() or None,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": source_url,
            "raw_data": strip_pii(raw),
        }

    # -----------------------------------------------------------------------
    # BaseVEScraper -- periodic ingestion
    # -----------------------------------------------------------------------

    async def poll_recent(self) -> int:
        """
        Light ingestion pass. Delegates to fetch_page(1) + normalize.

        Because fetch_page(1) always returns the full current dataset
        (RedAyuda has no updated_at ordering for a delta query), this
        is equivalent to a full sync on every call. Supabase upsert on
        (source, source_url) means duplicate rows are merged, not doubled.

        Returns count of rows upserted.
        """
        rows_upserted = 0
        error_msg: str | None = None
        try:
            records = await self.fetch_page(_ONLY_PAGE)
            for raw in records:
                normalized = self.normalize(raw)
                if normalized["full_name"] == "Desconocido" and not raw.get("nombre"):
                    continue
                await self.upsert_report(normalized)
                rows_upserted += 1
        except Exception as exc:
            error_msg = str(exc)
            logger.error("RedAyudaVEScraper.poll_recent failed: %s", exc)
        finally:
            await self.log_run(_SOURCE_NAME, "poll_recent", rows_upserted, 0, error_msg)
        logger.info("RedAyudaVEScraper.poll_recent: upserted %d rows", rows_upserted)
        return rows_upserted

    async def full_sweep(self) -> int:
        """
        Full ingestion sweep. Iterates pages until fetch_page returns empty.

        fetch_page returns data only on page 1 and empty on all subsequent
        pages, so this loop effectively runs exactly one fetch. The loop
        structure is kept for forward compatibility if fetch_page is later
        extended to support true pagination.

        Returns count of rows upserted.
        """
        rows_upserted = 0
        page = _ONLY_PAGE
        error_msg: str | None = None
        try:
            while True:
                records = await self.fetch_page(page)
                if not records:
                    break
                for raw in records:
                    normalized = self.normalize(raw)
                    if normalized["full_name"] == "Desconocido" and not raw.get("nombre"):
                        continue
                    await self.upsert_report(normalized)
                    rows_upserted += 1
                logger.debug(
                    "RedAyudaVEScraper.full_sweep: page %d, %d records, total=%d",
                    page,
                    len(records),
                    rows_upserted,
                )
                page += 1
        except Exception as exc:
            error_msg = str(exc)
            logger.error("RedAyudaVEScraper.full_sweep failed on page %d: %s", page, exc)
        finally:
            await self.log_run(_SOURCE_NAME, "full_sweep", rows_upserted, 0, error_msg)
        logger.info(
            "RedAyudaVEScraper.full_sweep: upserted %d rows across %d page(s)",
            rows_upserted,
            page - _ONLY_PAGE + 1,
        )
        return rows_upserted

    # -----------------------------------------------------------------------
    # BaseSearchSource -- per-query live search
    # -----------------------------------------------------------------------

    async def search_person(self, query: SearchQuery) -> list[SearchResult]:
        """
        Per-query search fired on every incoming WhatsApp reunion report.

        Strategy (in order):
          1. POST rpc/search_people with {"query": <name_variant>}.
             Tries each name variant and stops on first non-empty response.
          2. Internal ILIKE fallback on our 'reports' table filtered by
             source='redayuda_ve'. Covers records already ingested by
             poll_recent when the RPC is down or returns empty.

        query.full_name must be at least 3 characters.
        Returns results sorted by composite score (name 70%, age 15%, loc 15%).
        """
        if len((query.full_name or "").strip()) < 3:
            logger.debug("RedAyudaVEScraper.search_person: name too short, skipping")
            return []

        live_results = await self._rpc_search(query)
        if live_results:
            return live_results

        return await self._internal_search(query)

    async def _rpc_search(self, query: SearchQuery) -> list[SearchResult]:
        """
        Call rpc/search_people on RedAyuda's Supabase.

        The RPC accepts {"query": <name>} and returns cross-table results.
        Tries name variants in order; stops after the first that returns data.
        Deduplicates by lowercased full name to avoid showing the same person
        twice from different variant queries.

        Expected response fields (best-effort, some may be null):
          nombre / name / full_name  -- person name
          edad / age                 -- int or null
          ubicacion / loc / last_seen -- location string
          descripcion / detail / description -- description
          contacto / contact         -- phone or null
          foto_url / photo_url       -- image URL or null
          id                         -- record UUID
          categoria / category       -- "missing" | "found" | "hospital" etc.
        """
        rpc_url = f"{_RDA_SUPABASE_URL}/rest/v1/rpc/search_people"
        headers = self._rda_headers()
        results: list[SearchResult] = []
        seen_names: set[str] = set()

        variants = name_variants(query.full_name)
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                for variant in variants:
                    try:
                        resp = await client.post(
                            rpc_url,
                            headers=headers,
                            json={"query": variant},
                        )
                        if resp.status_code != 200:
                            logger.warning(
                                "RedAyudaVEScraper._rpc_search: status %d for variant '%s'",
                                resp.status_code,
                                variant,
                            )
                            continue
                        records = resp.json() or []
                        if not records:
                            continue
                        for rec in records:
                            result = self._rpc_to_result(rec, query)
                            if result is None:
                                continue
                            dedup_key = result.full_name.lower().strip()
                            if dedup_key in seen_names:
                                continue
                            seen_names.add(dedup_key)
                            results.append(result)
                        if results:
                            break
                    except httpx.TimeoutException:
                        logger.warning(
                            "RedAyudaVEScraper._rpc_search: timeout on variant '%s'", variant
                        )
                        break
                    except Exception as exc:
                        logger.warning(
                            "RedAyudaVEScraper._rpc_search: error on variant '%s': %s", variant, exc
                        )
        except Exception as exc:
            logger.error("RedAyudaVEScraper._rpc_search: client error: %s", exc)

        results.sort(key=lambda r: r.score, reverse=True)
        return results

    def _rpc_to_result(
        self, rec: dict[str, Any], query: SearchQuery
    ) -> SearchResult | None:
        """
        Convert a single rpc/search_people row to a SearchResult.

        Tries multiple field name candidates because the RPC may vary by
        category (missing uses 'nombre', hospital may use 'nombre' or 'name').
        Returns None if no usable name is found or name_similarity is below
        the minimum threshold.
        """
        raw_name = (
            rec.get("nombre")
            or rec.get("name")
            or rec.get("full_name")
            or ""
        ).strip()
        if not raw_name:
            return None

        ns = name_similarity(query.full_name, raw_name)
        if ns < _MIN_NAME_SCORE:
            return None

        category = (
            rec.get("categoria")
            or rec.get("category")
            or "missing"
        ).lower()

        if category in ("found", "encontrado", "localizado", "safe", "bien"):
            kind = "found"
        elif category in ("hospital", "ingresado", "hospitalized", "paciente"):
            kind = "found"
        else:
            kind = "missing"

        age_raw = rec.get("edad") or rec.get("age")
        try:
            age_int = int(age_raw) if age_raw is not None else None
        except (TypeError, ValueError):
            age_int = None

        age_s = age_match_score(query.age, age_int)

        location = (
            rec.get("ubicacion")
            or rec.get("estado")
            or rec.get("ciudad")
            or rec.get("loc")
            or rec.get("last_seen")
            or ""
        ).strip() or None
        loc_s = location_match_score(query.last_seen_location, location)

        score = composite_score(ns, age_s, loc_s)

        record_id = rec.get("id") or ""
        if record_id:
            # Use the same identifier format as normalize() for consistency.
            source_url = f"redayuda_{category}:{record_id}"
        else:
            q_param = urllib.parse.quote(raw_name)
            source_url = f"https://redayudavenezuela.com/buscar?q={q_param}"

        detail_parts: list[str] = []
        desc = (
            rec.get("descripcion")
            or rec.get("detail")
            or rec.get("description")
            or ""
        ).strip()
        if desc:
            detail_parts.append(desc)
        label = (rec.get("label") or rec.get("etiqueta") or "").strip()
        if label:
            detail_parts.append(label)
        detail = " | ".join(detail_parts) or None

        return SearchResult(
            source=self.source_name,
            full_name=raw_name,
            score=round(score, 3),
            name_similarity=round(ns, 3),
            location=location,
            age=age_int,
            detail=detail,
            contact=(rec.get("contacto") or rec.get("contact") or None),
            photo_url=(rec.get("foto_url") or rec.get("photo_url") or None),
            source_url=source_url,
            kind=kind,
            raw=rec,
        )

    async def _internal_search(self, query: SearchQuery) -> list[SearchResult]:
        """
        ILIKE fallback on our own 'reports' table filtered by source='redayuda_ve'.
        Covers records already ingested by poll_recent when the RPC is unavailable
        or returns empty.
        """
        safe_name = query.full_name.replace("%", "").replace("_", " ").strip()
        tokens = safe_name.split()
        search_token = tokens[-1] if len(tokens) >= 2 else safe_name

        params = {
            "full_name": f"ilike.*{search_token}*",
            "source": f"eq.{_SOURCE_NAME}",
            "select": "id,kind,full_name,age,last_seen_location,distinguishing_marks,source_url",
            "order": "created_at.desc",
            "limit": "20",
        }
        headers = {
            "apikey": _SUPABASE_KEY,
            "Authorization": f"Bearer {_SUPABASE_KEY}",
        }
        results: list[SearchResult] = []
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                resp = await client.get(
                    f"{_SUPABASE_URL}/rest/v1/reports",
                    headers=headers,
                    params=params,
                )
                if resp.status_code != 200:
                    logger.warning(
                        "RedAyudaVEScraper._internal_search: status %d", resp.status_code
                    )
                    return []
                for rec in resp.json() or []:
                    result_name = (rec.get("full_name") or "").strip()
                    if not result_name:
                        continue
                    ns = name_similarity(query.full_name, result_name)
                    if ns < _MIN_NAME_SCORE:
                        continue
                    age_s = age_match_score(query.age, rec.get("age"))
                    loc_s = location_match_score(
                        query.last_seen_location,
                        rec.get("last_seen_location"),
                    )
                    score = composite_score(ns, age_s, loc_s)
                    results.append(SearchResult(
                        source=self.source_name,
                        full_name=result_name,
                        score=round(score, 3),
                        name_similarity=round(ns, 3),
                        location=rec.get("last_seen_location"),
                        age=rec.get("age"),
                        detail=rec.get("distinguishing_marks") or None,
                        source_url=rec.get("source_url", "https://redayudavenezuela.com"),
                        kind=rec.get("kind"),
                        raw=rec,
                    ))
        except Exception as exc:
            logger.error("RedAyudaVEScraper._internal_search failed: %s", exc)

        results.sort(key=lambda r: r.score, reverse=True)
        return results
