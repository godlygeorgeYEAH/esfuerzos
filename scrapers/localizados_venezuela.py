"""
scrapers/localizados_venezuela.py -- Periodic scraper for Localizados Venezuela.

Source: https://localizadosvenezuela.com/api/v1/localizados
Public REST API listing persons confirmed at hospitals, shelters, and
other locations post-earthquake. Created by Giuseppe Gangi.

Only records "ya localizados" are published, so kind='found' for every row.
No cedula, no phone -- no PII to strip beyond what strip_pii covers.
No age field in the API response.

API reference: https://localizadosvenezuela.com/api
  GET /api/v1/localizados?page={n}&limit={size}
  Response: {"data": [...], "meta": {"page", "limit", "total", "totalPages"}}
  CORS: Access-Control-Allow-Origin: *
  No authentication required.

Pagination: pages are 1-indexed. Out-of-range pages return data=[].
With limit=100 the full dataset is ~45 pages (~4 500 records and growing).

Orchestrator registration (add to scraper_orchestrator.py _make_scrapers):
  from scrapers.localizados_venezuela import LocalizadosVenezuelaScraper
  "localizados_venezuela": LocalizadosVenezuelaScraper(sb_url, sb_key),
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_SOURCE_NAME = "localizados_venezuela"
_BASE_URL = "https://localizadosvenezuela.com/api/v1/localizados"
_PAGE_SIZE = 100

# The site sits behind Cloudflare and returns 403 to bare curl.
# A recognizable but non-deceptive User-Agent clears the WAF.
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ReuneVE/1.0; +https://reune.ve)",
    "Accept": "application/json",
    "Origin": "https://reune.ve",
}


class LocalizadosVenezuelaScraper(BaseScraper):
    """
    Periodic scraper for Localizados Venezuela.

    Fetches all published "localizados" records (persons confirmed at a
    hospital, shelter, or similar location) via the public REST API.
    kind='found' for every record -- the site explicitly rejects missing-person
    reports.

    Inherits run_poll() and run_full() from BaseScraper.
    fetch_page() uses httpx directly instead of the aiohttp _session_get()
    helper, matching the project-wide httpx preference.
    upsert_report() is overridden to enforce ignore-duplicates (constraint 6).
    """

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__(_SOURCE_NAME, supabase_url, supabase_key)
        # Cached after the first successful fetch; used to short-circuit
        # fetch_page() calls beyond the last known page.
        self._total_pages: int = 0

    # ------------------------------------------------------------------
    # Constraint 6 override: use ignore-duplicates (not the base-class
    # merge-duplicates default) for all inserts into 'reports'.
    # ------------------------------------------------------------------

    async def upsert_report(self, data: dict) -> bool:
        """
        POST a single report to Supabase with resolution=ignore-duplicates.
        Deduplication key: (source, source_url).
        Returns True on success, False on HTTP error.
        """
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
                self.source_name,
                resp.status_code,
                resp.text[:150],
            )
            return False

    # ------------------------------------------------------------------
    # BaseScraper abstract interface
    # ------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict]:
        """
        Fetch one page of records from the Localizados Venezuela API.

        Returns the list of raw record dicts, or [] when page > totalPages.
        Retries up to 3 times with exponential backoff (2 s, 4 s) before
        re-raising so BaseScraper.run_full() can increment stats['errors']
        and terminate cleanly instead of silently treating a mid-sweep failure
        as end-of-pages.

        Page ordering is not guaranteed to be chronological, so poll_recent
        (which only fetches page 1) is a best-effort catch of recent additions.
        Use full_sweep() for completeness.
        """
        # Short-circuit: don't request pages we already know don't exist.
        if self._total_pages and page > self._total_pages:
            return []

        params: dict = {"page": page, "limit": _PAGE_SIZE}

        for attempt in range(1, 4):  # 3 attempts with exponential backoff
            try:
                async with httpx.AsyncClient(timeout=30, headers=_HEADERS) as client:
                    resp = await client.get(_BASE_URL, params=params)
                    resp.raise_for_status()
                    payload = resp.json()

                records: list[dict] = payload.get("data", [])
                meta: dict = payload.get("meta", {})
                total_pages: int = meta.get("totalPages", 0)
                if total_pages:
                    self._total_pages = total_pages

                return records

            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[%s] fetch_page page=%d attempt %d HTTP error %s: %s",
                    self.source_name,
                    page,
                    attempt,
                    exc.response.status_code,
                    exc.response.text[:200],
                )
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

            except Exception as exc:
                logger.error(
                    "[%s] fetch_page page=%d attempt %d failed: %s",
                    self.source_name,
                    page,
                    attempt,
                    exc,
                    exc_info=True,
                )
                if attempt < 3:
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

        # Unreachable; loop always returns or raises.
        return []  # pragma: no cover

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Map a Localizados Venezuela API record to the 'reports' table schema.

        Field mapping:
          nombreCompleto -> full_name
          lugarNombre    -> last_seen_location (hospital / shelter name)
          direccion      -> appended to location when it adds context
          observaciones  -> distinguishing_marks (source notes)
          condicion      -> appended to marks when informative (not 'desconocido')
          slug           -> source_url suffix (unique per record, includes random suffix)

        age: not provided by the API.
        kind: always 'found' -- the site only lists confirmed-location persons.
        Deceased entries: condition is placed in distinguishing_marks, never in
        a boolean field (per system constraint 3).
        """
        nombre: str = (raw.get("nombreCompleto") or "").strip()
        if not nombre:
            return None

        slug: str = (raw.get("slug") or "").strip()
        if not slug:
            # Without a slug we cannot build a stable dedup key; skip the record.
            logger.debug("[%s] skipping record with no slug: %r", self.source_name, raw)
            return None

        # Location: named place (hospital/shelter) augmented by area of origin.
        lugar: str = (raw.get("lugarNombre") or "").strip()
        direccion: str = (raw.get("direccion") or "").strip()
        if lugar and direccion and direccion.lower() not in lugar.lower():
            location: Optional[str] = f"{lugar} ({direccion})"
        elif lugar:
            location = lugar
        elif direccion:
            location = direccion
        else:
            location = None

        # distinguishing_marks: concatenate observaciones + condicion when useful.
        observaciones: str = (raw.get("observaciones") or "").strip()
        condicion: str = (raw.get("condicion") or "").strip()

        marks_parts: list[str] = []
        if observaciones:
            marks_parts.append(observaciones)
        if condicion and condicion.lower() != "desconocido":
            marks_parts.append(f"Condicion: {condicion}")

        marks: Optional[str] = " | ".join(marks_parts) if marks_parts else None
        # Cap at 500 chars to match bulk_importer convention.
        if marks and len(marks) > 500:
            marks = marks[:497] + "..."

        return {
            "kind": "found",
            "full_name": nombre,
            "age": None,
            "last_seen_location": location,
            "distinguishing_marks": marks,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"localizados_venezuela:{slug}",
            # strip_pii removes 'direccion' and any other PII fields before storage.
            "raw_data": strip_pii(raw),
        }
