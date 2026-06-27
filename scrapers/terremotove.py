"""
api/scrapers/terremotove.py -- Periodic scraper for terremotove/KoboToolbox earthquake reports.

Ingests earthquake event and damage reports from the KoboToolbox public form
(asset a8XWDsdUcpBzXGtgQmiiro). No authentication required.

Data represents damage and event locations, NOT missing persons.
kind is set to 'found' (as in ubicacion de evento).

Pagination: limit=100 per page, start=(page-1)*100.
Stop condition: page returns fewer than 100 records.
"""
from __future__ import annotations

import logging

import httpx

from .base import BaseVEScraper

logger = logging.getLogger(__name__)

_SOURCE_NAME = "terremotove"
_KOBO_API_URL = (
    "https://kf.kobotoolbox.org/api/v2/assets/a8XWDsdUcpBzXGtgQmiiro/data/"
)
_PAGE_SIZE = 100


class TerremotoVEScraper(BaseVEScraper):
    """
    Periodic scraper for terremotove earthquake damage/event reports via KoboToolbox.

    Source: KoboToolbox public form asset a8XWDsdUcpBzXGtgQmiiro.
    No API key required.
    Records are normalized as event locations (kind='found'), not missing persons.
    """

    source_name = _SOURCE_NAME

    @staticmethod
    def _parse_location(raw: dict) -> str | None:
        """
        Extract a location string from the raw KoboToolbox record.
        Prefers 'Ubicacion' (lat/lon string from the form field).
        Falls back to '_geolocation' (list [lat, lon] from KoboToolbox metadata).
        """
        ubicacion = raw.get("Ubicacion")
        if ubicacion and str(ubicacion).strip():
            return str(ubicacion).strip()

        geo = raw.get("_geolocation")
        if geo:
            if isinstance(geo, (list, tuple)) and len(geo) >= 2:
                lat, lon = geo[0], geo[1]
                if lat is not None and lon is not None:
                    return f"{lat},{lon}"
            if isinstance(geo, str) and geo.strip():
                return geo.strip()

        return None

    @staticmethod
    def _normalize(raw: dict) -> dict:
        """
        Map KoboToolbox response fields to the 'reports' table schema.

        kind='found' indicates this is an event/damage location record.
        full_name carries the event type prefixed with 'EVENTO:' so it is
        distinguishable from missing-person records in shared result lists.
        """
        record_id = raw.get("_id", "")
        return {
            "kind": "found",
            "full_name": f"EVENTO: {raw.get('Evento', 'Sin tipo')}",
            "age": None,
            "last_seen_location": TerremotoVEScraper._parse_location(raw),
            "distinguishing_marks": raw.get("Descripcion_del_evento", ""),
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"terremotove:{record_id}",
            "raw_data": {k: v for k, v in raw.items() if k.lower() not in ("_id", "_uuid", "_submission_time", "_submitted_by", "meta")},
        }

    async def fetch_page(self, page: int) -> list[dict]:
        """
        Fetch one page of records from KoboToolbox.

        page=1 fetches records 0-99 (start=0).
        page=2 fetches records 100-199 (start=100).
        Returns the list of result dicts, or an empty list on error.
        """
        offset = (page - 1) * _PAGE_SIZE
        params: dict = {
            "format": "json",
            "limit": _PAGE_SIZE,
            "start": offset,
        }
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(_KOBO_API_URL, params=params)
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", [])
        except Exception as exc:
            logger.error(
                "TerremotoVEScraper.fetch_page page=%d failed: %s", page, exc
            )
            return []

    async def poll_recent(self) -> int:
        """
        Fetch the most recent page of records (page 1, records 0-99).
        Lightweight; intended to run every few minutes.
        Returns count of rows upserted.
        """
        count = 0
        error: str | None = None
        try:
            records = await self.fetch_page(1)
            for raw in records:
                await self.upsert_report(self._normalize(raw))
                count += 1
        except Exception as exc:
            error = str(exc)
            logger.error("TerremotoVEScraper.poll_recent error: %s", exc)
        await self.log_run(_SOURCE_NAME, "poll_recent", count, 0, error)
        return count

    async def full_sweep(self) -> int:
        """
        Paginate the full KoboToolbox dataset until a page returns fewer than
        _PAGE_SIZE records. May take several minutes on large datasets.
        Returns total count of rows upserted.
        """
        total = 0
        error: str | None = None
        page = 1
        try:
            while True:
                records = await self.fetch_page(page)
                if not records:
                    break
                for raw in records:
                    await self.upsert_report(self._normalize(raw))
                    total += 1
                logger.info(
                    "TerremotoVEScraper.full_sweep page=%d rows=%d total=%d",
                    page,
                    len(records),
                    total,
                )
                if len(records) < _PAGE_SIZE:
                    break
                page += 1
        except Exception as exc:
            error = str(exc)
            logger.error("TerremotoVEScraper.full_sweep error: %s", exc)
        await self.log_run(_SOURCE_NAME, "full_sweep", total, 0, error)
        return total
