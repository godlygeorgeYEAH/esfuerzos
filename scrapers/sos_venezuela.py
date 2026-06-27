"""
api/scrapers/sos_venezuela.py -- Scraper for SOS Venezuela 2026
https://sosvenezuela2026.com/api/persons/list
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_BASE_URL = "https://sosvenezuela2026.com/api/persons/list"
_PAGE_SIZE = 100


class SosVenezuelaScraper(BaseScraper):
    """Pulls missing/found persons from the SOS Venezuela 2026 API."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__("sos_venezuela", supabase_url, supabase_key)

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """GET /api/persons/list?offset={n}&limit=100 with 3 retries."""
        offset = (page - 1) * _PAGE_SIZE
        url = f"{_BASE_URL}?offset={offset}&limit={_PAGE_SIZE}"
        session = await self._session_get()
        last_exc: Optional[Exception] = None

        for attempt in range(3):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        for key in ("data", "persons", "results", "items"):
                            if key in data and isinstance(data[key], list):
                                return data[key]
                    return []
            except (aiohttp.ClientError, Exception) as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "[sos_venezuela] fetch_page(%d) attempt %d failed: %s -- retry in %ds",
                    page, attempt + 1, exc, wait,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"sos_venezuela fetch_page({page}) failed after 3 attempts: {last_exc}"
        )

    def normalize(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        display_name = (raw.get("display_name") or "").strip()
        if not display_name:
            return None

        status = (raw.get("status") or "").lower().strip()
        kind = "found" if status in ("encontrado", "found") else "missing"

        municipio = (raw.get("municipio") or "").strip()
        parroquia = (raw.get("parroquia") or "").strip()
        location_parts = [p for p in (municipio, parroquia) if p]
        last_seen_location = ", ".join(location_parts) or None

        record_id = raw.get("id")
        source_url = (
            f"https://sosvenezuela2026.com/persona/{record_id}"
            if record_id
            else f"sos::{display_name}"
        )

        return {
            "kind": kind,
            "full_name": display_name,
            "age": None,
            "last_seen_location": last_seen_location,
            "distinguishing_marks": None,
            "clothing": None,
            "source": "sos_venezuela",
            "source_url": source_url,
            # strip_pii removes 'cedula_masked' and any other PII
            "raw_data": strip_pii(raw),
        }
