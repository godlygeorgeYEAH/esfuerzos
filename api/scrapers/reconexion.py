"""
api/scrapers/reconexion.py -- Scraper for the Reconexion API
https://desaparecidos-terremoto-api.theempire.tech/api/personas
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import aiohttp

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_BASE_URL = "https://desaparecidos-terremoto-api.theempire.tech/api/personas"
_PAGE_SIZE = 100


class ReconexionScraper(BaseScraper):
    """Pulls missing-persons records from the Reconexion earthquake API."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__("reconexion", supabase_url, supabase_key)

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """GET /api/personas?page={n}&pageSize=100 with 3 retries (2**attempt backoff)."""
        url = f"{_BASE_URL}?page={page}&pageSize={_PAGE_SIZE}"
        session = await self._session_get()
        last_exc: Optional[Exception] = None

        for attempt in range(3):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    # API may return a list directly or wrap in an envelope
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        for key in ("data", "personas", "results", "items"):
                            if key in data and isinstance(data[key], list):
                                return data[key]
                    return []
            except (aiohttp.ClientError, Exception) as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "[reconexion] fetch_page(%d) attempt %d failed: %s -- retry in %ds",
                    page, attempt + 1, exc, wait,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"reconexion fetch_page({page}) failed after 3 attempts: {last_exc}"
        )

    def normalize(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        nombre = (raw.get("nombre") or "").strip()
        if not nombre:
            return None

        estado_raw = (raw.get("estado") or "").lower()
        kind = "found" if "localizado" in estado_raw else "missing"

        foto_url = (raw.get("foto_url") or "").strip()
        # foto_url is the dedup key; fall back to name-based synthetic URL
        source_url = foto_url if foto_url else f"reconexion::{nombre}"

        age: Optional[int] = None
        edad_raw = raw.get("edad")
        if edad_raw is not None:
            try:
                age = int(str(edad_raw).split()[0])
            except (ValueError, TypeError):
                age = None

        return {
            "kind": kind,
            "full_name": nombre,
            "age": age,
            "last_seen_location": raw.get("ubicacion") or None,
            "distinguishing_marks": raw.get("descripcion") or None,
            "clothing": None,
            "source": "reconexion",
            "source_url": source_url,
            # strip_pii removes 'contacto' and any other PII before storing
            "raw_data": strip_pii(raw),
        }
