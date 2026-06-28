"""
api/scrapers/venezreporta.py -- Venezuela Reporta via public REST API.

API: GET https://venezuelareporta.org/api/v1/personas
Params: status (buscando|encontrado), limit, offset
No API key required.

Replaces the previous HTML scraper. The source_url dedup key format
is preserved as "venezreporta-api:{id}" so re-running does not
re-insert records that were already scraped by the old scraper
(different key format anyway, so both live in the DB — no conflict).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any

import httpx

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_API_BASE = "https://venezuelareporta.org/api/v1"
_PERSONAS_PATH = "/personas"
_STATUSES = ("buscando", "encontrado")
_PAGE_SIZE = 100
_HEADERS = {"User-Agent": "ReuneVE-Bot/1.0 (+https://reune.ve)"}


class VenezReportaScraper(BaseScraper):
    """Ingests Venezuela Reporta records via the public /api/v1/personas endpoint."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__("venezreporta", supabase_url, supabase_key)

    async def _fetch_page(self, status: str, offset: int) -> list[dict[str, Any]]:
        """One page of records for a given status."""
        url = f"{_API_BASE}{_PERSONAS_PATH}"
        params = {"status": status, "limit": _PAGE_SIZE, "offset": offset}
        last_exc: Exception | None = None

        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=15, headers=_HEADERS) as cl:
                    r = await cl.get(url, params=params)
                    r.raise_for_status()
                    data = r.json()
                    # API may return list directly or wrapped in {"data": [...]}
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        for key in ("data", "personas", "results", "items"):
                            if isinstance(data.get(key), list):
                                return data[key]
                    logger.warning("[venezreporta] unexpected response shape: %s", str(data)[:120])
                    return []
            except Exception as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "[venezreporta] %s offset=%d attempt %d: %s — retry in %ds",
                    status, offset, attempt + 1, exc, wait,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"venezreporta {status} offset={offset} failed after 3 attempts: {last_exc}"
        )

    # BaseScraper abstract contract — used by run_poll base implementation
    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        return await self._fetch_page("buscando", (page - 1) * _PAGE_SIZE)

    def normalize(self, raw: dict[str, Any]) -> dict[str, Any] | None:
        # Field name variants across potential API versions
        name = (
            raw.get("name") or raw.get("nombre") or raw.get("full_name") or ""
        ).strip()
        if not name or len(name) < 3:
            return None

        status_text = (raw.get("status") or raw.get("estado") or "").lower()
        kind = "found" if "encontrado" in status_text or status_text == "found" else "missing"

        age_raw = raw.get("age") or raw.get("edad")
        age: int | None = None
        if age_raw is not None:
            try:
                age = int(str(age_raw).strip())
            except (ValueError, TypeError):
                pass

        location = (
            raw.get("ciudad") or raw.get("location") or raw.get("ultima_ubicacion") or None
        )

        # Use API id for stable dedup key; fall back to name hash
        record_id = raw.get("id") or raw.get("_id")
        if record_id:
            source_url = f"venezreporta-api:{record_id}"
        else:
            slug = hashlib.md5(name.lower().encode()).hexdigest()[:12]
            source_url = f"venezreporta-api:{slug}"

        return {
            "kind": kind,
            "full_name": name,
            "age": age,
            "last_seen_location": location,
            "distinguishing_marks": None,
            "clothing": None,
            "source": "venezreporta",
            "source_url": source_url,
            "raw_data": strip_pii({
                **{k: v for k, v in raw.items()},
                # Preserve foto_url for future photo pipeline ingestion
                "foto_url": raw.get("foto_url") or raw.get("photo_url") or raw.get("foto"),
            }),
        }

    # Override run_full to paginate both statuses via offset
    async def run_full(self, poll_interval: int = 3600) -> dict:
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        for status in _STATUSES:
            offset = 0
            while True:
                try:
                    rows = await self._fetch_page(status, offset)
                except Exception as exc:
                    logger.error("[venezreporta] full %s offset=%d: %s", status, offset, exc)
                    stats["errors"] += 1
                    break
                if not rows:
                    break
                for raw in rows:
                    try:
                        normalized = self.normalize(raw)
                        if normalized is None:
                            continue
                        ok = await self.upsert_report(normalized)
                        stats["inserted" if ok else "errors"] += 1
                    except Exception as exc:
                        logger.error("[venezreporta] full record error: %s", exc)
                        stats["errors"] += 1
                offset += _PAGE_SIZE
        await self.log_run("full", stats)
        logger.info("[venezreporta] full done: %s", stats)
        return stats

    # Override run_poll to fetch first page of both statuses
    async def run_poll(self, poll_interval: int = 300) -> dict:
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        for status in _STATUSES:
            try:
                rows = await self._fetch_page(status, 0)
            except Exception as exc:
                logger.error("[venezreporta] poll %s: %s", status, exc)
                stats["errors"] += 1
                continue
            for raw in rows:
                try:
                    normalized = self.normalize(raw)
                    if normalized is None:
                        continue
                    ok = await self.upsert_report(normalized)
                    stats["inserted" if ok else "errors"] += 1
                except Exception as exc:
                    logger.error("[venezreporta] poll record error: %s", exc)
                    stats["errors"] += 1
        await self.log_run("poll", stats)
        logger.info("[venezreporta] poll done: %s", stats)
        return stats
