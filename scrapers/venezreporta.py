"""
api/scrapers/venezreporta.py -- Scraper for Venezuela Reporta HTML pages
https://venezuelareporta.org/buscar?status={buscando|encontrado}&page={n}

Note: run_full and run_poll are fully overridden here (rather than using the
base-class versions) to avoid per-instance status state, which would be unsafe
if APScheduler runs a poll and full job concurrently on the same instance.
Status is passed as a parameter through the call chain.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_BASE_URL = "https://venezuelareporta.org"
_SEARCH_PATH = "/buscar"
_STATUSES = ("buscando", "encontrado")
_USER_AGENT = "CrisisVE-Bot/1.0 (+https://crisisve.org)"


class VenezReportaScraper(BaseScraper):
    """Scrapes buscando + encontrado pages from Venezuela Reporta."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__("venezreporta", supabase_url, supabase_key)

    # ------------------------------------------------------------------
    # fetch_page satisfies the abstract contract.
    # It fetches the 'buscando' status by default.
    # The overridden run_full/run_poll iterate both statuses directly
    # via _fetch_status_page, so this is not called in normal operation.
    # ------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        return await self._fetch_status_page("buscando", page)

    # ------------------------------------------------------------------
    # Internal: status-aware fetch
    # ------------------------------------------------------------------

    async def _fetch_status_page(
        self, status: str, page: int
    ) -> list[dict[str, Any]]:
        """Fetch and parse one page for a given status (buscando|encontrado)."""
        url = f"{_BASE_URL}{_SEARCH_PATH}?status={status}&page={page}"
        session = await self._session_get()
        last_exc: Optional[Exception] = None

        for attempt in range(3):
            try:
                async with session.get(
                    url, headers={"User-Agent": _USER_AGENT}
                ) as resp:
                    resp.raise_for_status()
                    html = await resp.text()
                    return self._parse_html(html, status, url)
            except (aiohttp.ClientError, Exception) as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning(
                    "[venezreporta] %s page %d attempt %d failed: %s -- retry in %ds",
                    status, page, attempt + 1, exc, wait,
                )
                await asyncio.sleep(wait)

        raise RuntimeError(
            f"venezreporta {status} page {page} failed after 3 attempts: {last_exc}"
        )

    def _parse_html(
        self, html: str, status: str, page_url: str
    ) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        records: list[dict[str, Any]] = []

        # Primary: find person cards containing an img with meaningful alt text
        cards = soup.select(
            "div[class*='card'], article, li[class*='person'], div[class*='person']"
        )

        if cards:
            for card in cards:
                img = card.find("img")
                if not img:
                    continue
                name = (img.get("alt") or "").strip()
                if not name or name.lower() in ("foto", "photo", "imagen", ""):
                    continue

                loc_el = card.find(
                    class_=lambda c: c and "text-ink-soft" in c  # type: ignore[arg-type]
                )
                location = loc_el.get_text(strip=True) if loc_el else None

                chip_el = card.find(
                    lambda tag: tag.name in ("span", "div")  # type: ignore[arg-type]
                    and tag.has_attr("class")
                    and any(
                        kw in " ".join(tag["class"])
                        for kw in ("chip", "badge", "status", "tag")
                    )
                )
                chip_text = (
                    chip_el.get_text(strip=True).lower() if chip_el else status
                )

                img_src = (img.get("src") or "").strip()
                full_src = urljoin(_BASE_URL, img_src) if img_src else page_url

                records.append({
                    "_name": name,
                    "_location": location,
                    "_status": chip_text,
                    "_source_url": full_src,
                    "_page_url": page_url,
                })
        else:
            # Fallback: scrape raw img tags with non-trivial alt text
            for img in soup.select("img[alt]"):
                name = (img.get("alt") or "").strip()
                if not name or name.lower() in ("foto", "photo", "imagen", ""):
                    continue
                img_src = (img.get("src") or "").strip()
                full_src = urljoin(_BASE_URL, img_src) if img_src else page_url
                records.append({
                    "_name": name,
                    "_location": None,
                    "_status": status,
                    "_source_url": full_src,
                    "_page_url": page_url,
                })

        return records

    def normalize(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        name = (raw.get("_name") or "").strip()
        if not name:
            return None

        status_text = (raw.get("_status") or "").lower()
        kind = "found" if "encontrado" in status_text or status_text == "found" else "missing"

        return {
            "kind": kind,
            "full_name": name,
            "age": None,
            "last_seen_location": raw.get("_location") or None,
            "distinguishing_marks": None,
            "clothing": None,
            "source": "venezreporta",
            "source_url": raw.get("_source_url") or raw.get("_page_url"),
            # Drop internal _ keys, strip any PII, store as raw_data
            "raw_data": strip_pii({
                k.lstrip("_"): v for k, v in raw.items()
            }),
        }

    # ------------------------------------------------------------------
    # Overrides: iterate both statuses without using instance-level state.
    # This avoids a concurrency bug where simultaneous poll + full jobs
    # would stomp a shared self._current_status field.
    # ------------------------------------------------------------------

    async def run_full(self, poll_interval: int = 3600) -> dict[str, int]:
        combined: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        for status in _STATUSES:
            page = 1
            while True:
                try:
                    rows = await self._fetch_status_page(status, page)
                except Exception as exc:
                    logger.error(
                        "[venezreporta] full %s page %d error: %s", status, page, exc
                    )
                    combined["errors"] += 1
                    break
                if not rows:
                    break
                for raw in rows:
                    try:
                        normalized = self.normalize(raw)
                        if normalized is None:
                            continue
                        ok = await self.upsert_report(normalized)
                        if ok:
                            combined["inserted"] += 1
                        else:
                            combined["errors"] += 1
                    except Exception as exc:
                        logger.error("[venezreporta] full record error: %s", exc)
                        combined["errors"] += 1
                page += 1
        await self.log_run("full", combined)
        logger.info("[venezreporta] full done: %s", combined)
        return combined

    async def run_poll(self, poll_interval: int = 300) -> dict[str, int]:
        combined: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        for status in _STATUSES:
            try:
                rows = await self._fetch_status_page(status, 1)
            except Exception as exc:
                logger.error("[venezreporta] poll %s error: %s", status, exc)
                combined["errors"] += 1
                continue
            for raw in rows:
                try:
                    normalized = self.normalize(raw)
                    if normalized is None:
                        continue
                    ok = await self.upsert_report(normalized)
                    if ok:
                        combined["inserted"] += 1
                    else:
                        combined["errors"] += 1
                except Exception as exc:
                    logger.error("[venezreporta] poll record error: %s", exc)
                    combined["errors"] += 1
        await self.log_run("poll", combined)
        logger.info("[venezreporta] poll done: %s", combined)
        return combined
