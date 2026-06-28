"""
scrapers/tuayudave.py -- Periodic scraper for TuAyudaVE (tuayudave.com).

Source: https://tuayudave.com  (~1,700 hospital-patient records)
Kind:   "found" for all rows — people who entered hospitals/clinics in Venezuela.

The site is server-rendered HTML (no public API; confirmed via probing every
/api/* and /__data.json variant -> 404). Records are paginated at
GET /?page=N, ~21 cards per page, ~81 pages total.

Card structure (confirmed from live HTML 2026-06-28):
  <div id="cards-grid">
    <div class="rounded-2xl ...">            <- one per person (direct child)
      <h3 class="... capitalize">Nombre Apellido</h3>
      <div class="mt-4 space-y-1.5 ...">
        <p><span class="font-semibold ...">Edad:</span> 8 años</p>
        <p><span class="font-semibold ...">Ubicación:</span> <span>hospital ... - caracas</span></p>
        <p><span class="font-semibold ...">Sexo:</span> ...</p>      (optional)
        <p><span class="font-semibold ...">Cédula:</span> ...</p>    (optional, PII)
      </div>
    </div>
  </div>

No stable per-record id is exposed, so the dedup key is a hash of name+location.

Constructor takes (supabase_url, supabase_key) like the other BaseScraper
subclasses; registered in scraper_orchestrator._make_scrapers().
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, strip_pii

logger = logging.getLogger(__name__)

_SOURCE_NAME = "tuayudave"
_BASE_URL = "https://tuayudave.com"
_MAX_PAGES = 200  # safety cap (real total ~81)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ReuneVE-Scraper/1.0; +https://reune.ve)",
    "Accept": "text/html",
}

# Label -> internal field. Labels are matched lowercased, accent-insensitive.
_LABEL_AGE = ("edad",)
_LABEL_LOC = ("ubicacion", "ubicación", "lugar", "centro")
_LABEL_SEX = ("sexo", "genero", "género")
_PII_LABELS = ("cedula", "cédula", "telefono", "teléfono", "contacto")


def _deaccent(s: str) -> str:
    return (s.lower()
            .replace("á", "a").replace("é", "e").replace("í", "i")
            .replace("ó", "o").replace("ú", "u"))


class TuAyudaVEScraper(BaseScraper):
    """Periodic HTML scraper for TuAyudaVE hospital-patient listings."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__(_SOURCE_NAME, supabase_url, supabase_key)

    async def fetch_page(self, page: int) -> list[dict[str, Any]]:
        """Fetch and parse one page of person cards. Returns [] past the end."""
        if page > _MAX_PAGES:
            return []
        url = f"{_BASE_URL}/?page={page}"
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=20, headers=_HEADERS,
                                             follow_redirects=True) as cl:
                    r = await cl.get(url)
                    r.raise_for_status()
                    return self._parse_html(r.text)
            except Exception as exc:
                last_exc = exc
                wait = 2 ** attempt
                logger.warning("[tuayudave] page %d attempt %d: %s — retry in %ds",
                               page, attempt + 1, exc, wait)
                await asyncio.sleep(wait)
        logger.error("[tuayudave] page %d failed after 3 attempts: %s", page, last_exc)
        return []

    def _parse_html(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        grid = soup.select_one("#cards-grid")
        if not grid:
            return []
        records: list[dict[str, Any]] = []
        for card in grid.find_all("div", recursive=False):
            h3 = card.find("h3")
            if not h3:
                continue
            name = h3.get_text(strip=True)
            if not name:
                continue
            fields: dict[str, str] = {}
            for p in card.find_all("p"):
                label_span = p.find("span", class_="font-semibold")
                if not label_span:
                    continue
                label = _deaccent(label_span.get_text(strip=True).rstrip(":"))
                # Value = full <p> text minus the label text
                value = p.get_text(" ", strip=True)
                lbl_txt = label_span.get_text(strip=True)
                if value.startswith(lbl_txt):
                    value = value[len(lbl_txt):].strip()
                fields[label] = value
            records.append({"_name": name, "_fields": fields})
        return records

    def normalize(self, raw: dict[str, Any]) -> Optional[dict[str, Any]]:
        name = (raw.get("_name") or "").strip()
        if not name or len(name) < 3:
            return None
        fields: dict[str, str] = raw.get("_fields") or {}

        age: Optional[int] = None
        location: Optional[str] = None
        for label, value in fields.items():
            if label in _LABEL_AGE:
                m = re.search(r"\d{1,3}", value)
                if m:
                    candidate = int(m.group(0))
                    if 0 < candidate < 130:
                        age = candidate
            elif any(label.startswith(l) for l in _LABEL_LOC):
                location = value or None

        slug = hashlib.md5(f"{name.lower()}|{(location or '').lower()}".encode()).hexdigest()[:16]

        # raw_data keeps non-PII fields (sexo, etc.); strip_pii drops cedula/phone
        keep = {k: v for k, v in fields.items() if k not in _PII_LABELS}
        return {
            "kind": "found",
            "full_name": name,
            "age": age,
            "last_seen_location": location,
            "distinguishing_marks": fields.get("sexo") and f"Sexo: {fields['sexo']}" or None,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"tuayudave:{slug}",
            "raw_data": strip_pii(keep),
        }
