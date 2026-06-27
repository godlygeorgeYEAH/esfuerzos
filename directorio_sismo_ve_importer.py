#!/usr/bin/env python3
"""
directorio_sismo_ve_importer.py

One-time / periodic importer for Directorio Sismo Venezuela 2026.
Source: https://directorio-sismo.netlify.app/

PURPOSE -- source discovery, NOT victim matching.
==================================================
This site is a curated meta-directory of active relief platforms
(missing persons registries, hospitals, shelters, aid orgs, etc.).
It contains NO person records, NO cedulas, and NO names of missing
individuals. Records must NOT be written to the `reports` table (which
feeds the matching pipeline).

Output:
  1. JSON file -- directorio_sismo_ve.json in the current working directory.
  2. Supabase `discovered_sources` table -- optional. Insert gracefully
     fails if the table does not exist yet; see the CREATE TABLE comment
     below. The team can run the migration whenever they want Supabase
     persistence.
  3. Stdout -- human-readable summary.

Run weekly or after major events to detect newly listed sources.

Usage:
    python directorio_sismo_ve_importer.py
    python directorio_sismo_ve_importer.py --output /data/sources.json
    python directorio_sismo_ve_importer.py --dry-run

Supabase migration (run once when ready to persist):
    CREATE TABLE IF NOT EXISTS discovered_sources (
        id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
        source_id    text        UNIQUE NOT NULL,
        name         text        NOT NULL,
        url          text,
        section      text,
        description  text,
        coverage     text,
        kind         text,
        raw_data     jsonb,
        discovered_at  timestamptz DEFAULT now(),
        last_seen_at   timestamptz DEFAULT now()
    );
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup, NavigableString, Tag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("directorio_sismo_ve")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SITE_URL = "https://directorio-sismo.netlify.app/"
SOURCE_NAME = "directorio_sismo_ve"
FETCH_TIMEOUT = 25.0

HEADERS = {
    "User-Agent": "ReuneVE-SourceDiscovery/1.0 (+https://reune.ve)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.8",
}

# Section heading text -> kind of platforms in that section.
# "missing": registries where families SEARCH for missing people.
# "found":   hospitals, shelters, aid orgs, found-persons registries.
SECTION_KEYWORDS: dict[str, str] = {
    # -> missing
    "desaparecid": "missing",
    "missing": "missing",
    "busca": "missing",
    "mascot": "missing",
    "huellas": "missing",
    "animal": "missing",
    "pet": "missing",
    "perdid": "missing",
    # -> found
    "salud": "found",
    "medic": "found",
    "hospital": "found",
    "localizad": "found",
    "encontrad": "found",
    "refugio": "found",
    "shelter": "found",
    "acopio": "found",
    "donaci": "found",
    "ayuda": "found",
    "voluntari": "found",
    "ingenier": "found",
    "daños": "found",
    "damage": "found",
    "structural": "found",
    "servic": "found",
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PlatformEntry:
    """One entry in the meta-directory (an organization, not a person)."""
    name: str
    url: str
    section: str
    description: str = ""
    coverage: str = "Venezuela"
    kind: str = "found"   # "missing" | "found"
    source_id: str = ""   # dedup key: SOURCE_NAME:domain/path

    def __post_init__(self) -> None:
        if not self.source_id:
            self.source_id = _build_source_id(self.name, self.url)


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _infer_kind(section: str, description: str = "") -> str:
    text = (section + " " + description).lower()
    # Check missing first (more restrictive)
    for kw, kind in SECTION_KEYWORDS.items():
        if kw in text and kind == "missing":
            return "missing"
    for kw, kind in SECTION_KEYWORDS.items():
        if kw in text and kind == "found":
            return "found"
    return "found"


def _clean_url(raw: str) -> Optional[str]:
    """Return a valid http/https URL or None."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.startswith(("#", "javascript:", "mailto:", "tel:", "+58", "/")):
        return None
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        p = urlparse(raw)
        if p.scheme in ("http", "https") and p.netloc:
            return raw
    except Exception:
        pass
    return None


def _build_source_id(name: str, url: str) -> str:
    """Stable, unique dedup key for a platform entry."""
    if url:
        try:
            p = urlparse(url)
            return f"{SOURCE_NAME}:{p.netloc}{p.path}".rstrip("/")
        except Exception:
            pass
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:60]
    return f"{SOURCE_NAME}:{slug}"


# ---------------------------------------------------------------------------
# HTML parser -- multiple strategies, deduped by URL
# ---------------------------------------------------------------------------


def _text(el: Tag) -> str:
    return el.get_text(separator=" ", strip=True) if el else ""


def _first_link(el: Tag) -> Optional[str]:
    """Return the href of the first <a> found inside el, or None."""
    a = el.find("a")
    if a:
        return _clean_url(a.get("href", ""))
    return None


def _parse_h2_h3_sections(soup: BeautifulSoup) -> list[PlatformEntry]:
    """
    Primary strategy: walk h2 section headings, then h3 resource headings.
    Each h3 is paired with an adjacent <a> (in self, parent, or nearby siblings)
    and a <p> description.

    Observed structure:
        <h2>Section Name</h2>
        <a href="URL">emoji label</a>   <- category badge (or resource link)
        <h3>Resource Name</h3>
        <p>Description</p>
    """
    entries: list[PlatformEntry] = []
    body = soup.find("body") or soup

    for h2 in body.find_all("h2"):
        section = _text(h2)
        if not section:
            continue

        # Iterate siblings until the next h2
        for sib in h2.next_siblings:
            if isinstance(sib, NavigableString):
                continue
            if not isinstance(sib, Tag):
                continue
            if sib.name == "h2":
                break
            if sib.name != "h3":
                continue

            # Found a resource heading
            h3 = sib
            name = _text(h3)
            if not name:
                continue

            url: Optional[str] = _first_link(h3)

            # Look backwards among siblings for a nearby <a>
            if not url:
                prev = h3.previous_sibling
                steps = 0
                while prev and steps < 4:
                    if isinstance(prev, Tag):
                        if prev.name == "h3":
                            break
                        url = _first_link(prev) or (
                            _clean_url(prev.get("href", "")) if prev.name == "a" else None
                        )
                        if url:
                            break
                    prev = prev.previous_sibling
                    steps += 1

            # Look forward among siblings for a link and description
            description = ""
            if not url:
                nxt = h3.next_sibling
                steps = 0
                while nxt and steps < 6:
                    if isinstance(nxt, Tag):
                        if nxt.name in ("h2", "h3"):
                            break
                        if nxt.name == "a" and not url:
                            url = _clean_url(nxt.get("href", ""))
                        elif not url:
                            url = _first_link(nxt)
                        if nxt.name == "p" and not description:
                            description = _text(nxt)
                    nxt = nxt.next_sibling
                    steps += 1
            else:
                nxt = h3.next_sibling
                steps = 0
                while nxt and steps < 4:
                    if isinstance(nxt, Tag):
                        if nxt.name in ("h2", "h3"):
                            break
                        if nxt.name == "p" and not description:
                            description = _text(nxt)
                    nxt = nxt.next_sibling
                    steps += 1

            if not url:
                logger.debug("No URL found for entry '%s' in section '%s'", name, section)
                continue

            entries.append(PlatformEntry(
                name=name,
                url=url,
                section=section,
                description=description[:300],
                kind=_infer_kind(section, description),
            ))

    return entries


def _parse_table_rows(soup: BeautifulSoup) -> list[PlatformEntry]:
    """
    Fallback: table-based layout.
    Expects: | Name | URL/link | Description | ... | Coverage |
    """
    entries: list[PlatformEntry] = []
    current_section = "general"

    for el in soup.find_all(["h2", "tr"]):
        if el.name == "h2":
            current_section = _text(el)
            continue

        cells = el.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        # Skip header rows
        if all(c.name == "th" for c in cells):
            continue

        name = _text(cells[0])
        url = _first_link(cells[0]) or _first_link(cells[1])
        if not url and len(cells) > 1:
            raw = _text(cells[1])
            url = _clean_url(raw)

        description = _text(cells[2]) if len(cells) > 2 else ""
        coverage = _text(cells[-1]) if len(cells) >= 4 else "Venezuela"

        if not name or not url:
            continue

        entries.append(PlatformEntry(
            name=name,
            url=url,
            section=current_section,
            description=description[:300],
            coverage=coverage,
            kind=_infer_kind(current_section, description),
        ))

    return entries


def _parse_list_items(soup: BeautifulSoup) -> list[PlatformEntry]:
    """
    Fallback: unordered/ordered list items, each with a link + optional description.
    """
    entries: list[PlatformEntry] = []
    current_section = "general"

    for el in soup.find_all(["h2", "li"]):
        if el.name == "h2":
            current_section = _text(el)
            continue

        a = el.find("a")
        if not a:
            continue
        url = _clean_url(a.get("href", ""))
        if not url:
            continue

        name = _text(a) or _text(el)
        if not name:
            continue
        description = _text(el)

        entries.append(PlatformEntry(
            name=name,
            url=url,
            section=current_section,
            description=description[:300],
            kind=_infer_kind(current_section, description),
        ))

    return entries


def _parse_all_links(soup: BeautifulSoup) -> list[PlatformEntry]:
    """
    Last-resort fallback: scrape every external <a> tag on the page.
    Uses parent heading context to infer the section.
    """
    entries: list[PlatformEntry] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        url = _clean_url(a["href"])
        if not url or url in seen_urls:
            continue
        # Skip same-site navigation
        parsed = urlparse(url)
        if "directorio-sismo" in parsed.netloc:
            continue

        name = _text(a)
        if not name or len(name) < 3:
            continue

        # Walk up to find nearest heading
        section = "general"
        parent = a.parent
        depth = 0
        while parent and depth < 8:
            for heading_tag in ("h1", "h2", "h3"):
                h = parent.find_previous(heading_tag)
                if h:
                    section = _text(h)
                    break
            if section != "general":
                break
            parent = parent.parent
            depth += 1

        description = _text(a.parent) if a.parent else ""
        seen_urls.add(url)

        entries.append(PlatformEntry(
            name=name,
            url=url,
            section=section,
            description=description[:300],
            kind=_infer_kind(section, description),
        ))

    return entries


def parse_directory(html: str) -> list[PlatformEntry]:
    """
    Run multiple parsing strategies and merge results.
    Primary: h2/h3 section structure.
    Fallbacks: tables -> li items -> all-links scan.
    Returns deduplicated entries sorted by section name.
    """
    soup = BeautifulSoup(html, "html.parser")

    all_entries: list[PlatformEntry] = []
    seen_urls: set[str] = set()

    def _add_unique(batch: list[PlatformEntry]) -> None:
        for e in batch:
            if e.url and e.url not in seen_urls:
                seen_urls.add(e.url)
                all_entries.append(e)

    primary = _parse_h2_h3_sections(soup)
    logger.info("h2/h3 strategy: %d entries", len(primary))
    _add_unique(primary)

    if len(all_entries) < 5:
        tables = _parse_table_rows(soup)
        logger.info("table strategy: %d entries", len(tables))
        _add_unique(tables)

    if len(all_entries) < 5:
        lists = _parse_list_items(soup)
        logger.info("list-item strategy: %d entries", len(lists))
        _add_unique(lists)

    if len(all_entries) < 5:
        fallback = _parse_all_links(soup)
        logger.info("all-links fallback: %d entries", len(fallback))
        _add_unique(fallback)

    all_entries.sort(key=lambda e: e.section)
    return all_entries


# ---------------------------------------------------------------------------
# Supabase persistence (optional)
# ---------------------------------------------------------------------------


async def _upsert_discovered_sources(
    client: httpx.AsyncClient,
    supabase_url: str,
    supabase_key: str,
    entries: list[PlatformEntry],
) -> dict:
    """
    Upsert entries into the `discovered_sources` Supabase table.
    If the table does not exist, logs the DDL and returns without error.

    Create table with:
        CREATE TABLE IF NOT EXISTS discovered_sources (
            id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
            source_id    text        UNIQUE NOT NULL,
            name         text        NOT NULL,
            url          text,
            section      text,
            description  text,
            coverage     text,
            kind         text,
            raw_data     jsonb,
            discovered_at  timestamptz DEFAULT now(),
            last_seen_at   timestamptz DEFAULT now()
        );
    """
    inserted = 0
    errors = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for entry in entries:
        row = {
            "source_id": entry.source_id,
            "name": entry.name,
            "url": entry.url,
            "section": entry.section,
            "description": entry.description,
            "coverage": entry.coverage,
            "kind": entry.kind,
            "last_seen_at": now_iso,
            "raw_data": asdict(entry),
        }
        try:
            resp = await client.post(
                f"{supabase_url}/rest/v1/discovered_sources",
                json=row,
                params={"on_conflict": "source_id"},
                headers={
                    "apikey": supabase_key,
                    "Authorization": f"Bearer {supabase_key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=ignore-duplicates,return=minimal",
                },
                timeout=10.0,
            )
            if resp.status_code in (200, 201):
                inserted += 1
            elif resp.status_code == 404:
                logger.warning(
                    "Table `discovered_sources` not found. "
                    "Run the CREATE TABLE migration from the module docstring, "
                    "then re-run this importer for Supabase persistence."
                )
                return {"inserted": 0, "errors": 0, "skipped": len(entries)}
            elif resp.status_code == 409:
                pass  # Duplicate on source_id -- already known platform.
            else:
                logger.warning(
                    "Supabase %d for '%s': %s",
                    resp.status_code,
                    entry.name,
                    resp.text[:200],
                )
                errors += 1
        except httpx.HTTPError as exc:
            logger.error("HTTP error upserting '%s': %s", entry.name, exc)
            errors += 1

    return {"inserted": inserted, "errors": errors}


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


async def run(output_path: str = "directorio_sismo_ve.json", dry_run: bool = False) -> dict:
    """
    Fetch the directory, parse it, write JSON, optionally persist to Supabase.
    Returns a summary dict.
    """
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

    async with httpx.AsyncClient(
        headers=HEADERS,
        follow_redirects=True,
        timeout=FETCH_TIMEOUT,
    ) as client:
        # 1. Fetch page
        logger.info("Fetching %s", SITE_URL)
        try:
            resp = await client.get(SITE_URL)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.error("Failed to fetch directory: %s", exc)
            return {"entries": 0, "inserted": 0, "errors": 1}

        logger.info("Fetched %d bytes (HTTP %d)", len(resp.text), resp.status_code)

        # 2. Parse
        entries = parse_directory(resp.text)
        logger.info("Parsed %d unique platform entries", len(entries))

        if not entries:
            logger.error(
                "ZERO entries parsed from %s. "
                "The page structure may have changed or requires JavaScript rendering. "
                "Inspect the HTML and update the parser before using output.",
                SITE_URL,
            )
            return {"entries": 0, "inserted": 0, "errors": 1}

        # 3. Print summary
        _print_summary(entries)

        if dry_run:
            logger.info("Dry run -- skipping JSON write and Supabase upsert.")
            return {"entries": len(entries), "inserted": 0, "errors": 0, "dry_run": True}

        # 4. Write JSON
        payload = {
            "source": SOURCE_NAME,
            "site_url": SITE_URL,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "count": len(entries),
            "platforms": [asdict(e) for e in entries],
        }
        try:
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            logger.info("Wrote %d entries to %s", len(entries), output_path)
        except OSError as exc:
            logger.error("Could not write JSON: %s", exc)

        # 5. Optional Supabase persist
        db_stats: dict = {"inserted": 0, "errors": 0}
        if supabase_url and supabase_key:
            logger.info("Upserting to Supabase discovered_sources table...")
            db_stats = await _upsert_discovered_sources(
                client, supabase_url, supabase_key, entries
            )
            logger.info("Supabase stats: %s", db_stats)
        else:
            logger.info(
                "SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set -- skipping DB write."
            )

        return {
            "entries": len(entries),
            "json_path": output_path,
            **db_stats,
        }


def _print_summary(entries: list[PlatformEntry]) -> None:
    missing_count = sum(1 for e in entries if e.kind == "missing")
    found_count = sum(1 for e in entries if e.kind == "found")

    print(f"\n{'=' * 60}")
    print(f"Directorio Sismo Venezuela -- {len(entries)} platforms discovered")
    print(f"  kind=missing (search/registry):  {missing_count}")
    print(f"  kind=found   (shelter/hospital/aid): {found_count}")
    print(f"{'=' * 60}")

    current_section = None
    for e in entries:
        if e.section != current_section:
            current_section = e.section
            print(f"\n[{e.section}]")
        print(f"  [{e.kind:7s}] {e.name}")
        print(f"             {e.url}")

    print(f"\n{'=' * 60}")
    print("NOTE: These are platforms/organizations, NOT victim records.")
    print("      Do NOT load directorio_sismo_ve into the reports/matching table.")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import platform list from Directorio Sismo Venezuela 2026."
    )
    parser.add_argument(
        "--output",
        default="directorio_sismo_ve.json",
        help="Path for JSON output (default: ./directorio_sismo_ve.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print only; skip JSON write and Supabase upsert.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = asyncio.run(run(output_path=args.output, dry_run=args.dry_run))
    print(f"\nResult: {json.dumps(result, indent=2)}")
    sys.exit(0 if result.get("errors", 0) == 0 else 1)
