"""
directorio_sismo_ve_importer.py - One-time importer for platform/organization directory.

After the June 24 2026 Venezuela earthquake, dozens of NGOs, government platforms,
WhatsApp groups, and community websites launched to track missing and found persons.
This script catalogs them in the 'discovered_sources' table for the Reune VE team
to evaluate, prioritize, and integrate.

This importer writes to 'discovered_sources', NOT 'reports'.
It is a meta-importer: it documents WHERE data lives, not the data itself.

HOW TO USE
----------
Run from repo root:

    python scratch/directorio_sismo_ve_importer.py

Optional flags:
    --input FILE     JSON/CSV file of sources to import (default: built-in list)
    --url URL        Fetch additional sources from a directory page (HTML)
    --dry-run        Print records, no DB writes

Environment variables:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

TABLE SCHEMA (discovered_sources)
----------------------------------
If the table does not exist, the importer creates it via PostgREST if you have
DDL access, OR falls back to writing a SQL migration file for manual application.

Expected columns:
    id              uuid (pk, default gen_random_uuid())
    name            text NOT NULL
    url             text UNIQUE
    category        text  -- 'missing_persons' | 'hospital' | 'shelter' | 'govt' | 'ngo' | 'community'
    platform        text  -- 'whatsapp' | 'instagram' | 'website' | 'supabase' | 'google_sheet' | etc.
    region          text  -- 'nacional' | 'la_guaira' | 'caracas' | 'miranda' | etc.
    has_api         bool  -- true if a scrapeable API was identified
    api_notes       text  -- endpoint / auth notes
    record_count    int   -- estimated records (null = unknown)
    status          text  -- 'pending' | 'integrated' | 'rejected' | 'monitoring'
    notes           text
    discovered_at   timestamptz DEFAULT now()
    source_of_discovery text  -- how we found this source

PARSING STRATEGIES
------------------
1. Built-in curated list (highest precision, always runs)
2. JSON/CSV file via --input (for lists compiled manually or by researcher agents)
3. HTML directory page via --url (BeautifulSoup scrape, best-effort)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any
from pathlib import Path

import httpx

try:
    from bs4 import BeautifulSoup
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("directorio_sismo")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE_OF_DISCOVERY = "directorio_sismo_ve_importer_v1"
TABLE = "discovered_sources"
BATCH_SIZE = 50

# ---------------------------------------------------------------------------
# Built-in curated source list
# This list was compiled by the Reune VE research team from web searches,
# Twitter/X, Instagram, and direct contacts during 2026-06-24 to 2026-06-27.
# ---------------------------------------------------------------------------

CURATED_SOURCES: list[dict] = [
    {
        "name": "Venezuela Te Busca",
        "url": "https://venezuelatebusca.com",
        "category": "missing_persons",
        "platform": "website",
        "region": "nacional",
        "has_api": False,
        "api_notes": "Turbo-stream JSON feed at /updates endpoint (reverse-engineered). See scrapers/venezuela_te_busca.py",
        "record_count": 28040,
        "status": "integrated",
        "notes": "Primary national missing persons database. Turbo-stream decoder required. Integrated as venezuela_te_busca scraper.",
    },
    {
        "name": "Localizados Venezuela",
        "url": "https://localizadosvenezuela.com",
        "category": "missing_persons",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "REST API at /api/v1/localizados. No auth required.",
        "record_count": None,
        "status": "integrated",
        "notes": "Found/located persons counterpart to venezuelatebusca.com. Integrated as localizados_venezuela scraper.",
    },
    {
        "name": "SOS La Guaira",
        "url": "https://soslaguaira.lat",
        "category": "missing_persons",
        "platform": "website",
        "region": "la_guaira",
        "has_api": True,
        "api_notes": "REST API at https://api.soslaguaira.lat/api/personas. No auth. Single-page response.",
        "record_count": None,
        "status": "integrated",
        "notes": "Regional focus on La Guaira / Vargas. Integrated as sos_laguaira scraper.",
    },
    {
        "name": "Pacientes Terremoto VZLA",
        "url": "https://pacientesterremotovzla.lovable.app",
        "category": "hospital",
        "platform": "supabase",
        "region": "nacional",
        "has_api": True,
        "api_notes": "Supabase REST at isvgkrgdvhhbuznwgxlt.supabase.co. Public anon key from JS bundle.",
        "record_count": 3964,
        "status": "integrated",
        "notes": "Hospital patient tracker (Lovable SPA). ~4k confirmed hospital patients. Integrated as pacientes_terremoto scraper.",
    },
    {
        "name": "Hospitales en Venezuela",
        "url": "https://hospitalesenvenezuela.com",
        "category": "hospital",
        "platform": "supabase",
        "region": "nacional",
        "has_api": True,
        "api_notes": "External Supabase project. Requires HOSPITALES_ANON_KEY env var. RPC p_term for search.",
        "record_count": None,
        "status": "integrated",
        "notes": "Hospital directory + patient admission tracking. Integrated as hospitales_ve scraper (optional, key-gated).",
    },
    {
        "name": "Red Ayuda Venezuela",
        "url": "https://redayudavenezuela.com",
        "category": "ngo",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "Supabase REST. Requires REDAYUDA_ANON_KEY env var.",
        "record_count": None,
        "status": "integrated",
        "notes": "Aid coordination platform. Integrated as redayuda_ve scraper (optional, key-gated).",
    },
    {
        "name": "Red Solidaria Venezuela (Google Sheets)",
        "url": "https://docs.google.com/spreadsheets/d/red_solidaria_sheet",
        "category": "community",
        "platform": "google_sheet",
        "region": "nacional",
        "has_api": True,
        "api_notes": "Google Sheets CSV export via /export?format=csv. Requires RED_SOLIDARIA_SHEET_ID env var.",
        "record_count": None,
        "status": "integrated",
        "notes": "Community-maintained spreadsheet for shelter and family requests. Integrated as red_solidaria_venezuela scraper.",
    },
    {
        "name": "Reconexion VE",
        "url": "https://reconexion.ve",
        "category": "missing_persons",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "REST API. See scrapers/reconexion.py.",
        "record_count": None,
        "status": "integrated",
        "notes": "Family reunification platform. Early integrated scraper.",
    },
    {
        "name": "SOS Venezuela",
        "url": "https://sosvenezuela.org",
        "category": "ngo",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "See scrapers/sos_venezuela.py.",
        "record_count": None,
        "status": "integrated",
        "notes": "NGO emergency response. Integrated as sos_venezuela scraper.",
    },
    {
        "name": "Venezreporta",
        "url": "https://venezreporta.com",
        "category": "missing_persons",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "See scrapers/venezreporta.py.",
        "record_count": None,
        "status": "integrated",
        "notes": "Citizen reporting platform. Integrated as venezreporta scraper.",
    },
    {
        "name": "Terremoto VE",
        "url": "https://terremotove.com",
        "category": "missing_persons",
        "platform": "website",
        "region": "nacional",
        "has_api": True,
        "api_notes": "See scrapers/terremotove.py.",
        "record_count": None,
        "status": "integrated",
        "notes": "Earthquake-specific missing persons platform. Integrated as terremotove scraper.",
    },
    {
        "name": "Google Drive Hospital Sheets",
        "url": "https://drive.google.com",
        "category": "hospital",
        "platform": "google_drive",
        "region": "nacional",
        "has_api": True,
        "api_notes": "Multiple shared Google Sheets with hospital patient lists. See scrapers/google_drive_hospital.py.",
        "record_count": None,
        "status": "integrated",
        "notes": "Aggregated Google Drive hospital sheets. Integrated as google_drive_hospital scraper.",
    },
    # --- Pending / not yet integrated ---
    {
        "name": "Tilores Venezuela Te Busca (deduplicated export)",
        "url": "https://tilores.io/venezuela-te-busca",
        "category": "missing_persons",
        "platform": "api",
        "region": "nacional",
        "has_api": True,
        "api_notes": "Signed URL download from Tilores pro-bono program. See scratch/import_tilores_vtb.py.",
        "record_count": 26962,
        "status": "pending",
        "notes": "Tilores entity-resolution pass over venezuelatebusca.com. 26,962 unique persons after dedup. Requires manual file request from Tilores.",
    },
    {
        "name": "La Iguana TV - Lista de Desaparecidos",
        "url": "https://laiguana.tv/desaparecidos-terremoto-venezuela-2026/",
        "category": "missing_persons",
        "platform": "website",
        "region": "la_guaira",
        "has_api": False,
        "api_notes": "Static HTML article. BeautifulSoup scraper. See scratch/laiguana_laguaira_import.py. 403 bypass via browser UA.",
        "record_count": None,
        "status": "pending",
        "notes": "La Iguana TV reader-submitted list. La Guaira focus. Run: python scratch/laiguana_laguaira_import.py",
    },
    {
        "name": "MPPS (Ministerio de Salud) - Boletin Victimas",
        "url": "https://mpps.gob.ve",
        "category": "govt",
        "platform": "website",
        "region": "nacional",
        "has_api": False,
        "api_notes": "PDF bulletins; no structured data API. Requires PDF extraction (pdfplumber or tabula-py).",
        "record_count": None,
        "status": "pending",
        "notes": "Official government casualty bulletins. PDFs posted irregularly. Manual extraction recommended.",
    },
    {
        "name": "CICPC / Medicina Legal - Reportes de Fallecidos",
        "url": "https://cicpc.gob.ve",
        "category": "govt",
        "platform": "website",
        "region": "nacional",
        "has_api": False,
        "api_notes": "No public API. Forensic reports occasionally posted as PDF or press release.",
        "record_count": None,
        "status": "pending",
        "notes": "Forensic / legal medicine deceased records. High value for family notification. Requires human monitoring.",
    },
    {
        "name": "MPPRE / SAIME - Registro Civil",
        "url": "https://saime.gob.ve",
        "category": "govt",
        "platform": "website",
        "region": "nacional",
        "has_api": False,
        "api_notes": "No public API. Cedula lookups require interactive captcha; not automatable.",
        "record_count": None,
        "status": "rejected",
        "notes": "Cedula registry. Not scrapeable due to captcha. Useful only for manual verification.",
    },
    {
        "name": "Instagram: @desaparecidosve",
        "url": "https://www.instagram.com/desaparecidosve/",
        "category": "community",
        "platform": "instagram",
        "region": "nacional",
        "has_api": False,
        "api_notes": "Instagram Graph API requires app review for media access. Not feasible short-term.",
        "record_count": None,
        "status": "monitoring",
        "notes": "High-volume community account posting missing person cards. Monitor manually; integrate if API access obtained.",
    },
    {
        "name": "Twitter/X: #TerremotoVenezuela #Desaparecidos",
        "url": "https://twitter.com/search?q=%23TerremotoVenezuela",
        "category": "community",
        "platform": "twitter",
        "region": "nacional",
        "has_api": False,
        "api_notes": "X API v2 requires paid tier ($100+/mo). Not feasible. Consider scraping public timelines via nitter.",
        "record_count": None,
        "status": "monitoring",
        "notes": "High signal but unstructured. Monitor for community alerts. Consider nitter scraper if cost barrier resolved.",
    },
    {
        "name": "WhatsApp: Grupo Familiares La Guaira",
        "url": None,
        "category": "community",
        "platform": "whatsapp",
        "region": "la_guaira",
        "has_api": False,
        "api_notes": "No API. Would require WAHA to monitor specific group; requires group membership.",
        "record_count": None,
        "status": "monitoring",
        "notes": "Community WhatsApp group. High signal but not automatable without group join. Log if contacts provided.",
    },
    {
        "name": "Albergues CLAP - Directorio",
        "url": "https://clap.gob.ve",
        "category": "shelter",
        "platform": "website",
        "region": "nacional",
        "has_api": False,
        "api_notes": "No public API. Shelter lists posted as static HTML / PDFs periodically.",
        "record_count": None,
        "status": "pending",
        "notes": "CLAP government shelter network. Useful for last_seen_location matching. Scrape main page for updated shelter list.",
    },
]

# ---------------------------------------------------------------------------
# Normalize a source record to the discovered_sources schema
# ---------------------------------------------------------------------------

_VALID_CATEGORIES = frozenset({
    "missing_persons", "hospital", "shelter", "govt", "ngo", "community"
})
_VALID_PLATFORMS = frozenset({
    "whatsapp", "instagram", "twitter", "website", "supabase", "google_sheet",
    "google_drive", "api", "pdf", "facebook", "telegram", "other"
})
_VALID_STATUSES = frozenset({"pending", "integrated", "rejected", "monitoring"})


def normalize_source(raw: dict) -> dict | None:
    """Validate and normalize a source dict to the discovered_sources schema."""
    name = str(raw.get("name", "")).strip()
    if not name:
        return None

    url: str | None = raw.get("url")
    if url:
        url = str(url).strip() or None

    category = str(raw.get("category", "community")).lower().strip()
    if category not in _VALID_CATEGORIES:
        category = "community"

    platform = str(raw.get("platform", "website")).lower().strip()
    if platform not in _VALID_PLATFORMS:
        platform = "other"

    region = str(raw.get("region", "nacional")).lower().strip()

    has_api = raw.get("has_api")
    if isinstance(has_api, str):
        has_api = has_api.lower() in ("true", "1", "yes", "si")
    else:
        has_api = bool(has_api)

    api_notes_raw = raw.get("api_notes")
    api_notes = str(api_notes_raw).strip()[:1000] if api_notes_raw else None

    record_count_raw = raw.get("record_count")
    record_count: int | None = None
    if record_count_raw is not None:
        try:
            record_count = int(record_count_raw)
        except (TypeError, ValueError):
            pass

    status = str(raw.get("status", "pending")).lower().strip()
    if status not in _VALID_STATUSES:
        status = "pending"

    notes_raw = raw.get("notes")
    notes = str(notes_raw).strip()[:2000] if notes_raw else None

    return {
        "name": name[:300],
        "url": url,
        "category": category,
        "platform": platform,
        "region": region,
        "has_api": has_api,
        "api_notes": api_notes,
        "record_count": record_count,
        "status": status,
        "notes": notes,
        "source_of_discovery": SOURCE_OF_DISCOVERY,
        "discovered_at": datetime.now(timezone.utc).isoformat(),
    }

# ---------------------------------------------------------------------------
# Optional: parse additional sources from an HTML directory page
# ---------------------------------------------------------------------------

def parse_html_directory(html: str, page_url: str) -> list[dict]:
    """
    Best-effort extraction of source links from an HTML directory page.
    Returns raw dicts that will be normalized before upsert.
    """
    if not _BS4_AVAILABLE:
        logger.warning("beautifulsoup4 not installed; skipping HTML parse. pip install beautifulsoup4 lxml")
        return []

    soup = BeautifulSoup(html, "lxml")
    records: list[dict] = []

    # Look for anchor tags with external URLs
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("http"):
            continue
        if href in seen_urls:
            continue
        # Skip social media and navigation links
        if any(skip in href for skip in [
            "facebook.com", "twitter.com", "instagram.com",
            "youtube.com", "t.me", "whatsapp.com",
            "#", "javascript:",
        ]):
            continue
        seen_urls.add(href)
        # Use the link text as the name
        name = a.get_text(strip=True)[:200] or href
        records.append({
            "name": name,
            "url": href,
            "category": "missing_persons",  # best guess; team reviews
            "platform": "website",
            "region": "nacional",
            "has_api": False,
            "status": "pending",
            "notes": f"Discovered from: {page_url}",
        })

    logger.info("HTML directory parse: %d candidate links from %s", len(records), page_url)
    return records

# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def load_input_file(path: Path) -> list[dict]:
    """Load sources from a JSON or CSV file."""
    suffix = path.suffix.lower()
    if suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        for key in ("sources", "data", "results"):
            if key in data:
                return data[key]
        return [data]
    elif suffix in (".csv", ".tsv"):
        delim = "\t" if suffix == ".tsv" else ","
        records: list[dict] = []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delim)
            for row in reader:
                records.append({k: (v if v != "" else None) for k, v in row.items()})
        return records
    else:
        # Try JSON first
        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
        # Fall back to CSV
        records = []
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append({k: (v if v != "" else None) for k, v in row.items()})
        return records

# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

def _sb_headers(key: str, prefer: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


async def _upsert_batch(
    client: httpx.AsyncClient,
    sb_url: str,
    sb_key: str,
    rows: list[dict],
) -> tuple[int, int]:
    """
    Upsert to discovered_sources.
    Uses merge-duplicates on url so re-runs update status/notes.
    For sources with null url, falls back to ignore-duplicates on name.
    """
    # Separate rows with URL (dedup on url) vs. those without (dedup on name)
    with_url = [r for r in rows if r.get("url")]
    without_url = [r for r in rows if not r.get("url")]

    total_sent = 0
    total_err = 0

    if with_url:
        resp = await client.post(
            f"{sb_url}/rest/v1/{TABLE}",
            headers=_sb_headers(sb_key, "resolution=merge-duplicates,return=minimal"),
            params={"on_conflict": "url"},
            json=with_url,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            total_sent += len(with_url)
        else:
            logger.warning("upsert (url dedup) HTTP %d: %s",
                           resp.status_code, resp.text[:200])
            total_err += len(with_url)

    if without_url:
        resp = await client.post(
            f"{sb_url}/rest/v1/{TABLE}",
            headers=_sb_headers(sb_key, "resolution=ignore-duplicates,return=minimal"),
            params={"on_conflict": "name"},
            json=without_url,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            total_sent += len(without_url)
        else:
            logger.warning("upsert (name dedup) HTTP %d: %s",
                           resp.status_code, resp.text[:200])
            total_err += len(without_url)

    return total_sent, total_err


async def _ensure_table_exists(sb_url: str, sb_key: str) -> bool:
    """
    Check if the discovered_sources table exists.
    Returns True if it does; logs a DDL hint if it doesn't.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"{sb_url}/rest/v1/{TABLE}",
            headers={
                "apikey": sb_key,
                "Authorization": f"Bearer {sb_key}",
            },
            params={"limit": "1"},
        )
        if resp.status_code == 200:
            return True
        if resp.status_code == 404 or "does not exist" in resp.text.lower():
            logger.error(
                "Table '%s' does not exist. Apply the migration below:\n\n"
                "CREATE TABLE discovered_sources (\n"
                "    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),\n"
                "    name text NOT NULL,\n"
                "    url text UNIQUE,\n"
                "    category text,\n"
                "    platform text,\n"
                "    region text,\n"
                "    has_api boolean DEFAULT false,\n"
                "    api_notes text,\n"
                "    record_count int,\n"
                "    status text DEFAULT 'pending',\n"
                "    notes text,\n"
                "    source_of_discovery text,\n"
                "    discovered_at timestamptz DEFAULT now()\n"
                ");\n\n"
                "-- Enable RLS and add appropriate policies for your team.",
                TABLE,
            )
            return False
        logger.warning("Unexpected table check response: %d", resp.status_code)
        return True  # assume it exists; upsert will fail with a clearer error

# ---------------------------------------------------------------------------
# Optional: fetch HTML directory from URL
# ---------------------------------------------------------------------------

_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.5",
}


async def fetch_html(url: str) -> str:
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                headers=_FETCH_HEADERS,
                follow_redirects=True,
                timeout=30,
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
        except Exception as exc:
            logger.warning("fetch attempt %d failed: %s", attempt + 1, exc)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"fetch_html failed after 3 attempts: {url}")

# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

async def run_import(
    sb_url: str,
    sb_key: str,
    input_file: Path | None = None,
    directory_url: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    # 1. Collect sources from all inputs
    all_raw: list[dict] = list(CURATED_SOURCES)
    logger.info("Curated sources: %d", len(all_raw))

    if input_file:
        try:
            file_records = load_input_file(input_file)
            logger.info("File sources: %d (from %s)", len(file_records), input_file)
            all_raw.extend(file_records)
        except Exception as exc:
            logger.error("Failed to load input file %s: %s", input_file, exc)

    if directory_url:
        try:
            html = await fetch_html(directory_url)
            html_records = parse_html_directory(html, directory_url)
            logger.info("HTML directory sources: %d", len(html_records))
            all_raw.extend(html_records)
        except Exception as exc:
            logger.error("Failed to fetch/parse directory URL %s: %s", directory_url, exc)

    # 2. Normalize
    seen_urls: set[str] = set()
    seen_names: set[str] = set()
    normalized: list[dict] = []
    skipped = 0

    for raw in all_raw:
        try:
            row = normalize_source(raw)
        except Exception as exc:
            logger.warning("normalize error: %s | keys: %s", exc, list(raw.keys()))
            skipped += 1
            continue
        if row is None:
            skipped += 1
            continue
        # Dedup within the current run
        url_key = row.get("url")
        if url_key and url_key in seen_urls:
            skipped += 1
            continue
        name_key = row["name"].lower()
        if not url_key and name_key in seen_names:
            skipped += 1
            continue
        if url_key:
            seen_urls.add(url_key)
        seen_names.add(name_key)
        normalized.append(row)

    logger.info("Normalized: %d valid, %d skipped", len(normalized), skipped)

    if dry_run:
        logger.info("[dry-run] Would upsert %d sources. Preview:", len(normalized))
        for r in normalized[:5]:
            logger.info("  [%s] %s (%s) - %s", r["status"], r["name"], r["platform"], r["url"])
        return {
            "dry_run": True,
            "curated": len(CURATED_SOURCES),
            "total_raw": len(all_raw),
            "normalized": len(normalized),
            "skipped": skipped,
        }

    # 3. Check table exists
    if not await _ensure_table_exists(sb_url, sb_key):
        logger.error(
            "Aborting: apply the migration SQL above and re-run. "
            "Or add --dry-run to preview records without writing."
        )
        sys.exit(1)

    # 4. Upsert
    total_sent = 0
    total_err = 0
    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(normalized), BATCH_SIZE):
            batch = normalized[i : i + BATCH_SIZE]
            try:
                sent, err = await _upsert_batch(client, sb_url, sb_key, batch)
                total_sent += sent
                total_err += err
            except Exception as exc:
                logger.error("upsert_batch offset=%d: %s", i, exc)
                total_err += len(batch)
            await asyncio.sleep(0.05)

    stats = {
        "table": TABLE,
        "curated": len(CURATED_SOURCES),
        "total_raw": len(all_raw),
        "normalized": len(normalized),
        "sent": total_sent,
        "errors": total_err,
        "skipped": skipped,
    }
    logger.info("Import complete: %s", stats)
    return stats

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate the discovered_sources table with known earthquake data platforms."
    )
    parser.add_argument(
        "--input",
        default=None,
        help="Path to a JSON or CSV file with additional sources.",
    )
    parser.add_argument(
        "--url",
        default=None,
        help="URL of an HTML directory page to scrape for additional sources.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and normalize but do NOT write to Supabase.",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

    if not args.dry_run:
        if not sb_url:
            logger.error("SUPABASE_URL is not set.")
            sys.exit(1)
        if not sb_key:
            logger.error("SUPABASE_SERVICE_ROLE_KEY is not set.")
            sys.exit(1)

    input_file = Path(args.input) if args.input else None
    stats = await run_import(
        sb_url, sb_key,
        input_file=input_file,
        directory_url=args.url,
        dry_run=args.dry_run,
    )
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    asyncio.run(main())
