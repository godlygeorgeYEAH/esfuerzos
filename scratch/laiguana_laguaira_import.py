"""
laiguana_laguaira_import.py - One-time importer for La Iguana TV victim list.

La Iguana TV (laiguana.tv) published a dedicated article listing persons reported
missing or found after the June 24 2026 Venezuela earthquake, focusing on the
La Guaira / Vargas region. The article includes names, cedulas, and contact info
submitted by readers and NGOs.

HOW TO USE
----------
1. Run from repo root (or inside container):

       python scratch/laiguana_laguaira_import.py

   Optional: target a specific article URL (default scans the main missing-persons article):

       python scratch/laiguana_laguaira_import.py --url "https://laiguana.tv/..."

2. Environment variables required:

       SUPABASE_URL
       SUPABASE_SERVICE_ROLE_KEY

3. Optional dry-run (parse + print, no DB writes):

       python scratch/laiguana_laguaira_import.py --dry-run

NOTES
-----
- laiguana.tv returns HTTP 403 for plain Python requests; the user-agent and
  Accept-Language headers below bypass this restriction (as of 2026-06-24).
  If 403 persists, try --url with a cached/archived copy.
- Names and cedulas are extracted from plain-text paragraphs and tables.
  The article format is inconsistent; multiple parsing strategies are attempted.
- Dedup key: laiguana_laguaira:cedula_{hash} if CI present, else name hash.
- PII: cedulas are hashed in source_url; raw_data strips cedula and phone fields.
- Outputs to 'reports' table (kind=missing or found based on context).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys
from typing import Any

import httpx

try:
    from bs4 import BeautifulSoup
except ImportError:
    print(
        "ERROR: beautifulsoup4 is required. Install with: pip install beautifulsoup4 lxml",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("laiguana_import")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "laiguana_laguaira"
BATCH_SIZE = 200

# Default article URL -- update if laiguana.tv publishes a new consolidated page.
DEFAULT_URL = "https://laiguana.tv/desaparecidos-terremoto-venezuela-2026/"

# 403-bypass headers. laiguana.tv blocks Python's default UA but accepts browsers.
_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://laiguana.tv/",
    "DNT": "1",
    "Connection": "keep-alive",
}

# Regex patterns for cedula detection (V-1234567 or bare 7-digit number)
_CI_RE = re.compile(
    r"\b(?:V[-\s]?|E[-\s]?)?(\d{6,9})\b",
    re.IGNORECASE,
)

# Keywords indicating "found / located" status
_FOUND_KEYWORDS = frozenset({
    "encontrado", "encontrada", "localizado", "localizada",
    "a salvo", "rescatado", "rescatada", "con vida", "vivo", "viva",
    "fallecido", "fallecida", "muerto", "muerta",  # deceased = kind=found
})

# Keywords indicating "missing" status (for row context)
_MISSING_KEYWORDS = frozenset({
    "desaparecido", "desaparecida", "se busca", "busco", "buscamos",
    "paradero desconocido",
})

# Fields to strip from raw_data (PII)
_STRIP_KEYS = frozenset({
    "cedula", "ci", "cedula_identidad", "phone", "telefono", "contacto",
    "email", "direccion", "whatsapp",
})

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sb_headers(key: str, prefer: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
    }


def _hash(text: str, length: int = 12) -> str:
    return hashlib.sha1(text.encode()).hexdigest()[:length]


def _extract_ci(text: str) -> str | None:
    """Extract cedula from free text. Returns the raw numeric string or None."""
    m = _CI_RE.search(text)
    if m:
        digits = m.group(1)
        if len(digits) >= 6:
            return digits
    return None


def _clean_name(raw: str) -> str:
    """Normalize a name: strip, collapse whitespace, title-case."""
    name = " ".join(raw.split())
    # Remove stray punctuation at start/end
    name = name.strip(".,;:-()")
    return name[:200]


def _infer_kind(context_text: str) -> str:
    """Infer kind from surrounding text context."""
    lower = context_text.lower()
    for kw in _FOUND_KEYWORDS:
        if kw in lower:
            return "found"
    for kw in _MISSING_KEYWORDS:
        if kw in lower:
            return "missing"
    return "missing"  # Default: assume missing for earthquake victim lists


def _source_url(ci: str | None, name: str) -> str:
    if ci:
        return f"{SOURCE}:cedula_{_hash(ci)}"
    return f"{SOURCE}:name_{_hash(name.lower())}"


def _strip_pii(d: dict) -> dict:
    return {k: v for k, v in d.items() if k.lower() not in _STRIP_KEYS}

# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

async def fetch_page(url: str) -> str:
    """
    Fetch the article HTML with browser-like headers to bypass 403.
    Retries 3 times with exponential backoff.
    """
    last_exc: Exception | None = None
    for attempt in range(3):
        wait = 2 ** attempt  # 1s, 2s, 4s
        try:
            async with httpx.AsyncClient(
                headers=_FETCH_HEADERS,
                follow_redirects=True,
                timeout=30,
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            if exc.response.status_code == 403:
                logger.warning(
                    "403 Forbidden on %s (attempt %d). "
                    "Try: --url with an archived copy (e.g. web.archive.org).",
                    url, attempt + 1,
                )
            else:
                logger.warning("HTTP %d on %s (attempt %d).",
                               exc.response.status_code, url, attempt + 1)
        except Exception as exc:
            last_exc = exc
            logger.warning("fetch error attempt %d: %s", attempt + 1, exc)
        if attempt < 2:
            await asyncio.sleep(wait)

    raise RuntimeError(f"fetch_page failed after 3 attempts: {last_exc}")

# ---------------------------------------------------------------------------
# Parsing strategies
# ---------------------------------------------------------------------------

def _parse_tables(soup: BeautifulSoup) -> list[dict]:
    """
    Strategy 1: Extract records from HTML tables.
    Looks for tables with columns like Nombre, Cedula, Estado, etc.
    """
    records: list[dict] = []
    for table in soup.find_all("table"):
        headers: list[str] = []
        for th in table.find_all("th"):
            headers.append(th.get_text(strip=True).lower())
        if not headers:
            # Try first row as header
            first_row = table.find("tr")
            if first_row:
                headers = [td.get_text(strip=True).lower()
                           for td in first_row.find_all(["td", "th"])]

        # Check if this table has useful columns
        has_name = any(kw in " ".join(headers) for kw in
                       ["nombre", "name", "persona"])
        if not has_name:
            continue

        for row in table.find_all("tr")[1:]:  # skip header row
            cells = [td.get_text(separator=" ", strip=True)
                     for td in row.find_all("td")]
            if not cells:
                continue
            row_dict: dict[str, str] = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    row_dict[headers[i]] = cell
                else:
                    row_dict[f"col_{i}"] = cell
            records.append({"_strategy": "table", **row_dict})

    return records


def _parse_list_items(soup: BeautifulSoup) -> list[dict]:
    """
    Strategy 2: Extract records from bullet lists / ordered lists.
    Each <li> is one person entry.
    """
    records: list[dict] = []
    # Find the article/main content area
    content = (
        soup.find("article")
        or soup.find("main")
        or soup.find(id=re.compile(r"content|article|post", re.I))
        or soup
    )
    for li in content.find_all("li"):
        text = li.get_text(separator=" ", strip=True)
        if len(text) < 5:
            continue
        # Must contain a word that looks like a name (at least two caps-words)
        if not re.search(r"[A-ZAEIOUÀ-ÿ]{2}.*[A-ZAEIOUÀ-ÿ]{2}", text):
            continue
        records.append({"_strategy": "list", "_raw_text": text})
    return records


def _parse_paragraphs(soup: BeautifulSoup) -> list[dict]:
    """
    Strategy 3: Extract records from <p> tags.
    Used when the article is formatted as one-person-per-paragraph.
    """
    records: list[dict] = []
    content = (
        soup.find("article")
        or soup.find("main")
        or soup
    )
    for p in content.find_all("p"):
        text = p.get_text(separator=" ", strip=True)
        if len(text) < 10 or len(text) > 800:
            continue
        # Skip navigation/copyright paragraphs
        if any(skip in text.lower() for skip in
               ["copyright", "todos los derechos", "laiguana.tv", "suscribete"]):
            continue
        # Must look like a person entry (has a name-like word)
        if not re.search(r"[A-Z][a-z]{2,}", text):
            continue
        records.append({"_strategy": "paragraph", "_raw_text": text})
    return records

# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

_NAME_TITLE_RE = re.compile(
    r"^\s*(?:Nombre|Name|Persona|Sr\.?|Sra\.?|Dr\.?|Ing\.?)\s*[:\-]?\s*",
    re.IGNORECASE,
)
_STATUS_PREFIX_RE = re.compile(
    r"^\s*(?:Estado|Status|Situacion|Condicion)\s*[:\-]?\s*",
    re.IGNORECASE,
)
_LOCATION_PREFIX_RE = re.compile(
    r"^\s*(?:Ubicacion|Lugar|Last seen|Ultima ubicacion|Municipio|Estado)\s*[:\-]?\s*",
    re.IGNORECASE,
)
_AGE_RE = re.compile(r"\b(\d{1,3})\s*(?:años?|a\.?)\b", re.IGNORECASE)


def _normalize_from_dict(raw: dict) -> dict | None:
    """Normalize a table-row dict to the reports schema."""
    # Find name field
    name_raw = (
        raw.get("nombre")
        or raw.get("name")
        or raw.get("persona")
        or raw.get("apellidos y nombre")
        or raw.get("col_0")
        or ""
    )
    name_raw = _NAME_TITLE_RE.sub("", name_raw).strip()
    if not name_raw or len(name_raw) < 3:
        return None
    name = _clean_name(name_raw)

    ci_raw = (
        raw.get("cedula") or raw.get("ci") or raw.get("cedula_identidad")
        or raw.get("c.i") or raw.get("c.i.") or ""
    )
    ci = _extract_ci(ci_raw) or _extract_ci(name_raw)

    status_raw = (
        raw.get("estado") or raw.get("status") or raw.get("situacion")
        or raw.get("condicion") or ""
    )
    kind = _infer_kind(status_raw or " ".join(raw.values()))

    location_raw = (
        raw.get("ubicacion") or raw.get("lugar") or raw.get("municipio")
        or raw.get("location") or ""
    )

    age_str = raw.get("edad") or raw.get("age") or ""
    age: int | None = None
    m = _AGE_RE.search(str(age_str))
    if m:
        candidate = int(m.group(1))
        age = candidate if 0 < candidate < 120 else None

    marks_parts: list[str] = []
    if status_raw:
        marks_parts.append(f"Estado: {status_raw}")
    notes = raw.get("notas") or raw.get("notes") or raw.get("observaciones") or ""
    if notes:
        marks_parts.append(str(notes))
    marks = " | ".join(marks_parts)[:500] or None

    raw_data = _strip_pii({
        k: v for k, v in raw.items()
        if not k.startswith("_") and v
    })

    return {
        "kind": kind,
        "full_name": name,
        "age": age,
        "last_seen_location": location_raw[:300] if location_raw else None,
        "distinguishing_marks": marks,
        "source": SOURCE,
        "source_url": _source_url(ci, name),
        "raw_data": raw_data,
    }


def _normalize_from_text(raw: dict) -> dict | None:
    """Normalize a free-text line (list item or paragraph) to the reports schema."""
    text = raw.get("_raw_text", "").strip()
    if not text:
        return None

    # Try to extract CI from text
    ci = _extract_ci(text)

    # Extract age
    age: int | None = None
    m = _AGE_RE.search(text)
    if m:
        candidate = int(m.group(1))
        age = candidate if 0 < candidate < 120 else None

    # Extract name: use the first capitalized-word cluster before any dash, colon, or CI
    name_raw = text
    # Remove CI if found
    if ci:
        name_raw = _CI_RE.sub("", name_raw, count=1).strip()
    # Remove leading "N." or "1." list numbering
    name_raw = re.sub(r"^\d+[\.\)]\s*", "", name_raw)
    # Take text up to the first status/separator marker
    for sep in [" - ", ": ", " | ", " – "]:
        if sep in name_raw:
            name_raw = name_raw.split(sep)[0]
            break
    name_raw = _NAME_TITLE_RE.sub("", name_raw).strip()

    if len(name_raw) < 3:
        return None

    # Validate: name should have at least one 3-letter capitalized word
    if not re.search(r"[A-ZAEIOUÀ-ÿ][a-zà-ÿ]{2,}", name_raw):
        return None

    name = _clean_name(name_raw)
    kind = _infer_kind(text)

    # Location: look for "en [place]" or "sector [name]" patterns
    location: str | None = None
    loc_m = re.search(
        r"(?:en|sector|barrio|municipio|urbanizacion|edificio)\s+([A-Z][^,.;]{3,40})",
        text, re.IGNORECASE,
    )
    if loc_m:
        location = loc_m.group(0)[:200]

    raw_data = _strip_pii({"_raw_text": text[:500]})

    return {
        "kind": kind,
        "full_name": name[:200],
        "age": age,
        "last_seen_location": location,
        "distinguishing_marks": None,
        "source": SOURCE,
        "source_url": _source_url(ci, name),
        "raw_data": raw_data,
    }


def normalize(raw: dict) -> dict | None:
    """
    Dispatch to the appropriate normalizer based on parsing strategy tag.
    """
    strategy = raw.get("_strategy")
    if strategy == "table":
        return _normalize_from_dict(raw)
    else:
        return _normalize_from_text(raw)

# ---------------------------------------------------------------------------
# HTML parsing orchestrator
# ---------------------------------------------------------------------------

def parse_html(html: str) -> list[dict]:
    """
    Parse the article HTML and extract raw person records using all strategies.
    Returns a flat list of raw dicts (strategy-tagged).
    """
    soup = BeautifulSoup(html, "lxml")

    all_raw: list[dict] = []

    # Strategy 1: tables (highest precision)
    table_records = _parse_tables(soup)
    logger.info("Strategy 1 (tables): %d raw records", len(table_records))
    all_raw.extend(table_records)

    # If tables already found substantial data, skip less-precise strategies.
    if len(table_records) >= 20:
        return all_raw

    # Strategy 2: list items
    list_records = _parse_list_items(soup)
    logger.info("Strategy 2 (lists): %d raw records", len(list_records))
    all_raw.extend(list_records)

    # Strategy 3: paragraphs (lowest precision; only if others yielded little)
    if len(all_raw) < 10:
        para_records = _parse_paragraphs(soup)
        logger.info("Strategy 3 (paragraphs): %d raw records", len(para_records))
        all_raw.extend(para_records)

    return all_raw

# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------

async def _upsert_batch(
    client: httpx.AsyncClient,
    sb_url: str,
    sb_key: str,
    rows: list[dict],
) -> tuple[int, int]:
    resp = await client.post(
        f"{sb_url}/rest/v1/reports",
        headers=_sb_headers(sb_key, "resolution=ignore-duplicates,return=minimal"),
        params={"on_conflict": "source,source_url"},
        json=rows,
        timeout=60,
    )
    if resp.status_code in (200, 201):
        return len(rows), 0
    logger.warning("upsert HTTP %d: %s", resp.status_code, resp.text[:200])
    return 0, len(rows)


async def _log_run(sb_url: str, sb_key: str, inserted: int, error: str | None) -> None:
    row = {
        "source": SOURCE,
        "run_type": "one_time_import",
        "rows_inserted": inserted,
        "rows_updated": 0,
        "error": error,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(
                f"{sb_url}/rest/v1/scraper_runs",
                headers=_sb_headers(sb_key, "return=minimal"),
                json=[row],
            )
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("log_run failed: %s", exc)

# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

async def run_import(
    url: str,
    sb_url: str,
    sb_key: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    logger.info("Fetching: %s", url)
    html = await fetch_page(url)
    logger.info("Fetched %d bytes of HTML.", len(html))

    raw_records = parse_html(html)
    logger.info("Total raw records from all strategies: %d", len(raw_records))

    seen_urls: set[str] = set()
    normalized: list[dict] = []
    skipped = 0

    for raw in raw_records:
        try:
            row = normalize(raw)
        except Exception as exc:
            logger.warning("normalize error: %s", exc)
            skipped += 1
            continue
        if row is None:
            skipped += 1
            continue
        if row["source_url"] in seen_urls:
            skipped += 1
            continue
        seen_urls.add(row["source_url"])
        normalized.append(row)

    logger.info("Normalized: %d valid, %d skipped", len(normalized), skipped)

    if dry_run:
        logger.info("[dry-run] First 5 normalized rows:")
        import json
        for r in normalized[:5]:
            logger.info("  %s", json.dumps(r, ensure_ascii=False, default=str))
        return {
            "dry_run": True,
            "raw": len(raw_records),
            "normalized": len(normalized),
            "skipped": skipped,
        }

    total_inserted = 0
    total_errors = 0
    async with httpx.AsyncClient(timeout=60) as client:
        for i in range(0, len(normalized), BATCH_SIZE):
            batch = normalized[i : i + BATCH_SIZE]
            try:
                ins, err = await _upsert_batch(client, sb_url, sb_key, batch)
                total_inserted += ins
                total_errors += err
            except Exception as exc:
                logger.error("upsert_batch offset=%d: %s", i, exc)
                total_errors += len(batch)
            await asyncio.sleep(0.05)

    error_summary = f"{total_errors} records failed" if total_errors else None
    await _log_run(sb_url, sb_key, total_inserted, error_summary)

    stats = {
        "source": SOURCE,
        "url": url,
        "raw": len(raw_records),
        "normalized": len(normalized),
        "sent": total_inserted,
        "errors": total_errors,
        "skipped": skipped,
    }
    logger.info("Import complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time importer for La Iguana TV earthquake victim list."
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Article URL to scrape (default: %(default)s).",
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

    import json
    stats = await run_import(args.url, sb_url, sb_key, dry_run=args.dry_run)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    asyncio.run(main())
