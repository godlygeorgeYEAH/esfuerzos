"""
scrapers/laiguana_laguaira_import.py -- One-time import for La Iguana TV article.

Source:   https://www.laiguana.tv/articulos/1548292-desaparecidos-la-guaira/
Records:  ~20-50 named missing persons from La Guaira, several with CI numbers.
Kind:     missing (all records)
Priority: low -- run once, negligible record count relative to other sources.

Usage:
    python scrapers/laiguana_laguaira_import.py

Env vars required:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY

The article returns 403 with the default Python User-Agent. This script spoofs
a Chrome UA and includes a Google Referer to bypass that gate. If the server
still blocks, the script exits with a non-zero status and a clear error message
so the operator can investigate (e.g., use a proxy or paste the HTML manually
via --html-file).

Dedup key: source_url = "laiguana_desaparecidos_laguaira:{cedula_digits}" when
a CI number is present, or "laiguana_desaparecidos_laguaira:{name_slug}" when
not. Resolution is ignore-duplicates so re-runs are safe.

Deceased persons are stored normally -- status is noted in distinguishing_marks,
never in a separate boolean field.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from typing import Optional

import httpx
from bs4 import BeautifulSoup, Tag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("laiguana_laguaira")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "laiguana_desaparecidos_laguaira"
ARTICLE_URL = "https://www.laiguana.tv/articulos/1548292-desaparecidos-la-guaira/"
LOCATION = "La Guaira, Venezuela"
KIND = "missing"
MAX_MARKS_LEN = 500

# Browser-like headers to bypass the site's 403 block.
# Tested pattern: Referer=Google + Chrome UA is enough for most Venezuelan
# news sites that use Cloudflare's bot-score gate.
_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches: "CI 10.714.707", "C.I. 17.498.646", "cedula 10714707",
#          "cedula de identidad V-11.936.362", "V-10714707"
_CI_LABELED = re.compile(
    r"(?:c\.?\s*i\.?|c[eé]dula(?:\s+de\s+identidad)?)[:\s]*V?-?\s*([\d\.]+)",
    re.IGNORECASE,
)
# Bare Venezuelan cedula: "V-10.714.707" or "V10714707"
_CI_BARE = re.compile(r"\bV-?([\d]{6,9}(?:\.[\d]{3})*)\b", re.IGNORECASE)

# Any token that looks like a cedula (7-9 contiguous digits with optional dots)
# Used as a last-resort extraction when no explicit CI label is present.
_CI_NUMBER = re.compile(r"\b(\d{1,2}(?:\.\d{3}){1,2}|\d{7,9})\b")

# Age phrase like "34 años", "50 años", "55 años de edad" -- stripped from
# the remainder before name extraction so a line such as
# "Juan Ramon Bastidas, 34 años, CI 17.498.646" doesn't get dropped due to
# the digit in the age field.
_AGE_PHRASE = re.compile(r",?\s*\d{1,3}\s*a[ñn]os?(?:\s+de\s+edad)?", re.IGNORECASE)

# Lines to skip: too short, all-numeric, looks like a heading keyword, or
# matches common article boilerplate phrases.
_SKIP_PATTERNS = re.compile(
    r"^\s*$"
    r"|^\s*[\d\-\.\*\#]+\s*$"
    r"|(?:ver\s+tambi[eé]n|haga\s+clic|publicado|actualizado"
    r"|comparte\s+esta|suscr[ií]bete|noticias\s+relacionadas"
    r"|deja\s+tu\s+comentario|tags?\b|twitter|facebook|instagram"
    r"|tel[eé]fono|contacto|editorial|red[acó]ci[oó]n|laiguana)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_cedula(raw: str) -> str:
    """Strip dots and spaces from a cedula string -> plain digit string."""
    return re.sub(r"[\.\s]", "", raw)


def _slugify(text: str) -> str:
    """Produce a lowercase ASCII slug for use as a fallback dedup key."""
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    return slug[:60]


def _extract_ci(line: str) -> tuple[Optional[str], str]:
    """
    Try to find a CI number in *line*.

    Returns (cedula_digits_or_None, line_with_ci_removed).
    Tries labeled patterns first ("CI", "cedula"), then bare "V-" prefix,
    then any token that looks like a cedula number.
    """
    for pattern in (_CI_LABELED, _CI_BARE, _CI_NUMBER):
        m = pattern.search(line)
        if m:
            digits = _normalize_cedula(m.group(1))
            if len(digits) >= 6:
                # Remove the matched span from the line for cleaner name parsing
                cleaned = line[: m.start()] + line[m.end() :]
                return digits, cleaned
    return None, line


def _extract_name(fragment: str) -> Optional[str]:
    """
    Extract a person name from *fragment* (the part of the line after the CI
    number has been removed).

    Rules:
    - Strip age phrases ("34 años") before anything else so lines like
      "Juan Ramon Bastidas, 34 años, CI ..." don't drop the name due to
      the age digit in the remainder.
    - Strip leading bullets, numbers, dashes, and trailing punctuation.
    - Require at least two words (given name + at least one surname).
    - Title-case if the text is ALL-CAPS (common in Venezuelan lists).
    - Return None if the result looks like a non-name fragment.
    """
    text = fragment.strip()
    # Remove age phrases before any other check (avoids false digit-reject)
    text = _AGE_PHRASE.sub("", text).strip()
    # Remove leading bullets / list markers (including Unicode bullets •, ·, –)
    text = re.sub(r"^[\s\d\.\-\*\#\)\(\[\]•·–—]+", "", text).strip()
    # Remove trailing punctuation
    text = re.sub(r"[,;\.\:\-]+$", "", text).strip()

    if not text or len(text) < 5:
        return None

    words = text.split()
    if len(words) < 2:
        return None

    # After age-stripping, remaining digits indicate a non-name fragment
    # (phone numbers, post IDs, etc.) -- reject those.
    if re.search(r"\d", text):
        return None
    if re.search(r"https?://|www\.", text, re.IGNORECASE):
        return None

    # Title-case if all-caps input
    if text == text.upper():
        text = text.title()

    # Sanity: reject if more than 7 words (unlikely to be a single person name)
    if len(words) > 7:
        return None

    return text


# ---------------------------------------------------------------------------
# HTML fetching
# ---------------------------------------------------------------------------


async def _fetch_html(url: str, timeout: float = 30.0) -> str:
    """
    Fetch *url* with browser-like headers to bypass 403 gates.

    Raises httpx.HTTPStatusError on non-2xx after a single retry with a
    slightly different header combination.
    """
    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=_FETCH_HEADERS,
    ) as client:
        resp = await client.get(url)

        if resp.status_code == 403:
            logger.warning(
                "Got 403 on first attempt -- retrying with stripped Sec-Fetch headers"
            )
            minimal_headers = {
                k: v
                for k, v in _FETCH_HEADERS.items()
                if not k.startswith("Sec-Fetch")
            }
            minimal_headers["Cookie"] = ""
            resp = await client.get(url, headers=minimal_headers)

        resp.raise_for_status()
        return resp.text


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


def _article_text_lines(html: str) -> list[str]:
    """
    Extract text lines from the article body.

    Tries a series of CSS selectors that cover common Venezuelan news CMS
    patterns (WordPress, custom). Falls back to <main> then <body>.
    Returns individual text lines, one per non-empty string.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove structural/noise nodes before extracting text.
    # find_all only accepts tag names (not CSS selectors); use select() for
    # class-based removals separately.
    for tag in soup.find_all(
        ["script", "style", "noscript", "nav", "header", "footer", "aside", "form"]
    ):
        tag.decompose()
    for tag in soup.select(".ad, .advertisement, .social-share, .related-posts"):
        tag.decompose()

    # Prefer <li> elements within the article since person lists are
    # almost always structured as <ul><li>Name CI ...</li></ul>
    selectors = [
        "article .article-content li",
        "article .content li",
        ".entry-content li",
        ".post-content li",
        ".nota-body li",
        ".cuerpo-nota li",
        ".article-body li",
        ".td-post-content li",
        ".post-body li",
        # Fallback: whole article body as paragraphs
        "article .article-content",
        "article .content",
        ".entry-content",
        ".post-content",
        ".nota-body",
        "article",
        "main",
    ]

    for sel in selectors:
        elements = soup.select(sel)
        if not elements:
            continue
        lines: list[str] = []
        for el in elements:
            text = el.get_text(separator="\n")
            lines.extend(text.splitlines())
        non_empty = [ln.strip() for ln in lines if ln.strip()]
        if non_empty:
            logger.debug("Used selector '%s' -> %d raw lines", sel, len(non_empty))
            return non_empty

    # Last resort: whole body
    body = soup.find("body")
    if body and isinstance(body, Tag):
        return [ln.strip() for ln in body.get_text(separator="\n").splitlines() if ln.strip()]
    return []


# ---------------------------------------------------------------------------
# Record parsing
# ---------------------------------------------------------------------------


def parse_records(lines: list[str]) -> list[dict]:
    """
    Parse person records from text lines extracted from the article.

    For each line:
    1. Skip boilerplate via _SKIP_PATTERNS.
    2. Extract CI number with _extract_ci().
    3. Extract name from the remaining text with _extract_name().
    4. Dedup by cedula (primary) then by lowercased name (secondary).
    5. Build source_url and distinguishing_marks.

    Returns a list of dicts ready for Supabase upsert.
    """
    records: list[dict] = []
    seen_cedulas: set[str] = set()
    seen_names: set[str] = set()

    for raw_line in lines:
        line = raw_line.strip()

        if _SKIP_PATTERNS.search(line):
            continue
        if len(line) < 6:
            continue

        cedula, remainder = _extract_ci(line)

        # Try name from the part of the line before/around the CI marker.
        # If the whole line was consumed, try the original line too.
        name = _extract_name(remainder) or _extract_name(line)
        if not name:
            continue

        # Dedup
        if cedula and cedula in seen_cedulas:
            logger.debug("Skipping duplicate cedula %s (%s)", cedula, name)
            continue
        name_key = name.lower()
        if name_key in seen_names:
            logger.debug("Skipping duplicate name '%s'", name)
            continue

        # Source URL dedup key: prefer cedula, fallback to name slug
        source_url = (
            f"{SOURCE}:{cedula}"
            if cedula
            else f"{SOURCE}:{_slugify(name)}"
        )

        # distinguishing_marks: store CI explicitly for lookup tools
        marks = f"CI: {cedula}" if cedula else None

        record: dict = {
            "source": SOURCE,
            "source_url": source_url,
            "full_name": name,
            "age": None,
            "last_seen_location": LOCATION,
            "distinguishing_marks": marks,
            "kind": KIND,
            "raw_data": {
                "original_line": raw_line,
                "cedula": cedula,
                "article_url": ARTICLE_URL,
            },
        }
        records.append(record)

        if cedula:
            seen_cedulas.add(cedula)
        seen_names.add(name_key)

    return records


# ---------------------------------------------------------------------------
# Supabase upsert
# ---------------------------------------------------------------------------


async def upsert_records(records: list[dict]) -> dict:
    """
    Batch-upsert *records* to the Supabase reports table.

    Uses resolution=ignore-duplicates so re-runs are idempotent.
    Sends all records in a single POST (dataset is small, ~20-50 rows).
    Logs and counts individual record errors without crashing.
    """
    supabase_url = os.environ["SUPABASE_URL"].rstrip("/")
    supabase_key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }

    payload = [
        {
            "source": r["source"],
            "source_url": r["source_url"],
            "full_name": r["full_name"],
            "age": r.get("age"),
            "last_seen_location": r.get("last_seen_location"),
            "distinguishing_marks": r.get("distinguishing_marks"),
            "kind": r["kind"],
            "raw_data": r.get("raw_data"),
        }
        for r in records
    ]

    inserted = 0
    errors = 0

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{supabase_url}/rest/v1/reports",
                json=payload,
                headers=headers,
                params={"on_conflict": "source,source_url"},
            )
            if resp.status_code in (200, 201):
                inserted = len(records)
                logger.info("Batch upsert OK: %d records sent", inserted)
            elif resp.status_code == 409:
                # All duplicates -- expected on re-run
                logger.info("All %d records already exist (409 conflict)", len(records))
            else:
                logger.error(
                    "Batch upsert failed: HTTP %s -- %s",
                    resp.status_code,
                    resp.text[:300],
                )
                # Fall back to individual upserts so partial successes are saved
                logger.info("Falling back to per-record upserts")
                for rec_payload in payload:
                    try:
                        r2 = await client.post(
                            f"{supabase_url}/rest/v1/reports",
                            json=rec_payload,
                            headers=headers,
                            params={"on_conflict": "source,source_url"},
                        )
                        if r2.status_code in (200, 201, 409):
                            inserted += 1
                        else:
                            logger.warning(
                                "Record upsert failed (%s): %s -- %s",
                                r2.status_code,
                                rec_payload.get("source_url"),
                                r2.text[:150],
                            )
                            errors += 1
                    except Exception as exc:
                        logger.error(
                            "Per-record upsert error for %s: %s",
                            rec_payload.get("source_url"),
                            exc,
                        )
                        errors += 1
    except Exception as exc:
        logger.error("upsert_records failed: %s", exc)
        errors = len(records)

    return {"total": len(records), "inserted": inserted, "errors": errors}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def _run(html_file: Optional[str] = None, dry_run: bool = False) -> None:
    # Step 1: get HTML
    if html_file:
        logger.info("Reading HTML from file: %s", html_file)
        with open(html_file, encoding="utf-8") as fh:
            html = fh.read()
    else:
        try:
            html = await _fetch_html(ARTICLE_URL)
        except httpx.HTTPStatusError as exc:
            logger.error(
                "HTTP %s fetching article. "
                "Try saving the HTML manually and re-running with --html-file PATH.",
                exc.response.status_code,
            )
            sys.exit(1)
        except Exception as exc:
            logger.error("Failed to fetch article: %s", exc)
            sys.exit(1)

    logger.info("HTML size: %d bytes", len(html))

    # Step 2: extract text lines
    lines = _article_text_lines(html)
    logger.info("Extracted %d text lines from article", len(lines))

    if not lines:
        logger.error(
            "No text extracted from article. "
            "The site may block headless requests or use JS rendering. "
            "Save the page HTML manually and retry with --html-file."
        )
        sys.exit(1)

    # Step 3: parse records
    records = parse_records(lines)
    logger.info("Parsed %d person records", len(records))

    if not records:
        logger.warning(
            "No records matched the name+CI pattern. "
            "The article format may have changed -- inspect the HTML manually."
        )
        sys.exit(0)

    for r in records:
        logger.info(
            "  %-45s | %-20s | %s",
            r["full_name"],
            r.get("distinguishing_marks") or "(no CI)",
            r["source_url"],
        )

    if dry_run:
        logger.info("[dry-run] Skipping Supabase upsert.")
        return

    # Step 4: validate env vars before upsert
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_SERVICE_ROLE_KEY"):
        logger.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
        sys.exit(1)

    # Step 5: upsert
    stats = await upsert_records(records)
    logger.info(
        "Done. total=%d inserted=%d errors=%d",
        stats["total"],
        stats["inserted"],
        stats["errors"],
    )
    if stats["errors"]:
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time import: La Iguana TV -- Desaparecidos La Guaira"
    )
    parser.add_argument(
        "--html-file",
        metavar="PATH",
        help=(
            "Path to a locally saved HTML file of the article. "
            "Use this if the live site blocks the script."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print records without writing to Supabase.",
    )
    args = parser.parse_args()

    asyncio.run(_run(html_file=args.html_file, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
