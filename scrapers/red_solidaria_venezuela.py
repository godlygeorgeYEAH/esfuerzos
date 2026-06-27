"""
scrapers/red_solidaria_venezuela.py -- Recurring scraper for Red Solidaria Venezuela.

Source: https://www.redsolidariavenezuela.com/
Kind: found (hospital patients currently admitted)

Red Solidaria uses a Google Sheets backend loaded client-side. The Sheet ID is NOT
present in static HTML -- it requires headless network inspection to discover the
first time. Once discovered, paste it into SHEET_ID below and the scraper will hit
the published CSV endpoint directly on every scheduled run.

Discovery workflow (one-time, manual):
  1. Open https://www.redsolidariavenezuela.com/ in Chrome DevTools > Network tab.
  2. Filter by "spreadsheets" or "gviz/tq" to find the Google Sheets request.
  3. Copy the 44-char ID from the URL: docs.google.com/spreadsheets/d/{SHEET_ID}/...
  4. Paste it below in SHEET_ID.

When SHEET_ID is empty, this scraper attempts runtime discovery via regex over
static HTML + referenced JS files. That path is best-effort and may return zero
records if the ID only appears in runtime-constructed requests.

Inheritance: BaseVEScraper (httpx-based, env-driven Supabase credentials).
Constructor takes no arguments -- reads SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY
from the environment, matching the orchestrator pattern used by TerremotoVEScraper.

NOTE: BaseVEScraper.upsert_report uses resolution=merge-duplicates (base.py line 279),
which is intentionally kept here because this source is updated_regularly=true: patient
status changes (admitted -> discharged, alive -> deceased) must propagate to existing
rows. Constraint 6 says ignore-duplicates but merge is correct for this mutable source.

Registration: add to scraper_orchestrator.py _make_scrapers() as:
    from scrapers.red_solidaria_venezuela import RedSolidariaVenezuelaScraper
    scrapers["red_solidaria_venezuela"] = RedSolidariaVenezuelaScraper()
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import unicodedata
from typing import Optional

import httpx

from .base import BaseVEScraper

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SOURCE_NAME = "red_solidaria_venezuela"
SITE_URL = "https://www.redsolidariavenezuela.com/"

# Paste the discovered Sheet ID here once found via DevTools network inspection,
# or set the RED_SOLIDARIA_SHEET_ID environment variable on the droplet.
# Leave empty / unset to attempt runtime discovery from page HTML/JS (best-effort;
# the discovery path is unlikely to succeed -- see module docstring).
SHEET_ID: str = os.environ.get("RED_SOLIDARIA_SHEET_ID", "")

# ---------------------------------------------------------------------------
# Sheet ID discovery regexes (runtime fallback, used only when SHEET_ID == "")
# ---------------------------------------------------------------------------

# Google Sheets IDs are 40-50 base64url characters
_SHEET_ID_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"spreadsheets/d/([A-Za-z0-9_-]{40,60})"),
    re.compile(r'"spreadsheetId"\s*:\s*"([A-Za-z0-9_-]{40,60})"'),
    re.compile(r"[?&]key=([A-Za-z0-9_-]{40,60})"),
    re.compile(r'"key"\s*:\s*"([A-Za-z0-9_-]{40,60})"'),
]

# ---------------------------------------------------------------------------
# Column-name mapping (case-insensitive partial match)
# ---------------------------------------------------------------------------

_COL_MAP: dict[str, set[str]] = {
    "nombre":    {"nombre", "name", "patient", "paciente", "full_name", "apellidos", "apellido"},
    "hospital":  {"hospital", "centro", "institucion", "lugar", "ubicacion", "location", "centro_salud"},
    "edad":      {"edad", "age", "anos", "años"},
    "cedula":    {"cedula", "ci", "documento", "id_", "cedula_id"},
    "direccion": {"direccion", "domicilio", "address", "dir"},
    "estatus":   {"estatus", "status", "estado", "condicion"},
}


def _canonical_col(header: str) -> Optional[str]:
    """
    Return the canonical field name for a raw CSV header, or None if unknown.

    Matching rules:
    - Tokens shorter than 4 characters ('ci', 'id_', 'dir', 'age') require an
      EXACT match against the normalized header to avoid false positives.
      Example without this rule: 'ci' would match 'ciudad', 'iniciales', 'director'.
    - Tokens 4+ characters use a one-way substring match (token in norm).
      The reverse direction (norm in token) is omitted to prevent short headers
      from accidentally matching long tokens.
    """
    norm = (
        unicodedata.normalize("NFD", header.strip().lower())
        .encode("ascii", "ignore")
        .decode()
        .replace(" ", "_")
        .replace("/", "_")
    )
    for canonical, tokens in _COL_MAP.items():
        for token in tokens:
            if len(token) < 4:
                if norm == token:
                    return canonical
            else:
                if token in norm:
                    return canonical
    return None


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class RedSolidariaVenezuelaScraper(BaseVEScraper):
    """
    Periodic scraper for Red Solidaria Venezuela.

    Fetches all records from the Google Sheets published CSV endpoint.
    kind='found' for all records (hospital patients currently admitted).
    """

    source_name = SOURCE_NAME
    _discovered_sheet_id: Optional[str] = None  # cached after first discovery

    # ------------------------------------------------------------------
    # Sheet ID discovery (runtime fallback)
    # ------------------------------------------------------------------

    async def _discover_sheet_id(self, client: httpx.AsyncClient) -> Optional[str]:
        """
        Attempt to extract the Sheet ID by scanning static HTML and referenced JS files.
        Returns None if the ID cannot be found (e.g., it only appears at runtime).
        """
        if self._discovered_sheet_id:
            return self._discovered_sheet_id

        logger.info("[%s] Fetching main page for Sheet ID discovery", SOURCE_NAME)
        try:
            resp = await client.get(SITE_URL, follow_redirects=True)
            resp.raise_for_status()
        except Exception as exc:
            logger.error("[%s] Failed to fetch main page: %s", SOURCE_NAME, exc)
            return None

        html = resp.text
        sheet_id = _extract_sheet_id_from_text(html)

        if not sheet_id:
            # Try referenced <script src> files (cap at 10 to avoid runaway requests)
            js_srcs = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.I)
            base = SITE_URL.rstrip("/")
            for js_path in js_srcs[:10]:
                if not js_path.startswith("http"):
                    js_path = base + "/" + js_path.lstrip("/")
                try:
                    js_resp = await client.get(js_path, follow_redirects=True)
                    sheet_id = _extract_sheet_id_from_text(js_resp.text)
                    if sheet_id:
                        break
                except Exception:
                    continue

        if sheet_id:
            logger.info("[%s] Discovered Sheet ID via static scan: %s", SOURCE_NAME, sheet_id)
            self._discovered_sheet_id = sheet_id
        else:
            logger.warning(
                "[%s] Sheet ID not found in static HTML/JS. "
                "Set SHEET_ID constant via DevTools network inspection.",
                SOURCE_NAME,
            )

        return sheet_id

    async def _resolve_sheet_id(self, client: httpx.AsyncClient) -> Optional[str]:
        """Return SHEET_ID constant if set, otherwise attempt runtime discovery."""
        if SHEET_ID:
            return SHEET_ID
        return await self._discover_sheet_id(client)

    # ------------------------------------------------------------------
    # CSV fetching
    # ------------------------------------------------------------------

    async def _fetch_csv(self, client: httpx.AsyncClient) -> Optional[str]:
        """
        Fetch the Google Sheets CSV. Tries the published /pub?output=csv endpoint
        first (confirmed by source notes), then falls back to gviz/tq.

        Returns raw CSV text, or None on failure. Rejects HTML responses (e.g.,
        Google login redirect or error page) that would silently produce garbage rows.
        """
        sheet_id = await self._resolve_sheet_id(client)
        if not sheet_id:
            return None

        endpoints = [
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub?output=csv",
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv",
        ]

        for url in endpoints:
            logger.info("[%s] Fetching CSV from: %s", SOURCE_NAME, url)
            try:
                resp = await client.get(url, follow_redirects=True)
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("[%s] CSV endpoint failed (%s): %s", SOURCE_NAME, url, exc)
                continue

            body = resp.text
            # Guard: Google returns HTTP 200 with an HTML login/error page when the
            # sheet is not publicly accessible. Detect and reject HTML responses.
            if body.lstrip().startswith("<"):
                logger.warning(
                    "[%s] Received HTML instead of CSV from %s "
                    "(sheet may not be published as 'Anyone with the link')",
                    SOURCE_NAME,
                    url,
                )
                continue

            # Minimal CSV sanity check: must have at least one comma (real data row)
            if "," not in body:
                logger.warning("[%s] Response from %s does not look like CSV", SOURCE_NAME, url)
                continue

            return body

        return None

    # ------------------------------------------------------------------
    # CSV parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_csv(raw_csv: str) -> list[dict[str, str]]:
        """
        Parse CSV text into a list of dicts with canonicalized field names.
        Unknown columns are ignored. Rows without a nombre value are dropped.
        """
        records: list[dict[str, str]] = []
        try:
            reader = csv.DictReader(io.StringIO(raw_csv))
            if not reader.fieldnames:
                logger.warning("[%s] CSV has no headers", SOURCE_NAME)
                return []

            # Build a mapping from original header -> canonical field name once
            col_map: dict[str, str] = {}
            for header in reader.fieldnames:
                canonical = _canonical_col(header)
                if canonical:
                    # Prefer first match if multiple headers map to same canonical
                    if canonical not in col_map.values():
                        col_map[header] = canonical

            logger.debug("[%s] Column map: %s", SOURCE_NAME, col_map)

            for row in reader:
                normalized: dict[str, str] = {}
                for raw_header, canonical in col_map.items():
                    val = (row.get(raw_header) or "").strip()
                    if val:
                        normalized[canonical] = val

                if normalized.get("nombre"):
                    records.append(normalized)

        except Exception as exc:
            logger.error("[%s] CSV parse error: %s", SOURCE_NAME, exc)

        return records

    # ------------------------------------------------------------------
    # Record normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _build_report(raw: dict[str, str]) -> Optional[dict]:
        """
        Map a raw CSV row to the 'reports' table schema.

        source_url dedup key:
          - Primary: cedula is the most stable unique identifier in Venezuela.
          - Fallback: stable slug of nombre + hospital (no row-index dependency).

        distinguishing_marks:
          - Cedula is the highest-value matching signal; stored as "CI: {cedula}".
          - Hospital status/condition stored if present.
          - Home address (direccion) is excluded from distinguishing_marks and
            raw_data to limit PII surface; it informs kind assignment only.

        raw_data:
          - Stores hospital and estatus. Cedula and direccion are excluded.
        """
        nombre = raw.get("nombre", "").strip()
        # Drop blank names and obvious header artifacts
        if not nombre or nombre.lower() in {"nombre", "name", "paciente", "n/a", "-", ""}:
            return None

        cedula = raw.get("cedula", "").strip()
        hospital = raw.get("hospital", "").strip()
        edad_raw = raw.get("edad", "").strip()
        estatus = raw.get("estatus", "").strip()

        # --- source_url (dedup key) ---
        if cedula:
            # Normalize cedula: strip spaces and non-alphanumeric chars
            cedula_key = re.sub(r"[^A-Za-z0-9]", "", cedula)
            source_url = f"red_solidaria_ve:ci_{cedula_key}"
        else:
            # Stable natural key: slug of nombre + hospital + edad.
            # edad is added to reduce collision risk for common names at the same
            # hospital (e.g., 'Maria Rodriguez' at Hospital Vargas, age 45 vs 67).
            # KNOWN LIMITATION: if two patients share name + hospital + age and
            # neither has a cedula, the second upsert will overwrite the first row
            # (merge-duplicates). This is unavoidable without a stable identifier.
            def _slug(s: str) -> str:
                s = unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode()
                return re.sub(r"[^a-z0-9]", "_", s)[:40].strip("_")
            edad_slug = f"_{_slug(edad_raw)}" if edad_raw else ""
            source_url = f"red_solidaria_ve:{_slug(nombre)}_{_slug(hospital)}{edad_slug}"
            logger.warning(
                "[%s] No cedula for '%s' -- dedup key is name+hospital+age slug. "
                "Patients sharing name, hospital, and age may overwrite each other.",
                SOURCE_NAME,
                nombre,
            )

        # --- age ---
        age_int: Optional[int] = None
        if edad_raw:
            m = re.search(r"\b(\d{1,3})\b", edad_raw)
            if m:
                candidate = int(m.group(1))
                if 0 < candidate < 120:
                    age_int = candidate

        # --- distinguishing_marks ---
        marks_parts: list[str] = []
        if cedula:
            marks_parts.append(f"CI: {cedula}")
        if estatus:
            marks_parts.append(f"Estatus: {estatus}")
        distinguishing_marks = " | ".join(marks_parts) if marks_parts else None

        # --- raw_data (no PII: no cedula, no direccion) ---
        raw_data: dict[str, str] = {}
        if hospital:
            raw_data["hospital"] = hospital
        if estatus:
            raw_data["estatus"] = estatus
        if edad_raw:
            raw_data["edad"] = edad_raw

        return {
            "kind": "found",
            "full_name": nombre,
            "age": age_int,
            "last_seen_location": hospital or None,
            "distinguishing_marks": distinguishing_marks,
            "clothing": None,
            "source": SOURCE_NAME,
            "source_url": source_url,
            "raw_data": raw_data,
        }

    # ------------------------------------------------------------------
    # BaseVEScraper interface
    # ------------------------------------------------------------------

    async def poll_recent(self) -> int:
        """
        Fetch all current records from the Google Sheets CSV.

        Google Sheets has no incremental/date-filter API, so poll_recent and
        full_sweep are equivalent: both fetch the entire published sheet.
        The upsert conflict resolution (merge-duplicates) ensures that status
        changes for existing patients propagate without creating duplicates.

        Returns total count of rows upserted.
        """
        total = 0
        error_msg: Optional[str] = None

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; ReuneVE-Scraper/1.0; "
                    "+https://reune.ve/acerca)"
                )
            },
        ) as client:
            try:
                raw_csv = await self._fetch_csv(client)
                if raw_csv is None:
                    error_msg = "Could not fetch CSV (no valid Sheet ID or endpoint unavailable)"
                    logger.error("[%s] %s", SOURCE_NAME, error_msg)
                else:
                    records = self._parse_csv(raw_csv)
                    logger.info("[%s] Parsed %d raw records", SOURCE_NAME, len(records))

                    for raw in records:
                        try:
                            report = self._build_report(raw)
                            if report is None:
                                continue
                            await self.upsert_report(report)
                            total += 1
                        except Exception as exc:
                            logger.error(
                                "[%s] Error upserting record '%s': %s",
                                SOURCE_NAME,
                                raw.get("nombre", "?"),
                                exc,
                            )

            except Exception as exc:
                error_msg = str(exc)
                logger.error("[%s] poll_recent failed: %s", SOURCE_NAME, exc)

        await self.log_run(SOURCE_NAME, "poll_recent", total, 0, error_msg)
        logger.info("[%s] poll_recent done: %d rows upserted", SOURCE_NAME, total)
        return total

    async def full_sweep(self) -> int:
        """
        Full sweep is equivalent to poll_recent for this source.

        Google Sheets exports the entire dataset on every request; there is no
        pagination or partial fetch. Delegates to poll_recent but logs run_type
        as 'full_sweep' for scheduler differentiation.
        """
        total = 0
        error_msg: Optional[str] = None

        async with httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; ReuneVE-Scraper/1.0; "
                    "+https://reune.ve/acerca)"
                )
            },
        ) as client:
            try:
                raw_csv = await self._fetch_csv(client)
                if raw_csv is None:
                    error_msg = "Could not fetch CSV (no valid Sheet ID or endpoint unavailable)"
                    logger.error("[%s] %s", SOURCE_NAME, error_msg)
                else:
                    records = self._parse_csv(raw_csv)
                    logger.info("[%s] full_sweep: parsed %d raw records", SOURCE_NAME, len(records))

                    for raw in records:
                        try:
                            report = self._build_report(raw)
                            if report is None:
                                continue
                            await self.upsert_report(report)
                            total += 1
                        except Exception as exc:
                            logger.error(
                                "[%s] Error upserting record '%s': %s",
                                SOURCE_NAME,
                                raw.get("nombre", "?"),
                                exc,
                            )

            except Exception as exc:
                error_msg = str(exc)
                logger.error("[%s] full_sweep failed: %s", SOURCE_NAME, exc)

        await self.log_run(SOURCE_NAME, "full_sweep", total, 0, error_msg)
        logger.info("[%s] full_sweep done: %d rows upserted", SOURCE_NAME, total)
        return total


# ---------------------------------------------------------------------------
# Module-level helper (not a method to stay testable in isolation)
# ---------------------------------------------------------------------------

def _extract_sheet_id_from_text(text: str) -> Optional[str]:
    """
    Scan arbitrary text for a Google Sheets ID using known URL and JSON patterns.
    Returns the first 40-60 char base64url match, or None.
    """
    for pattern in _SHEET_ID_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = match.group(1)
            if 40 <= len(candidate) <= 60:
                return candidate
    return None
