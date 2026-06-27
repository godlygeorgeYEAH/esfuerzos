"""
google_drive_hospital.py - Scraper for the SISMO 2026 hospital admissions registry.

Source: Google Drive public folder (INGRESOS HOSPITALARIOS SISMO 2026).
Document: "Registro Maestro de Pacientes" - Google Doc with table N°|HOSPITAL|NOMBRES|EDAD.

These records are `kind=found`: people confirmed at a hospital post-earthquake.
They match against `kind=missing` reports via text embedding similarity.

Export URL (no auth needed for public docs):
  https://docs.google.com/document/d/{doc_id}/export?format=txt
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Any

import httpx

logger = logging.getLogger(__name__)

FOLDER_ID = "1o36ifaRz45kAs5rKzci49aD0mP5JB_YI"

# Known document IDs in the folder - add more as they appear
DOCUMENT_IDS = [
    "125LObYNRazMhUuxeF8FFthiA5YJaGFyApKiUHyO4olo",  # Listado 2
]

SOURCE = "google_drive_hospital"
SUPABASE_BATCH = 25


def _export_url(doc_id: str) -> str:
    # HTML export is more reliable for table parsing (txt format varies)
    return f"https://docs.google.com/document/d/{doc_id}/export?format=html"


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return " ".join(text.split()).strip()


def _parse_patients_html(html: str) -> list[dict]:
    """Parse HTML table rows from Google Docs HTML export."""
    patients = []
    # Extract all <tr> blocks
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        # Extract cell text, strip all HTML tags
        cells_raw = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL | re.IGNORECASE)
        cells = [re.sub(r"<[^>]+>", "", c).strip() for c in cells_raw]
        cells = [" ".join(c.split()) for c in cells]  # collapse whitespace
        if len(cells) < 3:
            continue
        joined = " ".join(cells).upper()
        if "HOSPITAL" in joined and ("NOMBRE" in joined or "APELLIDO" in joined):
            continue

        if len(cells) >= 4:
            hospital_raw, name_raw, age_raw = cells[1], cells[2], cells[3]
        else:
            hospital_raw, name_raw, age_raw = cells[0], cells[1], cells[2]

        hospital_clean = re.sub(r"^\d+\s+", "", hospital_raw).strip()
        name_clean = name_raw.strip()
        age_clean = re.sub(r"\D", "", age_raw).strip()

        if not name_clean or not age_clean:
            continue
        if not re.match(r"^[A-ZÁÉÍÓÚÜÑ\s/\(\)\-\.]{3,}", name_clean, re.IGNORECASE):
            continue
        try:
            age = int(age_clean)
            if not (1 <= age <= 120):
                continue
        except ValueError:
            continue

        name_clean = _normalize(name_clean)
        hospital_clean = _normalize(hospital_clean) or "Hospital"
        if len(name_clean) < 3:
            continue

        patients.append({
            "kind": "found",
            "full_name": name_clean,
            "age": age,
            "last_seen_location": hospital_clean,
            "source": SOURCE,
        })
    return patients


def _parse_patients(raw_text: str) -> list[dict]:
    """
    Parse patient table from Google Doc text export.

    The document uses Markdown-style pipe tables:
      |   | 2 Hospital Universitario de Caracas  | CUYAN FRAN  | 60 |
      | 1  | Hospital Universitario de Caracas  | OROZCO YUSBELIS  | 35 |

    Cells: [maybe_num] | [num+hospital or hospital] | [NAME] | [AGE]
    """
    patients = []
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or not line.startswith("|"):
            continue

        # Split on pipes, strip whitespace from each cell
        cells = [c.strip() for c in line.split("|")]
        # Remove empty first/last elements from leading/trailing |
        cells = [c for c in cells if c]

        if len(cells) < 3:
            continue

        # Skip header row
        joined = " ".join(cells).upper()
        if "HOSPITAL" in joined and "NOMBRE" in joined:
            continue
        if ":-:" in line or "---" in line:
            continue

        # Extract hospital, name, age from cells
        # Common formats:
        #   4 cols: [num] [hospital] [name] [age]
        #   3 cols: [num+hospital] [name] [age]  (num embedded in hospital cell)
        if len(cells) >= 4:
            hospital_raw = cells[1]
            name_raw = cells[2]
            age_raw = cells[3]
        else:
            hospital_raw = cells[0]
            name_raw = cells[1]
            age_raw = cells[2]

        # Strip leading number from hospital cell (e.g. "2 Hospital X" → "Hospital X")
        hospital_clean = re.sub(r"^\d+\s+", "", hospital_raw).strip()
        # Strip markdown bold (**text**)
        hospital_clean = re.sub(r"\*+", "", hospital_clean).strip()
        name_clean = re.sub(r"\*+", "", name_raw).strip()
        age_clean = re.sub(r"\D", "", age_raw).strip()

        # Validate
        if not name_clean or not age_clean:
            continue
        if not re.match(r"^[A-ZÁÉÍÓÚÜÑ\s/\(\)\-\.]{3,}", name_clean, re.IGNORECASE):
            continue
        try:
            age = int(age_clean)
            if not (1 <= age <= 120):
                continue
        except ValueError:
            continue

        name_clean = _normalize(name_clean)
        hospital_clean = _normalize(hospital_clean) or "Hospital"
        if len(name_clean) < 3:
            continue

        patients.append({
            "kind": "found",
            "full_name": name_clean,
            "age": age,
            "last_seen_location": hospital_clean,
            "source": SOURCE,
        })

    return patients


class GoogleDriveHospitalScraper:
    """Polls the public Google Drive folder for hospital patient lists."""

    def __init__(self, supabase_url: str, supabase_key: str):
        self.sb_url = supabase_url.rstrip("/")
        self.sb_key = supabase_key
        self._client: httpx.AsyncClient | None = None

    def _sb_headers(self) -> dict:
        return {
            "apikey": self.sb_key,
            "Authorization": f"Bearer {self.sb_key}",
            "Content-Type": "application/json",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30, follow_redirects=True)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def poll_recent(self) -> int:
        """Fetch all known documents and upsert new patients. Returns inserted count."""
        total = 0
        for doc_id in DOCUMENT_IDS:
            try:
                total += await self._import_document(doc_id)
            except Exception as exc:
                logger.error("google_drive_hospital: doc %s error: %s", doc_id, exc)
        return total

    async def full_sweep(self) -> int:
        return await self.poll_recent()

    async def _import_document(self, doc_id: str) -> int:
        cl = await self._get_client()
        url = _export_url(doc_id)
        r = await cl.get(url)
        if r.status_code != 200:
            logger.warning("google_drive_hospital: export %d for doc %s", r.status_code, doc_id)
            return 0

        raw = r.text
        # Use HTML parser for HTML export, fallback to text parser
        if "<table" in raw.lower() or "<tr" in raw.lower():
            patients = _parse_patients_html(raw)
        else:
            patients = _parse_patients(raw)
        if not patients:
            logger.info("google_drive_hospital: no patients parsed from doc %s", doc_id)
            return 0

        logger.info("google_drive_hospital: parsed %d patients from doc %s", len(patients), doc_id)

        inserted = 0
        for i in range(0, len(patients), SUPABASE_BATCH):
            batch = patients[i: i + SUPABASE_BATCH]
            for p in batch:
                p["source_url"] = f"gdrive:{doc_id}:{p['full_name'][:40]}"
            resp = await cl.post(
                f"{self.sb_url}/rest/v1/reports",
                headers={
                    **self._sb_headers(),
                    "Prefer": "resolution=ignore-duplicates,return=minimal",
                },
                params={"on_conflict": "source,source_url"},
                json=batch,
            )
            if resp.status_code in (200, 201):
                inserted += len(batch)
            else:
                logger.warning(
                    "google_drive_hospital: upsert %d: %s",
                    resp.status_code, resp.text[:200],
                )

        logger.info("google_drive_hospital: inserted %d / %d from doc %s", inserted, len(patients), doc_id)
        return inserted
