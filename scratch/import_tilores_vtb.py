"""
import_tilores_vtb.py - One-time importer for the Tilores Venezuela Te Busca snapshot.

Tilores ran an entity-resolution pass over venezuelatebusca.com and produced a
deduplicated export (28,040 raw reports -> 26,962 unique individuals, 947 multi-report
groups merged, 75 conflicting missing/found records reconciled).  The export is a
static file (CSV or JSONL) obtained directly from Tilores under their pro-bono
humanitarian program -- there is no public API endpoint.

HOW TO USE
----------
1. Obtain the export file from Tilores (https://tilores.io/venezuela-te-busca).
   They provide CSV and JSONL; both formats are supported here.

2. Run from the repo root (or inside the container):

       python scratch/import_tilores_vtb.py --file /path/to/tilores_export.jsonl

   Or, if Tilores gave you a signed download URL:

       python scratch/import_tilores_vtb.py --url "https://signed-url..." --file tilores_export.jsonl

3. Environment variables required (already set in .env / container env):

       SUPABASE_URL
       SUPABASE_SERVICE_ROLE_KEY

ASSUMED FIELD NAMES
-------------------
Tilores field names are not officially documented here; the mapping below is
best-effort based on the Tilores schema conventions and typical VTB data.
Adjust the FIELD_MAP constant if the actual file uses different column names.
The defensive mapper tries multiple keys per logical field and falls through.

CONSTRAINTS HONORED
-------------------
- resolution=ignore-duplicates (baseline snapshot; re-running is safe, won't overwrite)
- kind=found for hospital/morgue/shelter AND for deceased persons
- Deceased status goes into distinguishing_marks; no "deceased" boolean field
- source_url dedup key: tilores_vtb:cedula_{hash} > tilores_vtb:{entity_id} > tilores_vtb:name_{hash}
- PII stripped before storing raw_data (via scrapers.base.strip_pii)
- Errors caught per-record; import continues on failure
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("import_tilores_vtb")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SOURCE = "tilores_vtb"
BATCH_SIZE = 500

# FIELD_MAP: maps logical field -> ordered list of candidate keys to try.
# Operator: adjust these lists if your Tilores file uses different column names.
FIELD_MAP: dict[str, list[str]] = {
    "entity_id":    ["id", "entity_id", "tilores_id", "record_id"],
    "full_name":    ["name", "full_name", "nombre", "nombre_completo", "person_name"],
    "cedula":       ["cedula", "document_number", "id_number", "doc", "cedula_identidad", "documento"],
    "status":       ["status", "estado", "state", "condition", "situacion"],
    "location":     ["last_seen_location", "location", "ubicacion", "last_seen", "lugar",
                     "lugar_visto", "lugar_desaparicion", "municipio", "estado_ubicacion"],
    "age":          ["age", "edad", "age_years"],
    "description":  ["description", "notes", "distinguishing_marks", "senas_particulares",
                     "caracteristicas", "señas_particulares", "notas", "descripcion"],
    "aliases":      ["aliases", "alias", "other_names", "nombres_alternos"],
    # contacts extracted for PII stripping only; NOT written to user-facing fields.
    "contacts":     ["contacts", "contactos", "phones", "telefonos"],
}

# Keywords that indicate a found/located person (case-insensitive).
_FOUND_TERMS = frozenset({
    "encontrado", "encontrada", "found", "localizado", "localizada",
    "located", "safe", "a_salvo", "refugio", "albergue", "shelter",
    "hospital", "clinica", "clinica_privada", "morgue", "anfiteatro",
})

# Keywords that indicate deceased (map to kind=found with note in marks).
_DECEASED_TERMS = frozenset({
    "fallecido", "fallecida", "deceased", "muerto", "muerta",
    "difunto", "difunta", "obito", "sin_vida", "cadaver",
})

# ---------------------------------------------------------------------------
# Supabase helpers
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
    POST a batch of rows to /rest/v1/reports.
    Returns (inserted, errors).
    Uses ignore-duplicates so re-running is safe (baseline snapshot).
    """
    resp = await client.post(
        f"{sb_url}/rest/v1/reports",
        headers=_sb_headers(sb_key, "resolution=ignore-duplicates,return=minimal"),
        params={"on_conflict": "source,source_url"},
        json=rows,
        timeout=60,
    )
    if resp.status_code in (200, 201):
        # resolution=ignore-duplicates + return=minimal => empty body.
        # We count rows POSTed, not rows actually new; on a fresh baseline
        # (first run) these are equivalent.  Rename 'inserted' -> 'sent' in
        # the final stats to avoid misleading re-run counts.
        return len(rows), 0
    # Non-fatal: log the error and count as errors.
    logger.warning("upsert_batch HTTP %d: %s", resp.status_code, resp.text[:200])
    return 0, len(rows)


async def _log_run(
    sb_url: str,
    sb_key: str,
    rows_inserted: int,
    rows_errors: int,
    error: str | None,
) -> None:
    # NOTE: rows_errors is NOT included in the row dict.
    # The scraper_runs table schema (see api/scrapers/base.py) defines only
    # rows_inserted, rows_updated, and error. PostgREST rejects inserts with
    # unknown columns, so adding rows_errors here would break every run.
    # The integer error count is encoded in the human-readable 'error' string
    # as a fallback. A future schema migration (add rows_errors INT) would
    # allow storing it as a queryable integer.
    row = {
        "source": SOURCE,
        "run_type": "one_time_import",
        "rows_inserted": rows_inserted,
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
# Field extraction helpers
# ---------------------------------------------------------------------------

def _pick(record: dict, candidates: list[str]) -> Any:
    """Return the first non-empty value from the list of candidate keys."""
    for key in candidates:
        v = record.get(key)
        if v is not None and v != "":
            return v
    return None


def _coerce_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, list):
        # contacts / aliases may be arrays; join them
        return "; ".join(str(x) for x in v if x)
    return str(v).strip() or None


def _infer_kind(status_raw: str | None) -> tuple[str, bool]:
    """
    Returns (kind, is_deceased).
    kind = 'found' | 'missing'
    is_deceased = True when the person has died (status goes into distinguishing_marks).

    Uses a negative lookbehind to avoid false positives on negated phrases:
    e.g. "no encontrado" -> normalized "no_encontrado" -> kind=missing (correct),
    because "no_" immediately precedes "encontrado".

    Residual limitation: Spanish negation scoping over compound phrases like
    "no encontrado ni localizado" will misclassify on the second term ("localizado"
    is not preceded by "no_"). Tilores status enums are expected to be single
    values (clean reconciled data), so this edge case is unlikely in practice.
    """
    if not status_raw:
        return "missing", False
    normalized = status_raw.lower().replace(" ", "_").replace("-", "_")
    for term in _DECEASED_TERMS:
        # Negative lookbehind: reject match if immediately preceded by "no_".
        if re.search(rf'(?<!no_){re.escape(term)}', normalized):
            return "found", True
    for term in _FOUND_TERMS:
        if re.search(rf'(?<!no_){re.escape(term)}', normalized):
            return "found", False
    return "missing", False


def _source_url(entity_id: str | None, cedula: str | None, full_name: str) -> str:
    """
    Build the dedup key for source_url.
    Priority: cedula > entity_id > sha1 of normalized name.

    Cedula is hashed (not stored raw) to avoid PII in the source_url column,
    consistent with the PII stripping applied to raw_data.
    """
    if cedula:
        # Hash cedula so the dedup key remains stable across runs without
        # storing the raw document number in the database.
        cedula_hash = hashlib.sha1(cedula.encode()).hexdigest()[:12]
        return f"{SOURCE}:cedula_{cedula_hash}"
    if entity_id:
        return f"{SOURCE}:{entity_id}"
    # Last resort: deterministic hash of name so re-runs don't create duplicates.
    name_hash = hashlib.sha1(full_name.lower().encode()).hexdigest()[:12]
    return f"{SOURCE}:name_{name_hash}"


def _parse_age(raw: Any) -> int | None:
    """
    Parse age from a numeric or text value.
    Falls back to scrapers.base.parse_age_int for text.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        age = int(raw)
        return age if 0 < age < 120 else None
    try:
        from scrapers.base import parse_age_int
        return parse_age_int(str(raw))
    except ImportError:
        m = re.search(r"\b(\d{1,3})\b", str(raw))
        if m:
            age = int(m.group(1))
            return age if 0 < age < 120 else None
    return None


_PII_KEYS_EXTENDED = frozenset({
    # Standard keys (scrapers.base.strip_pii)
    "cedula", "cedula_masked", "contacto", "telefono", "phone",
    "email", "direccion", "direccion_exacta", "numero_contacto",
    # Tilores field-name variants (base.strip_pii does not cover these)
    "cedula_identidad", "document_number", "id_number", "documento",
    "contacts", "contactos", "phones", "telefonos",
})


def _strip_pii(raw: dict) -> dict:
    """Strip PII from raw_data before storage.

    Uses a locally-defined extended set instead of scrapers.base.strip_pii, which
    has a narrower keyset that would miss Tilores-specific field names like
    document_number, id_number, contactos, etc.
    """
    return {k: v for k, v in raw.items() if k.lower() not in _PII_KEYS_EXTENDED}

# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize(record: dict) -> dict | None:
    """
    Map one Tilores entity record to the reports table schema.
    Returns None to skip the record (e.g., missing name).
    """
    entity_id  = _coerce_str(_pick(record, FIELD_MAP["entity_id"]))
    full_name  = _coerce_str(_pick(record, FIELD_MAP["full_name"]))
    cedula     = _coerce_str(_pick(record, FIELD_MAP["cedula"]))
    status_raw = _coerce_str(_pick(record, FIELD_MAP["status"]))
    location   = _coerce_str(_pick(record, FIELD_MAP["location"]))
    age_raw    = _pick(record, FIELD_MAP["age"])
    desc_raw   = _coerce_str(_pick(record, FIELD_MAP["description"]))
    aliases    = _coerce_str(_pick(record, FIELD_MAP["aliases"]))
    # contacts is extracted only to prevent accidental inclusion elsewhere;
    # it is PII and must NOT appear in distinguishing_marks or any user-facing field.

    if not full_name:
        return None

    kind, is_deceased = _infer_kind(status_raw)

    # Build distinguishing_marks.
    # Deceased status must be recorded here (constraint #3).
    # Contacts are intentionally excluded: they are PII (same keys stripped
    # from raw_data by _strip_pii) and must not appear in user-facing fields.
    marks_parts: list[str] = []
    if is_deceased and status_raw:
        marks_parts.append(f"Estado: {status_raw}")
    if desc_raw:
        marks_parts.append(desc_raw)
    if aliases:
        marks_parts.append(f"Otros nombres: {aliases}")
    distinguishing_marks = "; ".join(marks_parts)[:1000] or None

    return {
        "kind": kind,
        "full_name": full_name[:200],
        "age": _parse_age(age_raw),
        "last_seen_location": location[:300] if location else None,
        "distinguishing_marks": distinguishing_marks,
        "source": SOURCE,
        "source_url": _source_url(entity_id, cedula, full_name),
        "raw_data": _strip_pii(record),
    }

# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.warning("JSONL line %d skipped: %s", lineno, exc)
    return records


def _read_csv(path: Path, delimiter: str = ",") -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        for row in reader:
            # csv.DictReader returns OrderedDict with all values as str;
            # convert empty strings to None for consistent handling.
            records.append({k: (v if v != "" else None) for k, v in row.items()})
    return records


def _read_json(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return data
    # Some exporters wrap in a top-level key.
    for key in ("records", "data", "persons", "personas", "results", "entities"):
        if key in data:
            return data[key]
    # Fallback: wrap the single dict.
    return [data]


def load_file(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl" or suffix == ".ndjson":
        return _read_jsonl(path)
    if suffix == ".json":
        return _read_json(path)
    if suffix == ".csv":
        return _read_csv(path, delimiter=",")
    if suffix == ".tsv":
        return _read_csv(path, delimiter="\t")
    # Unknown extension: try JSONL first, then CSV.
    logger.warning("Unknown extension '%s'; trying JSONL then CSV.", suffix)
    try:
        return _read_jsonl(path)
    except Exception:
        return _read_csv(path)

# ---------------------------------------------------------------------------
# Optional download from a signed URL
# ---------------------------------------------------------------------------

async def download_file(url: str, dest: Path) -> None:
    """
    Download the export file from a signed URL provided by Tilores.
    Only needed when --url is supplied; local --file is preferred.
    """
    logger.info("Downloading Tilores export from signed URL -> %s", dest)
    async with httpx.AsyncClient(follow_redirects=True, timeout=300) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with dest.open("wb") as fh:
                async for chunk in resp.aiter_bytes(chunk_size=65_536):
                    fh.write(chunk)
    logger.info("Download complete: %s (%.1f KB)", dest, dest.stat().st_size / 1024)

# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

async def run_import(file_path: Path, sb_url: str, sb_key: str, dry_run: bool = False) -> dict:
    """
    Load the Tilores export file, normalize all records, batch-upsert to Supabase.
    Returns a stats dict.
    """
    logger.info("Loading %s ...", file_path)
    raw_records = load_file(file_path)
    logger.info("Loaded %d raw records from file.", len(raw_records))

    # Normalize + dedup within the file (Tilores should already be deduped, but be safe).
    seen_source_urls: set[str] = set()
    normalized: list[dict] = []
    skipped_no_name = 0
    skipped_in_file_dupe = 0

    for record in raw_records:
        try:
            row = normalize(record)
        except Exception as exc:
            logger.warning("normalize error (skipping): %s | record keys: %s",
                           exc, list(record.keys())[:10])
            continue

        if row is None:
            skipped_no_name += 1
            continue

        if row["source_url"] in seen_source_urls:
            skipped_in_file_dupe += 1
            continue

        seen_source_urls.add(row["source_url"])
        normalized.append(row)

    logger.info(
        "Normalization: %d valid | %d skipped (no name) | %d in-file dupes",
        len(normalized), skipped_no_name, skipped_in_file_dupe,
    )

    if dry_run:
        logger.info("[dry-run] Would upsert %d records. First 3:", len(normalized))
        for r in normalized[:3]:
            logger.info("  %s", json.dumps(r, ensure_ascii=False, default=str))
        return {
            "dry_run": True,
            "normalized": len(normalized),
            "skipped_no_name": skipped_no_name,
            "skipped_in_file_dupe": skipped_in_file_dupe,
        }

    total_inserted = 0
    total_errors = 0

    async with httpx.AsyncClient(timeout=60) as client:
        for batch_start in range(0, len(normalized), BATCH_SIZE):
            batch = normalized[batch_start : batch_start + BATCH_SIZE]
            try:
                ins, err = await _upsert_batch(client, sb_url, sb_key, batch)
                total_inserted += ins
                total_errors += err
            except Exception as exc:
                logger.error("upsert_batch offset=%d: %s", batch_start, exc)
                total_errors += len(batch)

            progress = min(batch_start + BATCH_SIZE, len(normalized))
            logger.info(
                "Progress: %d/%d | inserted=%d errors=%d",
                progress, len(normalized), total_inserted, total_errors,
            )
            await asyncio.sleep(0.05)  # yield; avoid saturating Supabase

    # Log the run regardless of errors.
    error_summary = f"{total_errors} records failed" if total_errors else None
    await _log_run(sb_url, sb_key, rows_inserted=total_inserted, rows_errors=total_errors, error=error_summary)

    stats = {
        "source": SOURCE,
        "file": str(file_path),
        "raw_records": len(raw_records),
        "normalized": len(normalized),
        # 'sent' = rows POSTed; equals new rows on first run; on re-runs,
        # existing rows are silently skipped by ignore-duplicates (not counted separately).
        "sent": total_inserted,
        "errors": total_errors,
        "skipped_no_name": skipped_no_name,
        "skipped_in_file_dupe": skipped_in_file_dupe,
    }
    logger.info("Import complete: %s", stats)
    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time importer for the Tilores Venezuela Te Busca deduplicated export."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the Tilores export file (CSV, JSONL, or JSON).",
    )
    parser.add_argument(
        "--url",
        default=None,
        help=(
            "Optional signed download URL from Tilores. "
            "If provided, the file is downloaded to --file before importing."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and normalize records but do NOT write to Supabase.",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    file_path = Path(args.file)

    # Validate env.
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

    if not args.dry_run:
        if not sb_url:
            logger.error("SUPABASE_URL is not set. Aborting.")
            sys.exit(1)
        if not sb_key:
            logger.error("SUPABASE_SERVICE_ROLE_KEY is not set. Aborting.")
            sys.exit(1)

    # Optional download step.
    if args.url:
        await download_file(args.url, file_path)

    if not file_path.exists():
        logger.error("File not found: %s", file_path)
        sys.exit(1)

    stats = await run_import(file_path, sb_url, sb_key, dry_run=args.dry_run)
    print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    asyncio.run(main())
