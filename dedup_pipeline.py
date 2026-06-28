"""
dedup_pipeline.py - Background deduplication for the Reune VE 'reports' table.

The same missing/found person is reported across many scrapers with slightly
different name spellings ("Vergara Blanco Arantza", "Vergara Blanca Aranza",
"Vergara Arantza") and the same location (e.g. a hospital). This pipeline
clusters those near-duplicates and marks the non-canonical rows in raw_data
so fuzzy search and human review can collapse them.

Design:
  - NEVER deletes rows. Only annotates raw_data.possible_duplicate_of.
  - Buckets by (first-name token + normalized location prefix) so WRatio runs
    only within small candidate groups, not across the whole table.
  - Within a bucket, rows with WRatio(name_a, name_b) >= _FUZZ_THRESHOLD are
    one cluster. The most complete row (age + location + longest marks) is
    canonical; ties go to the older row (stable, earliest created_at).
  - Idempotent: rows already pointing at the same canonical are skipped.

Scheduling: registered on the existing APScheduler in main.py with a 4h
IntervalTrigger and max_instances=1, so no hand-rolled backoff loop is needed.

Entry point: async def run_dedup_pipeline(app) -> dict
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:  # pragma: no cover - dep is pinned, guard is defensive
    _HAS_RAPIDFUZZ = False
    logger.warning("rapidfuzz not installed — dedup pipeline disabled")

# Tuning
_PAGE_SIZE = 1000            # PostgREST default max rows per request
_MAX_PAGES = 70              # cover up to 70k rows per run (whole table)
_FUZZ_THRESHOLD = 85.0       # WRatio >= this == same person
_LOC_PREFIX = 30             # chars of normalized location used for bucketing
_PATCH_CONCURRENCY = 8       # max simultaneous PATCH calls


def _norm_loc(loc: str | None) -> str:
    return "".join((loc or "").lower().split())[:_LOC_PREFIX]


def _completeness(rec: dict) -> int:
    """Higher = more complete. Drives canonical selection."""
    score = 0
    if rec.get("age") is not None:
        score += 10
    if rec.get("last_seen_location"):
        score += 5
    marks = rec.get("distinguishing_marks") or ""
    score += min(len(marks), 200)  # cap so a wall of text can't dominate
    return score


def _sb_headers(key: str, prefer: str = "") -> dict:
    h = {"apikey": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if prefer:
        h["Prefer"] = prefer
    return h


async def _fetch_reports(client: httpx.AsyncClient, sb: str, key: str) -> list[dict]:
    """Paginate the whole reports table (PostgREST caps each request at ~1000)."""
    rows: list[dict] = []
    for page in range(_MAX_PAGES):
        offset = page * _PAGE_SIZE
        r = await client.get(
            f"{sb}/rest/v1/reports",
            headers=_sb_headers(key),
            params={
                "select": "id,full_name,age,last_seen_location,kind,source_url,raw_data,created_at",
                "full_name": "not.is.null",
                "limit": str(_PAGE_SIZE),
                "offset": str(offset),
                "order": "created_at.desc",
            },
        )
        r.raise_for_status()
        batch = r.json() or []
        rows.extend(batch)
        if len(batch) < _PAGE_SIZE:
            break
    return rows


def _cluster(rows: list[dict]) -> list[tuple[dict, list[dict]]]:
    """Return (canonical, [duplicates]) tuples for each multi-row cluster."""
    # Bucket by first-name token + location prefix + kind
    buckets: dict[str, list[dict]] = {}
    for rec in rows:
        tokens = (rec.get("full_name") or "").lower().split()
        if not tokens:
            continue
        bkey = f"{rec.get('kind','')}|{tokens[0]}|{_norm_loc(rec.get('last_seen_location'))}"
        buckets.setdefault(bkey, []).append(rec)

    clusters: list[tuple[dict, list[dict]]] = []
    for group in buckets.values():
        if len(group) < 2:
            continue
        used: set[str] = set()
        for i, anchor in enumerate(group):
            if anchor["id"] in used:
                continue
            members = [anchor]
            used.add(anchor["id"])
            a_name = anchor.get("full_name") or ""
            for other in group[i + 1:]:
                if other["id"] in used:
                    continue
                b_name = other.get("full_name") or ""
                if _fuzz.WRatio(a_name, b_name) >= _FUZZ_THRESHOLD:
                    members.append(other)
                    used.add(other["id"])
            if len(members) < 2:
                continue
            # Canonical: most complete; tie -> older (earliest created_at)
            canonical = max(
                members,
                key=lambda m: (_completeness(m), _neg_created(m)),
            )
            dups = [m for m in members if m["id"] != canonical["id"]]
            clusters.append((canonical, dups))
    return clusters


def _neg_created(rec: dict) -> str:
    """Sort helper: older created_at wins ties. We want max() to pick the
    earliest, so invert by returning a value that is larger for older dates.
    created_at is ISO-8601; lexicographic compare works. Negate by mapping to
    a reverse-sortable surrogate."""
    # For max() tie-break: prefer the smallest created_at. Return a string whose
    # natural order is reversed so the earliest date yields the largest key.
    ts = rec.get("created_at") or "9999"
    # Invert each char so earlier (smaller) timestamps sort larger.
    return "".join(chr(255 - ord(c)) for c in ts)


async def _mark_duplicate(
    client: httpx.AsyncClient, sb: str, key: str,
    dup: dict, canonical: dict, stamp: str,
) -> bool:
    raw = dup.get("raw_data")
    if not isinstance(raw, dict):
        raw = {}
    canon_ref = canonical.get("source_url") or canonical.get("id")
    if raw.get("possible_duplicate_of") == canon_ref:
        return False  # already marked at this canonical — idempotent skip
    merged = {**raw, "possible_duplicate_of": canon_ref, "dedup_run_at": stamp}
    r = await client.patch(
        f"{sb}/rest/v1/reports",
        headers=_sb_headers(key, "return=minimal"),
        params={"id": f"eq.{dup['id']}"},
        json={"raw_data": merged},
    )
    if r.status_code in (200, 204):
        return True
    logger.warning("dedup mark failed %d: %s", r.status_code, r.text[:120])
    return False


async def run_dedup_pipeline(app: Any) -> dict:
    """Scan recent reports, cluster near-duplicates, mark non-canonical rows."""
    if not _HAS_RAPIDFUZZ:
        return {"checked": 0, "duplicates_marked": 0, "errors": 0, "skipped": "no rapidfuzz"}

    sb = app.state.supabase_url.rstrip("/")
    key = app.state.supabase_service_key
    stamp = datetime.now(timezone.utc).isoformat()
    marked = 0
    errors = 0
    rows: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            rows = await _fetch_reports(client, sb, key)
            clusters = _cluster(rows)
            logger.info("dedup: %d rows scanned, %d duplicate clusters", len(rows), len(clusters))

            sem = asyncio.Semaphore(_PATCH_CONCURRENCY)

            async def _do(dup: dict, canon: dict):
                nonlocal marked, errors
                async with sem:
                    try:
                        if await _mark_duplicate(client, sb, key, dup, canon, stamp):
                            marked += 1
                    except Exception as exc:
                        errors += 1
                        logger.warning("dedup mark exception: %s", exc)

            tasks = [_do(dup, canon) for canon, dups in clusters for dup in dups]
            if tasks:
                await asyncio.gather(*tasks)
    except Exception as exc:
        errors += 1
        logger.error("run_dedup_pipeline error: %s", exc)

    result = {"checked": len(rows), "duplicates_marked": marked, "errors": errors}
    logger.info("dedup pipeline done: %s", result)
    return result
