"""
scrapers/venezuela_te_busca.py -- Recurring scraper for Venezuela Te Busca.

Source: https://venezuelatebusca.com/
Kind: both (missing and found)

Venezuela Te Busca is a React Router 7 (Remix v2) SSR app that lists missing-
persons reports submitted after the June 2026 Venezuela earthquake. As of
2026-06-27 it holds ~37,297 records across ~1,554 pages of 24.

Data endpoint:
  GET /.data?_routes=routes%2F_index&page=N
  Response: flat JSON array in turbo-stream encoding (~157 KB per page)
  No authentication required for page/data loads.

Bot protection:
  Cloudflare WAF is present but passes page loads with browser-like headers.
  Cloudflare Turnstile only fires on form submissions (POST), not on GET.
  A shared httpx.AsyncClient carries session cookies across the full sweep,
  which is the "session management" the spec notes reference.

Turbo-stream decoding:
  The response is a flat JSON array. Encoded objects use {"_N": V} notation:
    - N is an integer -> field name is arr[N]
    - V is a negative integer -> null sentinel (maps to None)
    - V is a non-negative integer -> index into arr (arr[V] is the value)
    - V is a bool or None -> stored inline (returned as-is)
  The _decode_turbostream() function decodes person dicts from this array.
  PII fields (reporter, finder, tips, sources) are excluded at decode time
  via an explicit whitelist -- they are never stored in raw_data.

Kind assignment:
  status == "missing"                      -> kind = "missing"
  status in {"found", "already_found"}     -> kind = "found"
  hospitalStatus == "deceased" is stored in distinguishing_marks; kind stays
  "found". There is no boolean deceased field (constraint 3).

Deduplication:
  source_url = "venezuelatebusca:{uuid}" (UUID stable from source)
  resolution = ignore-duplicates (constraint 6).
  Tradeoff: status changes (missing -> found) will NOT propagate on re-run.
  Change the Prefer header in upsert_report() to merge-duplicates if the team
  decides freshness matters more than immutability.

Tilores note:
  If a Tilores bulk import already covers historical records, run only
  run_poll() after the snapshot date to avoid double-counting. The
  ignore-duplicates resolution ensures no within-source duplicates
  regardless. Cross-source deduplication is the match_engine's job.

Register in scraper_orchestrator._make_scrapers():
    from scrapers.venezuela_te_busca import VenezuelaTeBuscaScraper
    scrapers["venezuela_te_busca"] = VenezuelaTeBuscaScraper(sb_url, sb_key)
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import httpx

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

_SOURCE_NAME = "venezuela_te_busca"
_BASE_URL = "https://venezuelatebusca.com"
_DATA_PATH = "/.data"
_ROUTES_PARAM = "routes/_index"

_PAGE_SIZE = 24     # Fixed by the source (24 records per page)
_POLL_PAGES = 3     # Pages fetched per poll cycle: 72 newest records
_PAGE_DELAY = 0.35  # Seconds between pages during full sweep
_MAX_PAGES = 2500   # Safety cap (~1,554 pages at 37k records as of 2026-06-27)
_REQUEST_TIMEOUT = 30

# Browser-like headers required to pass Cloudflare page-load fingerprinting.
# Turnstile is present but only activates on form submissions, not GET requests.
_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/x-component,*/*;q=0.9",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.8",
    "Referer": "https://venezuelatebusca.com/",
    "DNT": "1",
}

# Explicit whitelist of scalar fields to extract from each person object.
# Everything outside this set is ignored at decode time.
_PERSON_SCALAR_KEYS: frozenset[str] = frozenset({
    "id",
    "firstName",
    "lastName",
    "idNumber",
    "age",
    "gender",
    "lastSeen",
    "description",
    "status",
    "photoUrl",
    "createdAt",
    "updatedAt",
    "hospitalName",
    "hospitalStatus",
    "foundNote",
})

# PII-bearing fields excluded entirely -- never decoded, never reach raw_data.
# reporter: {name, phone, email} of the person who submitted the report
# finder:   {name, phone} of the person who found the subject
# tips:     list of follow-up messages with reporter contact info
# sources:  external links that may include contact details
_PII_KEYS: frozenset[str] = frozenset({
    "reporter",
    "finder",
    "tips",
    "sources",
    "lastActivityAt",
})

# status values that map to kind="found"
_FOUND_STATUSES: frozenset[str] = frozenset({"found", "already_found"})


# ---------------------------------------------------------------------------
# Turbo-stream decoder (module-level for testability)
# ---------------------------------------------------------------------------

def _resolve(v: Any, arr: list) -> Any:
    """
    Resolve a turbo-stream encoded value from the flat array.

    Rules:
    - bool/None: returned as-is (stored inline in encoded objects)
    - negative int: null sentinel -> None
    - non-negative int: index into arr -> arr[v]
    - anything else (float, str): returned as-is (rare; treat as direct value)

    The bool check MUST come before the int check because in Python
    bool is a subclass of int: isinstance(True, int) is True.
    Without the explicit bool guard, True would resolve to arr[1].
    """
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, int):
        if v < 0:
            return None       # all negative sentinels (-5, -7, ...) -> None
        if v < len(arr):
            return arr[v]
        return None           # out-of-bounds index
    return v                  # direct scalar (string, float, etc.)


def _decode_turbostream(arr: list) -> tuple[list[dict], bool]:
    """
    Decode a turbo-stream flat array from VTB's React Router 7 data endpoint.

    Returns (persons, has_more) where:
    - persons:   list of raw person dicts containing only _PERSON_SCALAR_KEYS;
                 PII fields (_PII_KEYS) are excluded at this layer.
    - has_more:  True if more pages exist (defaults to True on parsing failure
                 so the caller falls back to the empty-list termination check).

    Algorithm:
    1. Locate the string "persons" in the flat array to get its key index.
    2. Find the encoded data object that owns "persons" as a field (the object
       that contains {"_<persons_key_idx>": <persons_list_ref>}).
    3. Decode that data object to extract the persons list reference and hasMore.
    4. For each integer reference in the persons list, decode the person object
       at arr[ref], extracting only whitelisted scalar fields.

    Encoded objects use {"_N": V} notation: N indexes the key name string
    in arr, V is a reference resolved by _resolve().
    """
    if not isinstance(arr, list) or len(arr) < 5:
        return [], False

    # Step 1: find "persons" key position
    try:
        persons_key_pos = arr.index("persons")
    except ValueError:
        return [], False

    persons_encoded_key = f"_{persons_key_pos}"

    # Step 2 & 3: find the data-level encoded object and decode it
    persons_refs: list = []
    has_more: bool = True   # default True: fall back to empty-list termination

    for item in arr:
        if not isinstance(item, dict) or persons_encoded_key not in item:
            continue

        # Decode all fields of the data object
        for enc_key, v in item.items():
            if not enc_key.startswith("_"):
                continue
            try:
                key_idx = int(enc_key[1:])
            except ValueError:
                continue
            if key_idx < 0 or key_idx >= len(arr):
                continue

            key_name = arr[key_idx]
            val = _resolve(v, arr)

            if key_name == "persons":
                if isinstance(val, list):
                    persons_refs = val
            elif key_name == "hasMore":
                if val is not None:
                    has_more = bool(val)

        break  # exactly one data object owns the "persons" field

    # Step 4: decode each person object
    persons: list[dict] = []

    for ref in persons_refs:
        if not isinstance(ref, int) or ref < 0 or ref >= len(arr):
            continue

        encoded_person = arr[ref]
        if not isinstance(encoded_person, dict):
            continue

        person: dict[str, Any] = {}
        for enc_key, v in encoded_person.items():
            if not enc_key.startswith("_"):
                continue
            try:
                key_idx = int(enc_key[1:])
            except ValueError:
                continue
            if key_idx < 0 or key_idx >= len(arr):
                continue

            key_name = arr[key_idx]

            # Whitelist: skip PII and unknown fields entirely
            if key_name in _PII_KEYS or key_name not in _PERSON_SCALAR_KEYS:
                continue

            val = _resolve(v, arr)

            # Skip nested objects/lists (unexpected for whitelisted scalar keys)
            if isinstance(val, (dict, list)):
                continue

            person[key_name] = val

        persons.append(person)

    return persons, has_more


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class VenezuelaTeBuscaScraper(BaseScraper):
    """
    Recurring scraper for Venezuela Te Busca (https://venezuelatebusca.com/).

    Fetches person reports (missing and found) via the React Router 7
    single-fetch data endpoint, decodes the turbo-stream flat-array format,
    and upserts into the Supabase 'reports' table.

    Constructor: VenezuelaTeBuscaScraper(supabase_url, supabase_key)
    Matches the BaseScraper pattern; credentials passed explicitly so the
    orchestrator can control which Supabase project is the target.

    Register in scraper_orchestrator._make_scrapers():
        from scrapers.venezuela_te_busca import VenezuelaTeBuscaScraper
        scrapers["venezuela_te_busca"] = VenezuelaTeBuscaScraper(sb_url, sb_key)
    """

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        super().__init__(_SOURCE_NAME, supabase_url, supabase_key)

    # ------------------------------------------------------------------
    # Constraint 6 override: ignore-duplicates (not base-class merge)
    # ------------------------------------------------------------------

    async def upsert_report(self, data: dict) -> bool:
        """
        POST a single report with resolution=ignore-duplicates.

        Deduplication key: (source, source_url) = ("venezuela_te_busca", "venezuelatebusca:{uuid}").

        Tradeoff: first-seen state is preserved. Status changes (missing -> found)
        will NOT propagate on re-runs. Change the Prefer header below to
        'resolution=merge-duplicates,return=minimal' if the team decides
        freshness (e.g., reunification updates) is more important.
        """
        async with httpx.AsyncClient(timeout=15) as cl:
            resp = await cl.post(
                f"{self._supabase_url}/rest/v1/reports",
                headers=self._sb_headers(
                    "resolution=ignore-duplicates,return=minimal"
                ),
                params={"on_conflict": "source,source_url"},
                json=[data],
            )
            if resp.status_code in (200, 201):
                return True
            logger.warning(
                "[%s] upsert_report HTTP %d: %s",
                self.source_name,
                resp.status_code,
                resp.text[:150],
            )
            return False

    # ------------------------------------------------------------------
    # Internal HTTP fetch
    # ------------------------------------------------------------------

    async def _fetch_raw_page(
        self, page: int, client: httpx.AsyncClient
    ) -> tuple[list[dict], bool]:
        """
        Fetch one page from the VTB turbo-stream endpoint and decode it.

        Uses the provided shared client so Cloudflare session cookies persist
        across the full sweep. Returns ([], False) on any error so the caller
        can log and continue -- never raises.

        Includes one retry with a 2-second wait on transient failures.
        """
        params: dict[str, Any] = {"_routes": _ROUTES_PARAM, "page": page}
        url = f"{_BASE_URL}{_DATA_PATH}"

        last_exc: Optional[Exception] = None
        for attempt in range(2):
            try:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                arr = resp.json()
                if not isinstance(arr, list):
                    logger.warning(
                        "[%s] page %d: unexpected response type %s",
                        _SOURCE_NAME, page, type(arr).__name__,
                    )
                    return [], False
                return _decode_turbostream(arr)
            except Exception as exc:
                last_exc = exc
                if attempt == 0:
                    await asyncio.sleep(2.0)

        logger.error(
            "[%s] page %d failed after 2 attempts: %s", _SOURCE_NAME, page, last_exc
        )
        return [], False

    # ------------------------------------------------------------------
    # BaseScraper abstract contract
    # ------------------------------------------------------------------

    async def fetch_page(self, page: int) -> list[dict]:
        """
        Satisfy the BaseScraper abstract method.

        Creates a per-call httpx client (stateless; no cookie sharing).
        Used if the base-class run_poll/run_full are called directly.
        The overridden run_poll() and run_full() below use _fetch_raw_page()
        with a shared client instead, which is preferred.
        """
        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            persons, _ = await self._fetch_raw_page(page, client)
            return persons

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Map a decoded VTB person dict to the 'reports' table schema.

        Field mapping:
          firstName + lastName -> full_name (trimmed, space-joined)
          idNumber             -> "CI: {idNumber}" prefix in distinguishing_marks
          age                  -> age (int, validated 0 < n < 120; None if missing)
          lastSeen             -> last_seen_location
          description          -> appended to distinguishing_marks
          hospitalName         -> appended as "Hospital: {name}"
          hospitalStatus       -> appended as "Estado hospital: {status}"
                                  (includes "deceased" -- never stored as a bool field)
          foundNote            -> appended as "Nota: {note}" (resolver's notes)
          status               -> kind ("missing" -> "missing"; "found"/"already_found"
                                  -> "found")
          gender, photoUrl, status, createdAt, updatedAt,
          hospitalName, hospitalStatus -> raw_data (safe, no PII)
          id                   -> source_url suffix (stable UUID)

        PII exclusion:
          reporter / finder / tips / sources are excluded upstream in
          _decode_turbostream() -- they never reach this method.
        """
        person_id = (raw.get("id") or "").strip()
        if not person_id:
            return None

        first = (raw.get("firstName") or "").strip()
        last = (raw.get("lastName") or "").strip()
        full_name = f"{first} {last}".strip()
        if not full_name:
            return None

        # kind assignment
        status = (raw.get("status") or "missing").lower().strip()
        kind = "found" if status in _FOUND_STATUSES else "missing"

        # age (validated)
        age_int: Optional[int] = None
        age_val = raw.get("age")
        if age_val is not None:
            try:
                candidate = int(age_val)
                if 0 < candidate < 120:
                    age_int = candidate
            except (TypeError, ValueError):
                pass

        # last_seen_location
        location: Optional[str] = (raw.get("lastSeen") or "").strip() or None

        # distinguishing_marks: CI + description + hospital info + foundNote
        marks_parts: list[str] = []

        id_number = (raw.get("idNumber") or "").strip()
        if id_number:
            marks_parts.append(f"CI: {id_number}")

        description = (raw.get("description") or "").strip()
        if description:
            marks_parts.append(description)

        hospital_name = (raw.get("hospitalName") or "").strip()
        if hospital_name:
            marks_parts.append(f"Hospital: {hospital_name}")

        hospital_status = (raw.get("hospitalStatus") or "").strip()
        if hospital_status:
            marks_parts.append(f"Estado hospital: {hospital_status}")

        found_note = (raw.get("foundNote") or "").strip()
        if found_note:
            marks_parts.append(f"Nota: {found_note}")

        marks: Optional[str] = " | ".join(marks_parts) if marks_parts else None
        if marks and len(marks) > 500:
            marks = marks[:497] + "..."

        # raw_data: explicit whitelist -- no PII (reporter/finder/tips excluded upstream)
        raw_data: dict[str, Any] = {}
        for field_name in (
            "status", "gender", "photoUrl", "createdAt", "updatedAt",
            "hospitalName", "hospitalStatus",
        ):
            val = raw.get(field_name)
            if val is not None:
                raw_data[field_name] = val

        return {
            "kind": kind,
            "full_name": full_name,
            "age": age_int,
            "last_seen_location": location,
            "distinguishing_marks": marks,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"venezuelatebusca:{person_id}",
            "raw_data": raw_data,
        }

    # ------------------------------------------------------------------
    # Overridden run methods: shared client, delays, multi-page poll
    # ------------------------------------------------------------------

    async def run_poll(self, poll_interval: int = 300) -> dict:
        """
        Fetch the newest _POLL_PAGES pages (72 records by default).

        Records are returned newest-first, so page 1 holds the most recent
        submissions. During an active crisis, more than 24 records can arrive
        between 5-minute poll cycles, so _POLL_PAGES=3 is a safer window.

        Uses a single shared httpx client across all poll pages so Cloudflare
        session cookies (if issued) persist within the poll.

        Returns dict: {"inserted": N, "updated": 0, "errors": M}
        """
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}

        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            for page in range(1, _POLL_PAGES + 1):
                persons, _ = await self._fetch_raw_page(page, client)
                if not persons:
                    break
                for raw in persons:
                    try:
                        normalized = self.normalize(raw)
                        if normalized is None:
                            continue
                        ok = await self.upsert_report(normalized)
                        stats["inserted" if ok else "errors"] += 1
                    except Exception as exc:
                        logger.error(
                            "[%s] poll record error: %s", self.source_name, exc
                        )
                        stats["errors"] += 1

        await self.log_run("poll", stats)
        logger.info("[%s] poll done: %s", self.source_name, stats)
        return stats

    async def run_full(self, poll_interval: int = 3600) -> dict:
        """
        Paginate all pages until the persons list is empty or hasMore is False.

        Primary termination: empty persons list (confirmed safe by testing
        page=99999, which returns an empty persons array).
        Secondary termination: hasMore=False from the pagination object
        (avoids one extra request after the last page).

        A _PAGE_DELAY second sleep between pages avoids hammering the site
        across ~1,554 pages. A _MAX_PAGES safety cap prevents infinite loops
        if the site changes behavior.

        Tilores note: if a Tilores bulk import already covers historical data,
        skip this method and run only run_poll() after the import date.
        The ignore-duplicates resolution ensures no within-source duplication
        but calling run_full() after a Tilores import wastes ~9 minutes.

        Returns dict: {"inserted": N, "updated": 0, "errors": M}
        """
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}

        async with httpx.AsyncClient(
            timeout=_REQUEST_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            page = 1
            while page <= _MAX_PAGES:
                if page > 1:
                    await asyncio.sleep(_PAGE_DELAY)

                persons, has_more = await self._fetch_raw_page(page, client)

                if not persons:
                    logger.info(
                        "[%s] full_sweep: page %d returned 0 records, stopping.",
                        self.source_name, page,
                    )
                    break

                for raw in persons:
                    try:
                        normalized = self.normalize(raw)
                        if normalized is None:
                            continue
                        ok = await self.upsert_report(normalized)
                        stats["inserted" if ok else "errors"] += 1
                    except Exception as exc:
                        logger.error(
                            "[%s] full record error: %s", self.source_name, exc
                        )
                        stats["errors"] += 1

                logger.info(
                    "[%s] full_sweep page %d: %d records, has_more=%s, total_inserted=%d",
                    self.source_name, page, len(persons), has_more,
                    stats["inserted"],
                )

                if not has_more:
                    break

                page += 1

        await self.log_run("full", stats)
        logger.info("[%s] full done: %s", self.source_name, stats)
        return stats
