"""
api/scrapers/base.py -- Shared base classes and data types for Reune VE.

Two independent hierarchies live here:

  BaseVEScraper    -- abstract periodic scraper (poll_recent / full_sweep).
                      Subclasses upsert into the 'reports' table (scraped aggregate).

  BaseSearchSource -- abstract per-query search source (search_person).
                      Subclasses are auto-registered via __init_subclass__ and
                      discovered at runtime by ExploratorySearchOrchestrator.
                      Adding a new source = subclass + set source_name + implement
                      search_person(). One import in __init__.py registers it.

Shared data types:
  SearchQuery   -- normalized input, built from 'reunion_reports' bot intake fields
  SearchResult  -- normalized output, one per candidate person found

IMPORTANT: Two tables exist in this project.
  reunion_reports -- WhatsApp bot intake (name TEXT, age TEXT, location TEXT, no embeddings)
  reports         -- scraper aggregate (full_name TEXT, age INT, text_embedding vector(768))
  These are NOT the same table. The exploratory search fires against 'reunion_reports'
  context and calls external APIs. The match_engine.py / pgvector path operates on
  'reports' and is NOT currently wired to the bot intake path.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False
    logger.warning(
        "rapidfuzz not installed -- name scoring degrades to constant 0.5. "
        "pip install rapidfuzz"
    )


# ---------------------------------------------------------------------------
# Shared data types
# ---------------------------------------------------------------------------

@dataclass
class SearchQuery:
    """
    Normalized input to every search source.
    Built from a 'reunion_reports' row (bot intake fields).
    """
    full_name: str                        # required; min 3 chars
    age: int | None = None               # parsed from age TEXT field
    last_seen_location: str | None = None # maps to location column
    kind: str = "missing"                # 'missing' | 'found'
    report_id: str | None = None         # reunion_reports.id UUID
    reporter_phone: str | None = None    # WhatsApp chat ID for follow-up


@dataclass
class SearchResult:
    """
    Normalized output from any search source.
    Stored in 'external_leads' table; displayed in WhatsApp follow-up.
    """
    source: str                           # source_name of the emitting class
    full_name: str
    score: float                          # composite 0.0-1.0 (main ranking signal)
    name_similarity: float                # rapidfuzz WRatio 0.0-1.0
    location: str | None = None          # hospital, city, or last-seen location
    age: int | None = None
    detail: str | None = None            # free-text from source
    contact: str | None = None           # phone number if present
    photo_url: str | None = None
    source_url: str | None = None        # direct URL to the source record
    kind: str | None = None              # 'missing'|'found'|'hospital_patient'|'safe'
    raw: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Scoring utilities (used by all sources)
# ---------------------------------------------------------------------------

def name_similarity(a: str, b: str) -> float:
    """
    WRatio fuzzy name match. Returns 0.0-1.0.
    Degrades to constant 0.5 if rapidfuzz is not installed.
    WRatio handles partial matches and word-order differences
    (e.g., 'Rodriguez Jose' vs 'Jose Rodriguez').
    """
    if not _HAS_RAPIDFUZZ or not a or not b:
        return 0.5
    return _fuzz.WRatio(a.lower(), b.lower()) / 100.0


def age_match_score(query_age: int | None, result_age: int | None) -> float:
    """
    1.0  -- match within 5 years
    0.6  -- match within 15 years
    0.0  -- outside 15-year window
    0.5  -- neutral (either age is None)
    """
    if query_age is None or result_age is None:
        return 0.5
    diff = abs(query_age - result_age)
    if diff <= 5:
        return 1.0
    if diff <= 15:
        return 0.6
    return 0.0


def location_match_score(query_loc: str | None, result_loc: str | None) -> float:
    """
    Partial ratio on location strings, 0.0-1.0.
    Returns 0.5 (neutral) if either location is missing.
    Uses partial_ratio to handle 'La Guaira' matching 'Maiquetia, La Guaira'.
    """
    if not query_loc or not result_loc:
        return 0.5
    if not _HAS_RAPIDFUZZ:
        return 0.5
    return _fuzz.partial_ratio(query_loc.lower(), result_loc.lower()) / 100.0


def composite_score(
    ns: float,
    age_s: float = 0.5,
    loc_s: float = 0.5,
    *,
    name_weight: float = 0.70,
    age_weight: float = 0.15,
    loc_weight: float = 0.15,
) -> float:
    """
    Weighted composite: name 70%, age 15%, location 15%.
    Name dominates because age and location are often missing in crisis data.
    """
    return name_weight * ns + age_weight * age_s + loc_weight * loc_s


def name_variants(full_name: str) -> list[str]:
    """
    Return search variants to maximise recall against fuzzy APIs.

    Order matters: try most specific first to exit early on a hit.
      1. Full name as given         ('Jose Rodriguez')
      2. Reversed token order       ('Rodriguez Jose')
      3. Last token (surname)       ('Rodriguez')
      4. First token (given name)   ('Jose')

    Filters out variants shorter than 3 chars (API minimum).
    Deduplicates while preserving order.
    """
    clean = full_name.strip()
    parts = clean.split()
    seen: set[str] = set()
    variants: list[str] = []

    candidates = [clean]
    if len(parts) >= 2:
        candidates.append(" ".join(reversed(parts)))
        candidates.append(parts[-1])  # surname
        if parts[0] != parts[-1]:
            candidates.append(parts[0])  # given name

    for c in candidates:
        if len(c) >= 3 and c not in seen:
            seen.add(c)
            variants.append(c)

    return variants


def parse_age_int(age_text: str | None) -> int | None:
    """
    Parse age from the reunion_reports.age TEXT field.
    Handles: '25', '25 años', 'aproximadamente 30', '~40', '30-35'.
    Returns None if unparseable.
    """
    if not age_text:
        return None
    import re
    m = re.search(r"\b(\d{1,3})\b", age_text)
    if m:
        age = int(m.group(1))
        return age if 0 < age < 120 else None
    return None


# ---------------------------------------------------------------------------
# BaseSearchSource -- per-query search (fires on each WhatsApp report)
# ---------------------------------------------------------------------------

class BaseSearchSource(ABC):
    """
    Abstract base for all per-query search sources.

    Subclasses are auto-registered at class-definition time via __init_subclass__.
    The orchestrator calls BaseSearchSource.build_sources() at startup.

    Contract for subclasses:
      - Set class attribute source_name: str (unique, stored in SearchResult.source)
      - Set class attribute timeout_seconds: float (per-source budget)
      - Implement search_person(query: SearchQuery) -> list[SearchResult]
      - Never raise from search_person; swallow exceptions and return []
      - Return results sorted by score descending

    Optional:
      - Set enabled = False to temporarily skip this source
    """

    _registry: list[type["BaseSearchSource"]] = []

    source_name: str = ""        # must be set by every concrete subclass
    timeout_seconds: float = 8.0
    enabled: bool = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Only register classes that set source_name (concrete, not abstract).
        if cls.source_name:
            BaseSearchSource._registry.append(cls)
            logger.debug("Registered search source: %s", cls.source_name)

    @classmethod
    def build_sources(cls) -> list["BaseSearchSource"]:
        """Instantiate all registered, enabled concrete sources."""
        return [
            klass()
            for klass in cls._registry
            if getattr(klass, "enabled", True)
        ]

    @abstractmethod
    async def search_person(self, query: SearchQuery) -> list[SearchResult]:
        """
        Search this source for a person matching query.
        Implementation must finish within timeout_seconds.
        Must never raise -- catch all exceptions internally and return [].
        """
        ...


# ---------------------------------------------------------------------------
# BaseVEScraper -- periodic scraper (poll_recent / full_sweep)
# ---------------------------------------------------------------------------

_SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")


class BaseVEScraper(ABC):
    """
    Abstract base for periodic scrapers that ingest into the 'reports' table.

    poll_recent()  -- lightweight, runs every 5 min; fetches recent N records
    full_sweep()   -- heavy, runs less frequently; paginates entire source

    Subclasses MAY also implement BaseSearchSource if the source site has a
    live search API. To do both, inherit from both:
        class MyScraper(BaseVEScraper, BaseSearchSource):
            source_name = "my_source"

    Note: the 'reports' table is separate from 'reunion_reports' (bot intake).
    Data ingested here is NOT automatically available for per-query bot search
    without a text embedding sidecar wired to the bot intake path.
    """

    source_name: str = ""

    def _sb_headers(self, prefer: str = "resolution=merge-duplicates,return=minimal") -> dict:
        return {
            "apikey": _SUPABASE_KEY,
            "Authorization": f"Bearer {_SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }

    async def upsert_report(self, data: dict) -> None:
        """
        Write a normalized report dict to the Supabase 'reports' table.
        Conflicts on (source, source_url) are merged (update existing row).

        Expected fields: kind, full_name, age (int), last_seen_location,
          distinguishing_marks, clothing, source, source_url, raw_data
        """
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{_SUPABASE_URL}/rest/v1/reports",
                headers=self._sb_headers(),
                params={"on_conflict": "source,source_url"},
                json=[data],
            )
            resp.raise_for_status()

    async def log_run(
        self,
        source: str,
        run_type: str,
        rows_inserted: int,
        rows_updated: int,
        error: str | None = None,
    ) -> None:
        """Write a run log entry to the 'scraper_runs' table."""
        row = {
            "source": source,
            "run_type": run_type,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "error": error,
        }
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.post(
                    f"{_SUPABASE_URL}/rest/v1/scraper_runs",
                    headers=self._sb_headers("return=minimal"),
                    json=[row],
                )
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("log_run failed for %s: %s", source, exc)

    async def close(self) -> None:
        """Release any persistent HTTP client sessions. Override if needed."""

    @abstractmethod
    async def poll_recent(self) -> int:
        """
        Fetch recently added records from the source.
        Returns count of rows upserted.
        """
        ...

    @abstractmethod
    async def full_sweep(self) -> int:
        """
        Full paginated crawl of the source. May take minutes.
        Returns count of rows upserted.
        """
        ...


# ---------------------------------------------------------------------------
# BaseScraper -- legacy aiohttp-based periodic scraper
# Used by: ReconexionScraper, SosVenezuelaScraper, VenezReportaScraper
# ---------------------------------------------------------------------------

try:
    import aiohttp as _aiohttp
    _HAS_AIOHTTP = True
except ImportError:
    _HAS_AIOHTTP = False

_PII_KEYS = frozenset({
    "cedula", "cedula_masked", "contacto", "telefono", "phone",
    "email", "direccion", "direccion_exacta", "numero_contacto",
})


def strip_pii(raw: dict) -> dict:
    """Remove known PII fields before storing raw_data."""
    return {k: v for k, v in raw.items() if k.lower() not in _PII_KEYS}


class BaseScraper(ABC):
    """
    Abstract base for aiohttp-backed scrapers that ingest into 'reports'.

    Subclasses must implement:
      fetch_page(page: int) -> list[dict]   -- one page of raw records
      normalize(raw: dict) -> Optional[dict] -- map to reports schema (None = skip)

    The base class provides:
      run_poll()  -- fetch page 1, upsert all records
      run_full()  -- paginate until empty page, upsert all records
      upsert_report(data) -- POST to Supabase /rest/v1/reports
      log_run(run_type, stats) -- write to scraper_runs table
    """

    def __init__(self, source_name: str, supabase_url: str, supabase_key: str) -> None:
        self.source_name = source_name
        self._supabase_url = supabase_url.rstrip("/")
        self._supabase_key = supabase_key
        self._session: Any = None  # aiohttp.ClientSession

    async def _session_get(self) -> Any:
        if not _HAS_AIOHTTP:
            raise RuntimeError("aiohttp is required for BaseScraper but not installed")
        if self._session is None or self._session.closed:
            self._session = _aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()

    def _sb_headers(self, prefer: str = "resolution=merge-duplicates,return=minimal") -> dict:
        return {
            "apikey": self._supabase_key,
            "Authorization": f"Bearer {self._supabase_key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }

    async def upsert_report(self, data: dict) -> bool:
        async with httpx.AsyncClient(timeout=15) as cl:
            resp = await cl.post(
                f"{self._supabase_url}/rest/v1/reports",
                headers=self._sb_headers(),
                params={"on_conflict": "source,source_url"},
                json=[data],
            )
            if resp.status_code in (200, 201):
                return True
            logger.warning(
                "[%s] upsert_report %d: %s",
                self.source_name, resp.status_code, resp.text[:150],
            )
            return False

    async def log_run(self, run_type: str, stats: dict) -> None:
        row = {
            "source": self.source_name,
            "run_type": run_type,
            "rows_inserted": stats.get("inserted", 0),
            "rows_updated": stats.get("updated", 0),
            "error": str(stats["errors"]) if stats.get("errors") else None,
        }
        async with httpx.AsyncClient(timeout=10) as cl:
            try:
                resp = await cl.post(
                    f"{self._supabase_url}/rest/v1/scraper_runs",
                    headers=self._sb_headers("return=minimal"),
                    json=[row],
                )
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("log_run failed for %s: %s", self.source_name, exc)

    @abstractmethod
    async def fetch_page(self, page: int) -> list[dict]:
        ...

    @abstractmethod
    def normalize(self, raw: dict) -> Any:
        ...

    async def run_poll(self, poll_interval: int = 300) -> dict:
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        try:
            rows = await self.fetch_page(1)
            for raw in rows:
                try:
                    normalized = self.normalize(raw)
                    if normalized is None:
                        continue
                    ok = await self.upsert_report(normalized)
                    stats["inserted" if ok else "errors"] += 1
                except Exception as exc:
                    logger.error("[%s] poll record error: %s", self.source_name, exc)
                    stats["errors"] += 1
        except Exception as exc:
            logger.error("[%s] poll error: %s", self.source_name, exc)
            stats["errors"] += 1
        await self.log_run("poll", stats)
        logger.info("[%s] poll done: %s", self.source_name, stats)
        return stats

    # Safety bound: stop paginating after this many pages so a source that never
    # returns an empty page (API quirk / loop) can't spin forever each sweep.
    _MAX_FULL_PAGES = 2000

    async def run_full(self, poll_interval: int = 3600) -> dict:
        stats: dict[str, int] = {"inserted": 0, "updated": 0, "errors": 0}
        page = 1
        while True:
            if page > self._MAX_FULL_PAGES:
                logger.warning("[%s] full sweep hit page cap %d — stopping",
                               self.source_name, self._MAX_FULL_PAGES)
                break
            try:
                rows = await self.fetch_page(page)
            except Exception as exc:
                logger.error("[%s] full page %d error: %s", self.source_name, page, exc)
                stats["errors"] += 1
                break
            if not rows:
                break
            for raw in rows:
                try:
                    normalized = self.normalize(raw)
                    if normalized is None:
                        continue
                    ok = await self.upsert_report(normalized)
                    stats["inserted" if ok else "errors"] += 1
                except Exception as exc:
                    logger.error("[%s] full record error: %s", self.source_name, exc)
                    stats["errors"] += 1
            page += 1
        await self.log_run("full", stats)
        logger.info("[%s] full done: %s", self.source_name, stats)
        return stats
