"""
scrapers/pacientes_terremoto.py -- Pacientes en Hospitales tras Terremoto VZLA.

Source:  https://pacientesterremotovzla.lovable.app/
Backend: Supabase project isvgkrgdvhhbuznwgxlt (extracted from JS bundle)
API:     Supabase REST v1, public anon key (baked into the SPA bundle --
         this is a read-only publishable key, not a secret).

Data: hospital patients confirmed after the June 24 2026 Venezuela earthquake.
kind: 'found' for all records -- persons located and receiving care in hospitals.

Dedup: resolution=ignore-duplicates on (source, source_url). This freezes
first-seen patient state. Tradeoff: status changes (estable -> dado de alta)
will NOT propagate on re-runs. The base class default is merge-duplicates and
most scrapers prefer that for freshness, but the project constraint marks
ignore-duplicates non-negotiable. A one-line change in upsert_report below
switches back to merge if the team decides freshness matters more.

Credentials: _ANON_KEY is Supabase's 'sb_publishable_' format, designed to be
public (equivalent to Stripe's pk_). Read from PACIENTES_TERREMOTO_ANON_KEY
env var first; falls back to the known-public bundle value so the scraper
works out of the box without extra config.
Using BaseVEScraper (httpx, poll_recent/full_sweep) instead of legacy
BaseScraper (aiohttp, fetch_page/normalize) because: (1) constraint requires
httpx; (2) terremotove.py is the direct analog and uses this interface.

Activation: register in scraper_orchestrator._make_scrapers() to enable:
    from scrapers.pacientes_terremoto import PacientesTerremotoVZLAScraper
    scrapers["pacientes_terremoto"] = PacientesTerremotoVZLAScraper()

Confirmed 3,964 records as of 2026-06-27. Ready to enable.

Poll strategy:
  poll_recent  -- 200 most recently updated rows (order=updated_at.desc).
  full_sweep   -- paginate all rows, PAGE_SIZE batches, order=id.asc for
                  deterministic coverage even if rows are inserted mid-sweep.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from .base import BaseVEScraper

# Our own Supabase (output target) -- same env vars BaseVEScraper uses
_OWN_SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
_OWN_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

logger = logging.getLogger(__name__)

_SOURCE_NAME = "pacientes_terremoto"

# Public anon (publishable) key embedded verbatim in the SPA JS bundle at
# https://pacientesterremotovzla.lovable.app/assets/index-D-JHs570.js
# Supabase anon keys are designed to be public; RLS restricts them to
# read-only SELECT on public tables. Safe to store in source code.
# Read from env first to allow override without a code change.
_ANON_KEY: str = os.environ.get(
    "PACIENTES_TERREMOTO_ANON_KEY",
    "sb_publishable_RA6UKM1XFORLMkRnglxuVQ_34u2bxYT",
)
_REMOTE_SUPABASE_URL = "https://isvgkrgdvhhbuznwgxlt.supabase.co"
_PEOPLE_URL = f"{_REMOTE_SUPABASE_URL}/rest/v1/people"

_PAGE_SIZE = 200

# Fields stripped from raw_data before storage (PII or internal FK noise)
_STRIP_KEYS = frozenset({"reported_by", "hospital_id"})

# Supabase select projection -- join hospitals in one request
_SELECT = (
    "id,full_name,age,status,notes,is_safe,"
    "created_at,updated_at,"
    "hospitals(name,city,state)"
)


class PacientesTerremotoVZLAScraper(BaseVEScraper):
    """
    Periodic scraper for Pacientes en Hospitales tras Terremoto VZLA.

    Hits the Supabase REST API of the Lovable SPA using the public anon key
    extracted from the frontend JS bundle. Fetches hospital patient records
    (kind='found') with an embedded join to populate last_seen_location.

    A single httpx.AsyncClient is shared across all requests within a scraper
    instance to avoid per-record TCP connection overhead on full_sweep (~4k rows).
    Call close() at application shutdown to release the client.

    Constructor takes no arguments -- reads output Supabase credentials from
    SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY env vars (via BaseVEScraper).

    Register in scraper_orchestrator._make_scrapers() to activate:
        scrapers["pacientes_terremoto"] = PacientesTerremotoVZLAScraper()
    """

    source_name = _SOURCE_NAME

    def __init__(self) -> None:
        super().__init__()
        # Shared client reused for all fetches and upserts within this instance.
        # Eliminates per-record TCP teardown/setup on full_sweep (~4 k rows).
        # Default timeout=30 covers source API fetches; upsert_report overrides
        # to 15 s per POST via the per-request timeout parameter.
        self._http: httpx.AsyncClient = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        """Release the shared HTTP client. Call at application shutdown."""
        await self._http.aclose()

    def _api_headers(self) -> dict[str, str]:
        """HTTP headers for the source Supabase REST API (anon key)."""
        return {
            "apikey": _ANON_KEY,
            "Authorization": f"Bearer {_ANON_KEY}",
            "Accept": "application/json",
        }

    async def upsert_report(self, data: dict) -> None:
        """
        Override base upsert to use ignore-duplicates per project constraint 6.
        First-seen patient state is preserved; status changes on re-runs are
        silently ignored. Revert to merge-duplicates if freshness is needed.

        Raises RuntimeError early if SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY
        are unset, avoiding an opaque httpx connection error on a malformed URL.
        """
        if not _OWN_SUPABASE_URL or not _OWN_SUPABASE_KEY:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars must be set"
            )
        headers = {
            "apikey": _OWN_SUPABASE_KEY,
            "Authorization": f"Bearer {_OWN_SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=ignore-duplicates,return=minimal",
        }
        resp = await self._http.post(
            f"{_OWN_SUPABASE_URL}/rest/v1/reports",
            headers=headers,
            params={"on_conflict": "source,source_url"},
            json=[data],
            timeout=15,  # tighter budget for writes vs. source API reads (30 s)
        )
        resp.raise_for_status()

    @staticmethod
    def _hospital_location(hospital: dict | None) -> str | None:
        """
        Build a location string from the embedded hospitals join dict.
        Format: 'Name, City, State' -- missing parts are skipped.
        """
        if not hospital:
            return None
        parts = [
            hospital.get("name"),
            hospital.get("city"),
            hospital.get("state"),
        ]
        location = ", ".join(p for p in parts if p)
        return location or None

    @staticmethod
    def _normalize(raw: dict) -> dict | None:
        """
        Map a Supabase 'people' row (with embedded 'hospitals') to the
        'reports' table schema. Returns None if full_name is empty.

        Field mapping:
          full_name  -> full_name (already uppercase in source)
          age        -> age (int, already numeric in source)
          hospitals  -> last_seen_location (joined name, city, state)
          status     -> distinguishing_marks prefix 'Estado: X'
          is_safe    -> appended to distinguishing_marks if True
          notes      -> appended to distinguishing_marks
          kind       -> 'found' always (confirmed hospital patient)
          source_url -> 'pacientes_terremoto:{uuid}' for deduplication
          raw_data   -> stripped of PII keys (reported_by) and FK noise
        """
        full_name = (raw.get("full_name") or "").strip()
        if not full_name:
            return None

        person_id = raw.get("id", "")
        hospital = raw.get("hospitals") or {}
        location = PacientesTerremotoVZLAScraper._hospital_location(hospital)

        # Build distinguishing_marks: status + is_safe flag + notes
        marks_parts: list[str] = []
        status = (raw.get("status") or "").strip()
        if status:
            marks_parts.append(f"Estado: {status}")
        if raw.get("is_safe"):
            marks_parts.append("Confirmado en buen estado")
        notes = (raw.get("notes") or "").strip()
        if notes:
            marks_parts.append(notes)
        marks: str | None = " | ".join(marks_parts) if marks_parts else None
        if marks and len(marks) > 500:
            marks = marks[:497] + "..."

        age_int: int | None = None
        age_val = raw.get("age")
        if age_val is not None:
            try:
                candidate = int(age_val)
                age_int = candidate if 0 < candidate < 120 else None
            except (TypeError, ValueError):
                age_int = None

        raw_data = {
            k: v
            for k, v in raw.items()
            if k.lower() not in _STRIP_KEYS and k != "hospitals"
        }

        return {
            "kind": "found",
            "full_name": full_name,
            "age": age_int,
            "last_seen_location": location,
            "distinguishing_marks": marks,
            "clothing": None,
            "source": _SOURCE_NAME,
            "source_url": f"pacientes_terremoto:{person_id}",
            "raw_data": raw_data,
        }

    async def _fetch_page(
        self,
        offset: int,
        limit: int = _PAGE_SIZE,
        order: str = "id.asc",
    ) -> list[dict]:
        """
        Fetch one batch from the source Supabase REST API.

        Uses embedded select to join hospitals in a single request.
        Default order=id.asc gives deterministic pagination even when new
        rows are inserted mid-sweep (stable cursor). poll_recent overrides
        to updated_at.desc to surface the most recently changed admissions.

        Raises on any HTTP or parsing error. Callers are responsible for
        catching and deciding whether to abort (full_sweep) or surface the
        error (poll_recent outer try). The previous swallow-and-return-[]
        pattern masked transient 5xx errors as end-of-data, truncating sweeps
        silently and logging a clean run.
        """
        params: dict[str, Any] = {
            "select": _SELECT,
            "limit": limit,
            "offset": offset,
            "order": order,
        }
        resp = await self._http.get(
            _PEOPLE_URL,
            headers=self._api_headers(),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()

    async def poll_recent(self) -> int:
        """
        Fetch the 200 most recently updated records.
        Lightweight; runs every few minutes to catch new hospital admissions.
        Returns count of rows upserted.
        """
        count = 0
        errors = 0
        error: str | None = None
        try:
            records = await self._fetch_page(
                offset=0,
                limit=_PAGE_SIZE,
                order="updated_at.desc",
            )
            for raw in records:
                try:
                    normalized = self._normalize(raw)
                    if normalized is None:
                        continue
                    await self.upsert_report(normalized)
                    count += 1
                except Exception as exc:
                    errors += 1
                    logger.error(
                        "PacientesTerremotoVZLAScraper.poll_recent record error: %s",
                        exc,
                    )
        except Exception as exc:
            error = str(exc)
            logger.error(
                "PacientesTerremotoVZLAScraper.poll_recent outer error: %s", exc
            )
        await self.log_run(_SOURCE_NAME, "poll_recent", count, errors, error)
        return count

    async def full_sweep(self) -> int:
        """
        Paginate all records in PAGE_SIZE batches ordered by id ASC.
        Ordering by id gives stable cursor semantics: rows inserted mid-sweep
        land at the end of the id sequence and are picked up on the next page.
        Stops when a page returns fewer rows than PAGE_SIZE.

        A fetch failure (network error, 5xx) is caught at the page level,
        recorded in error, and causes an immediate break so log_run reports
        the failure instead of a falsely clean run with truncated data.

        Returns total count of rows upserted.
        """
        total = 0
        errors = 0
        error: str | None = None
        offset = 0
        try:
            while True:
                try:
                    records = await self._fetch_page(
                        offset=offset,
                        limit=_PAGE_SIZE,
                        order="id.asc",
                    )
                except Exception as exc:
                    error = str(exc)
                    logger.error(
                        "PacientesTerremotoVZLAScraper.full_sweep"
                        " offset=%d fetch error: %s",
                        offset,
                        exc,
                    )
                    break

                if not records:
                    break

                for raw in records:
                    try:
                        normalized = self._normalize(raw)
                        if normalized is None:
                            continue
                        await self.upsert_report(normalized)
                        total += 1
                    except Exception as exc:
                        errors += 1
                        logger.error(
                            "PacientesTerremotoVZLAScraper.full_sweep record error: %s",
                            exc,
                        )

                logger.info(
                    "PacientesTerremotoVZLAScraper.full_sweep"
                    " offset=%d rows=%d total=%d",
                    offset,
                    len(records),
                    total,
                )
                if len(records) < _PAGE_SIZE:
                    break
                offset += _PAGE_SIZE
        except Exception as exc:
            error = str(exc)
            logger.error(
                "PacientesTerremotoVZLAScraper.full_sweep outer error: %s", exc
            )
        await self.log_run(_SOURCE_NAME, "full_sweep", total, errors, error)
        return total
