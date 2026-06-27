"""
api/scrapers/internal_source.py -- Per-query search against our own Supabase DB.

Searches the 'reunion_reports' table (WhatsApp bot intake) via ILIKE on the name
field for reports of the OPPOSITE kind to the incoming query.

Design note on pgvector vs ILIKE:
  The match_engine.py module uses pgvector (match_reports_by_text RPC) against the
  'reports' table (scraper aggregate), which requires a SentenceTransformer model
  (~500MB-1GB RAM). The Reune container is capped at 256MB (CLAUDE.md rule 5) and
  match_engine.process_new_report is not currently wired to the bot intake path.
  Until a dedicated embedding sidecar is available, per-query internal search uses
  ILIKE on 'reunion_reports' instead. This covers bot-submitted reports only.

  The scraped data in 'reports' is reachable via the periodic match_engine runs
  when process_new_report is properly wired. That is a separate task.

Threshold: only returns results where name_similarity >= 0.45. ILIKE pre-filters
by substring; rapidfuzz post-filters by whole-name similarity to eliminate
low-quality partial matches (e.g., 'Rodriguez' matching 'Jose Rodriguez de la Cruz').
"""
from __future__ import annotations

import logging
import os

import httpx

from .base import (
    BaseSearchSource,
    SearchQuery,
    SearchResult,
    age_match_score,
    composite_score,
    location_match_score,
    name_similarity,
    parse_age_int,
)

logger = logging.getLogger(__name__)

_SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Minimum name similarity to include in results (post-ILIKE filter)
_MIN_NAME_SIM = 0.45


class InternalReunionSource(BaseSearchSource):
    """
    Searches 'reunion_reports' (bot intake) for candidates of the opposite kind.
    Uses ILIKE on the name column + rapidfuzz post-filtering.
    Covers bot-submitted reports not yet in the scraped 'reports' aggregate.
    """

    source_name = "internal_reunion"
    timeout_seconds = 5.0

    async def search_person(self, query: SearchQuery) -> list[SearchResult]:
        opposite_kind = "found" if query.kind == "missing" else "missing"

        # Sanitize name for ILIKE (remove % _ which are LIKE wildcards)
        safe_name = query.full_name.replace("%", "").replace("_", " ").strip()
        if len(safe_name) < 3:
            return []

        # Extract first surname token for ILIKE; a broader match than the full name
        # increases recall. rapidfuzz post-filters precision.
        tokens = safe_name.split()
        search_token = tokens[-1] if len(tokens) >= 2 else safe_name

        params: dict[str, str] = {
            "name": f"ilike.*{search_token}*",
            "kind": f"eq.{opposite_kind}",
            "select": "id,kind,name,age,location,marks,found_state,photo_url",
            "order": "created_at.desc",
            "limit": "30",
        }
        headers = {
            "apikey": _SUPABASE_KEY,
            "Authorization": f"Bearer {_SUPABASE_KEY}",
        }

        results: list[SearchResult] = []
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                resp = await client.get(
                    f"{_SUPABASE_URL}/rest/v1/reunion_reports",
                    headers=headers,
                    params=params,
                )
                if resp.status_code != 200:
                    logger.warning(
                        "InternalReunionSource HTTP %s", resp.status_code
                    )
                    return []

                for rec in resp.json() or []:
                    result_name = (rec.get("name") or "").strip()
                    if not result_name:
                        continue

                    ns = name_similarity(query.full_name, result_name)
                    if ns < _MIN_NAME_SIM:
                        continue

                    result_age = parse_age_int(rec.get("age"))
                    location = rec.get("location")
                    marks = rec.get("marks")

                    age_s = age_match_score(query.age, result_age)
                    loc_s = location_match_score(query.last_seen_location, location)
                    score = composite_score(ns, age_s, loc_s)

                    kind_map = {"missing": "missing", "found": "found"}
                    result_kind = kind_map.get(rec.get("kind", ""), rec.get("kind"))

                    results.append(SearchResult(
                        source=self.source_name,
                        full_name=result_name,
                        score=round(score, 3),
                        name_similarity=round(ns, 3),
                        location=location,
                        age=result_age,
                        detail=marks,
                        photo_url=rec.get("photo_url"),
                        source_url=None,
                        kind=result_kind,
                        raw=rec,
                    ))

        except Exception as exc:
            logger.error("InternalReunionSource.search_person failed: %s", exc)

        results.sort(key=lambda r: r.score, reverse=True)
        return results
