"""
api/search_orchestrator.py -- ExploratorySearchOrchestrator for Reune VE bot.

Fires as an asyncio BackgroundTask on every new WhatsApp report (missing or found).
Queries all registered search sources in parallel, stores leads, sends follow-up.

Usage (in flows.py::create_report, after successful DB insert):
    import asyncio
    from api.search_orchestrator import run_exploratory_search
    from api.scrapers.base import SearchQuery, parse_age_int

    asyncio.create_task(
        run_exploratory_search(
            SearchQuery(
                full_name=data["name"],
                age=parse_age_int(data.get("age")),
                last_seen_location=data.get("location"),
                kind=kind,
                report_id=report_id,
                reporter_phone=phone,
            )
        )
    )

Architecture:
  1. Build all registered sources via BaseSearchSource.build_sources()
  2. Fan out to all sources via asyncio.gather (parallel, NOT sequential)
  3. Each source wraps its own call in asyncio.wait_for(timeout_seconds)
     and returns [] on failure -- graceful degradation
  4. No outer gather timeout: each source self-limits; wrapping gather in
     an outer wait_for would discard partial results from completed sources
     when any slow source triggers the cap. Budget is managed per-source.
  5. Flatten + sort results by score
  6. Write score >= STORE_THRESHOLD to 'external_leads' table
  7. Send WhatsApp follow-up if any result score >= NOTIFY_THRESHOLD

Table: external_leads
  References reunion_reports(id) -- NOT reports(id).
  The bot intake table is 'reunion_reports'. The scraped aggregate is 'reports'.
  These are separate tables. The FK goes to the correct one.

Known gap:
  match_engine.process_new_report (pgvector matching against 'reports') is NOT
  currently called from create_report. Wiring it requires an app_state object
  with a SentenceTransformer model (~500MB RAM) which exceeds the 256MB container
  limit. Until a dedicated embedding sidecar is available, pgvector internal
  matching is not part of the exploratory search path.
  The internal source here uses ILIKE on 'reunion_reports' (bot-submitted data only).
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx

# Import scrapers package to trigger __init_subclass__ registration
import api.scrapers  # noqa: F401

from api.scrapers.base import BaseSearchSource, SearchQuery, SearchResult

logger = logging.getLogger(__name__)

_SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
_SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
_WAHA_URL: str = os.environ.get("WAHA_URL", "http://localhost:3000")
_WAHA_TOKEN: str = os.environ.get("WAHA_API_TOKEN", "")
_WAHA_SESSION: str = os.environ.get("WAHA_SESSION", "reune")

STORE_THRESHOLD: float = 0.40    # minimum score to persist to external_leads
NOTIFY_THRESHOLD: float = 0.60   # minimum score to trigger WhatsApp follow-up
MAX_NOTIFY_RESULTS: int = 5      # max results shown in WhatsApp message

_SOURCE_LABELS: dict[str, str] = {
    "internal_reunion":  "Base interna Reune",
    "hospitales_ve":     "Hospitales Venezuela",
    "red_ayuda_ve":      "Red Ayuda Venezuela",
    "reconexion":        "Reconexion VE",
    "sos_venezuela":     "SOS Venezuela",
    "venezreporta":      "VenezReporta",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _search_one_source(
    source: BaseSearchSource,
    query: SearchQuery,
) -> list[SearchResult]:
    """
    Run search_person on one source with its configured per-source timeout.
    Returns [] on timeout or any exception (graceful degradation).
    """
    try:
        return await asyncio.wait_for(
            source.search_person(query),
            timeout=source.timeout_seconds,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "Source '%s' timed out after %.1fs for query '%s'",
            source.source_name, source.timeout_seconds, query.full_name,
        )
        return []
    except Exception as exc:
        logger.error(
            "Source '%s' raised unexpected error for query '%s': %s",
            source.source_name, query.full_name, exc,
        )
        return []


def _sb_headers() -> dict:
    return {
        "apikey": _SUPABASE_KEY,
        "Authorization": f"Bearer {_SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _waha_headers() -> dict:
    return {"X-Api-Key": _WAHA_TOKEN} if _WAHA_TOKEN else {}


async def _store_leads(
    report_id: str | None,
    results: list[SearchResult],
) -> None:
    """
    Write results with score >= STORE_THRESHOLD to the 'external_leads' table.
    report_id references reunion_reports.id (bot intake table).
    """
    rows = [
        {
            "report_id": report_id,
            "source": r.source,
            "source_url": r.source_url,
            "full_name": r.full_name,
            "age": r.age,
            "location": r.location,
            "detail": r.detail,
            "contact": r.contact,
            "photo_url": r.photo_url,
            "score": r.score,
            "name_similarity": r.name_similarity,
            "kind": r.kind,
            "raw_data": r.raw,
        }
        for r in results
        if r.score >= STORE_THRESHOLD
    ]
    if not rows:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{_SUPABASE_URL}/rest/v1/external_leads",
                headers=_sb_headers(),
                json=rows,
            )
            resp.raise_for_status()
        logger.info(
            "Stored %d external lead(s) for report_id=%s", len(rows), report_id
        )
    except Exception as exc:
        logger.error("_store_leads failed for report_id=%s: %s", report_id, exc)


def _format_whatsapp_message(
    query: SearchQuery,
    strong_leads: list[SearchResult],
) -> str:
    """
    Build a WhatsApp follow-up message for leads with score >= NOTIFY_THRESHOLD.
    Keeps lines short (mobile-readable). No em dashes. Max MAX_NOTIFY_RESULTS shown.

    Sample output:
      Buscamos a Jose Rodriguez en todas las fuentes disponibles.
      Encontramos 2 posible(s) coincidencia(s):

      1. *Jose Rodrigues* | Hospital Perez Carreno | Caracas
         Detalle: 32 anos - La Guaira
         Fuente: Hospitales Venezuela

      IMPORTANTE: Estos resultados no estan confirmados...
    """
    top = strong_leads[:MAX_NOTIFY_RESULTS]
    header = (
        f"Buscamos a {query.full_name} en todas las fuentes disponibles.\n"
        f"Encontramos {len(top)} posible(s) coincidencia(s):\n"
    )

    entries: list[str] = []
    for i, r in enumerate(top, 1):
        lines = [f"{i}. *{r.full_name}*"]
        if r.location:
            lines.append(f"   Ubicacion: {r.location}")
        if r.detail:
            lines.append(f"   Detalle: {r.detail}")
        if r.contact:
            lines.append(f"   Contacto: {r.contact}")
        source_label = _SOURCE_LABELS.get(r.source.split("/")[0], r.source)
        lines.append(f"   Fuente: {source_label}")
        entries.append("\n".join(lines))

    footer = (
        "\nIMPORTANTE: Estos resultados son tentativos y no estan confirmados. "
        "Verifica directamente con el centro de atencion o llama al numero indicado "
        "antes de actuar.\n"
        "Si ya encontraste a tu familiar, responde ENCONTRADO para cerrar este reporte."
    )

    return header + "\n\n".join(entries) + footer


async def _send_whatsapp(phone: str, text: str) -> None:
    """Send follow-up message via WAHA."""
    payload = {"chatId": phone, "text": text, "session": _WAHA_SESSION}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{_WAHA_URL}/api/sendText",
                json=payload,
                headers=_waha_headers(),
            )
            resp.raise_for_status()
        logger.info("Exploratory search follow-up sent to %s", phone)
    except Exception as exc:
        logger.error("_send_whatsapp to %s failed: %s", phone, exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def run_exploratory_search(
    query: SearchQuery,
    app_state: Any = None,  # reserved for future DI; not used currently
) -> list[SearchResult]:
    """
    Main entry point. Call as asyncio.create_task for non-blocking execution.

    Args:
        query:     SearchQuery built from the incoming WhatsApp report data.
        app_state: Reserved for future dependency injection (e.g., model sidecar).
                   Currently unused; env vars are read directly.

    Returns:
        Flattened list of SearchResult with score >= STORE_THRESHOLD.
        Callers that fire this as a background task can ignore the return value.

    Side effects:
        1. Writes to 'external_leads' table (score >= STORE_THRESHOLD)
        2. Sends WhatsApp follow-up to query.reporter_phone if any score >= NOTIFY_THRESHOLD

    Timing:
        Per-source budgets (6-7s each) are enforced via asyncio.wait_for.
        All sources run in parallel via asyncio.gather.
        Total wall-clock time <= max(source.timeout_seconds) + overhead ~= 7-8s.
        This is below the 10s requirement without an outer hard cap that would
        discard partial results from faster sources if a slow one is in flight.
    """
    name = (query.full_name or "").strip()
    if len(name) < 3:
        logger.warning(
            "run_exploratory_search: name '%s' too short, skipping", name
        )
        return []

    sources = BaseSearchSource.build_sources()
    if not sources:
        logger.warning("run_exploratory_search: no search sources registered")
        return []

    logger.info(
        "Exploratory search | name='%s' | %d source(s)",
        name, len(sources),
    )

    # Parallel fan-out. Each task handles its own timeout + exception.
    tasks = [_search_one_source(src, query) for src in sources]
    results_per_source: list[list[SearchResult]] = await asyncio.gather(*tasks)

    # Flatten, sort by score descending
    all_results: list[SearchResult] = []
    for batch in results_per_source:
        all_results.extend(batch)
    all_results.sort(key=lambda r: r.score, reverse=True)

    # Log summary
    source_counts: dict[str, int] = {}
    for r in all_results:
        key = r.source.split("/")[0]
        source_counts[key] = source_counts.get(key, 0) + 1
    logger.info(
        "Exploratory search complete | name='%s' | total=%d | by_source=%s",
        name, len(all_results), source_counts,
    )

    # Persist leads
    await _store_leads(query.report_id, all_results)

    # Notify reporter
    strong_leads = [r for r in all_results if r.score >= NOTIFY_THRESHOLD]
    if strong_leads and query.reporter_phone:
        msg = _format_whatsapp_message(query, strong_leads)
        await _send_whatsapp(query.reporter_phone, msg)
    elif not strong_leads:
        logger.info(
            "No strong leads (>= %.2f) for '%s'; no follow-up sent",
            NOTIFY_THRESHOLD, name,
        )

    return [r for r in all_results if r.score >= STORE_THRESHOLD]
