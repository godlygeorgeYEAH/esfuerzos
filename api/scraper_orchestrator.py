"""
scraper_orchestrator.py - Bootstrap and run all scrapers.

Called from main.py lifespan. Supports both legacy BaseScraper (run_poll/run_full)
and new BaseVEScraper (poll_recent/full_sweep) interfaces.
"""

from __future__ import annotations

import asyncio
import logging

from config import get_settings
from scrapers.reconexion import ReconexionScraper
from scrapers.sos_venezuela import SosVenezuelaScraper
from scrapers.venezreporta import VenezReportaScraper
from scrapers.terremotove import TerremotoVEScraper as TerremotoveScraper
from scrapers.google_drive_hospital import GoogleDriveHospitalScraper

logger = logging.getLogger("orchestrator")
settings = get_settings()


def _make_scrapers() -> dict:
    sb_url = settings.supabase_url
    sb_key = settings.supabase_service_role_key

    # BaseScraper subclasses take (supabase_url, supabase_key) in constructor.
    # BaseVEScraper subclasses read SUPABASE_URL/KEY from env.
    scrapers = {
        "reconexion": ReconexionScraper(sb_url, sb_key),
        "sos_venezuela": SosVenezuelaScraper(sb_url, sb_key),
        "venezreporta": VenezReportaScraper(sb_url, sb_key),
        "terremotove": TerremotoveScraper(),
        "google_drive_hospital": GoogleDriveHospitalScraper(sb_url, sb_key),
    }

    # Optional scrapers (require API keys)
    if settings.hospitales_anon_key:
        from scrapers.hospitales_ve import HospitalesVEScraper
        scrapers["hospitales_ve"] = HospitalesVEScraper(sb_url, sb_key)
    if settings.redayuda_anon_key:
        from scrapers.redayuda_ve import RedAyudaVEScraper
        scrapers["redayuda_ve"] = RedAyudaVEScraper()

    return scrapers


async def _run_poll(scraper_name: str, scrapers: dict) -> None:
    scraper = scrapers.get(scraper_name)
    if not scraper:
        return
    try:
        if hasattr(scraper, "run_poll"):
            stats = await scraper.run_poll()
        elif hasattr(scraper, "poll_recent"):
            stats = {"inserted": await scraper.poll_recent()}
        else:
            stats = {}
        logger.info("[orchestrator] %s poll: %s", scraper_name, stats)
    except Exception as exc:
        logger.error("[orchestrator] %s poll error: %s", scraper_name, exc)


async def _run_full(scraper_name: str, scrapers: dict) -> None:
    scraper = scrapers.get(scraper_name)
    if not scraper:
        return
    try:
        if hasattr(scraper, "run_full"):
            stats = await scraper.run_full()
        elif hasattr(scraper, "full_sweep"):
            stats = {"inserted": await scraper.full_sweep()}
        else:
            stats = {}
        logger.info("[orchestrator] %s full: %s", scraper_name, stats)
    except Exception as exc:
        logger.error("[orchestrator] %s full error: %s", scraper_name, exc)


async def _startup_sweep(scrapers: dict) -> None:
    """Run a full sweep of all scrapers at startup."""
    logger.info("Startup sweep: %d scrapers", len(scrapers))
    tasks = [_run_full(name, scrapers) for name in scrapers]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Startup sweep complete.")
