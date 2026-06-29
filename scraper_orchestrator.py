"""
scraper_orchestrator.py - Bootstrap and run all scrapers.

Called from main.py lifespan. Supports both legacy BaseScraper (run_poll/run_full)
and new BaseVEScraper (poll_recent/full_sweep) interfaces.
"""

from __future__ import annotations

import asyncio
import logging

from config import get_settings
from scrapers.sos_venezuela import SosVenezuelaScraper
from scrapers.venezreporta import VenezReportaScraper
from scrapers.terremotove import TerremotoVEScraper as TerremotoveScraper
from scrapers.google_drive_hospital import GoogleDriveHospitalScraper
from scrapers.red_solidaria_venezuela import RedSolidariaVenezuelaScraper
from scrapers.localizados_venezuela import LocalizadosVenezuelaScraper
from scrapers.venezuela_te_busca import VenezuelaTeBuscaScraper
from scrapers.sos_laguaira import SosLaGuairaScraper
from scrapers.pacientes_terremoto import PacientesTerremotoVZLAScraper
from scrapers.hospital_consolidado import HospitalConsolidadoScraper
from scrapers.desaparecidos_venezuela import DesaparecidosVenezuelaScraper
from scrapers.localizave import LocalizaveScraper
from scrapers.tuayudave import TuAyudaVEScraper

logger = logging.getLogger("orchestrator")
settings = get_settings()


def _make_scrapers() -> dict:
    sb_url = settings.supabase_url
    sb_key = settings.supabase_service_role_key

    # BaseScraper subclasses take (supabase_url, supabase_key) in constructor.
    # BaseVEScraper subclasses read SUPABASE_URL/KEY from env.
    scrapers = {
        "sos_venezuela": SosVenezuelaScraper(sb_url, sb_key),
        "venezreporta": VenezReportaScraper(sb_url, sb_key),
        "terremotove": TerremotoveScraper(),
        "google_drive_hospital": GoogleDriveHospitalScraper(sb_url, sb_key),
        "red_solidaria_venezuela": RedSolidariaVenezuelaScraper(),
        "localizados_venezuela": LocalizadosVenezuelaScraper(sb_url, sb_key),
        "venezuela_te_busca": VenezuelaTeBuscaScraper(sb_url, sb_key),
        "sos_laguaira": SosLaGuairaScraper(sb_url, sb_key),
        "pacientes_terremoto": PacientesTerremotoVZLAScraper(),
        "tuayudave": TuAyudaVEScraper(sb_url, sb_key),
        "hospital_consolidado": HospitalConsolidadoScraper(),
        "desaparecidos_venezuela": DesaparecidosVenezuelaScraper(),
        "localizave": LocalizaveScraper(),
    }

    # Reconexión integrator API (replaces the old 403'd reconexion scraper). Uses
    # curl_cffi to pass CloudFront's TLS fingerprinting. Enabled only with a key.
    if settings.reconexion_api_key:
        from scrapers.reconexion_api import ReconexionAPIScraper
        scrapers["reconexion"] = ReconexionAPIScraper()

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
