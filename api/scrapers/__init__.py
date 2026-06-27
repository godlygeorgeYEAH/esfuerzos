"""
api/scrapers/__init__.py

Importing this package registers all BaseSearchSource subclasses
via __init_subclass__. The orchestrator imports this module once at startup
and then calls BaseSearchSource.build_sources() to get all active sources.

To add a new search source:
  1. Create api/scrapers/my_source.py
  2. Subclass BaseSearchSource; set source_name; implement search_person()
  3. Add the import below
  That is all. The orchestrator discovers it automatically.

Import failures are caught individually so one broken source
does not block the others.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_SOURCES = [
    ("api.scrapers.internal_source", "InternalReunionSource"),
    ("api.scrapers.hospitales_ve",   "HospitalesVESource"),
    # redayuda_ve: full scraper + search (BaseVEScraper + BaseSearchSource, source_name="redayuda_ve")
    # Canonical implementation. red_ayuda.py removed (duplicate, hardcoded key).
    ("api.scrapers.redayuda_ve",     "RedAyudaVEScraper"),
    ("api.scrapers.reconexion",      "ReconexionScraper"),
    ("api.scrapers.sos_venezuela",   "SOSVenezuelaScraper"),
    ("api.scrapers.venezreporta",    "VenezReportaScraper"),
]

for _module, _class in _SOURCES:
    try:
        import importlib
        importlib.import_module(_module)
    except Exception as _exc:
        logger.warning("Could not import %s.%s: %s", _module, _class, _exc)
