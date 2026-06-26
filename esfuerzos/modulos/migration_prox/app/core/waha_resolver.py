"""
Resuelve a qué Negocio pertenece un mensaje entrante de WAHA.

Modo WAHA_FREE_TIER=True (desarrollo):
  WAHA free tier solo soporta una sesión llamada "default".
  El resolver ignora el campo waha_session del payload y retorna
  el único negocio activo. Si hay cero o más de uno, falla con un
  error claro para evitar enrutamiento silencioso incorrecto.

Modo WAHA_FREE_TIER=False (producción):
  El resolver busca el negocio cuyo waha_session coincide con
  el campo "session" del payload entrante de WAHA.
"""

import logging
from sqlalchemy.orm import Session
from app.models.negocio import Negocio
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def resolve_negocio(waha_session: str, db: Session) -> Negocio | None:
    """
    Retorna el Negocio correspondiente al mensaje entrante, o None si no se puede resolver.

    Args:
        waha_session: valor del campo "session" en el payload de WAHA.
        db: sesión de base de datos.
    """
    if settings.waha_free_tier:
        negocios_activos = db.query(Negocio).filter(Negocio.is_active == True).all()

        if len(negocios_activos) == 0:
            logger.error("WAHA_FREE_TIER=True pero no hay negocios activos en la base de datos.")
            return None

        if len(negocios_activos) > 1:
            logger.error(
                "WAHA_FREE_TIER=True pero hay %d negocios activos. "
                "En free tier solo puede haber uno. Establece WAHA_FREE_TIER=false "
                "para enrutamiento multi-tenant por waha_session.",
                len(negocios_activos),
            )
            return None

        return negocios_activos[0]

    # Modo multi-tenant: enrutamiento por waha_session
    negocio = (
        db.query(Negocio)
        .filter(Negocio.waha_session == waha_session, Negocio.is_active == True)
        .first()
    )
    if not negocio:
        logger.warning("No se encontró negocio activo para waha_session='%s'.", waha_session)
    return negocio
