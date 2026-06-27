"""
Resuelve a qué Operacion pertenece un mensaje entrante de WAHA.

Modo WAHA_FREE_TIER=True (desarrollo):
  WAHA free tier solo soporta una sesión llamada "default".
  El resolver ignora el campo waha_session del payload y retorna
  la única operación activa. Si hay cero o más de una, falla con un
  error claro para evitar enrutamiento silencioso incorrecto.

Modo WAHA_FREE_TIER=False (producción):
  El resolver busca la operación cuyo waha_session coincide con
  el campo "session" del payload entrante de WAHA.
"""

import logging
from sqlalchemy.orm import Session
from app.models.negocio import Operacion
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


def resolve_operacion(waha_session: str, db: Session) -> Operacion | None:
    """
    Retorna la Operacion correspondiente al mensaje entrante, o None si no se puede resolver.

    Args:
        waha_session: valor del campo "session" en el payload de WAHA.
        db: sesión de base de datos.
    """
    if settings.waha_free_tier:
        operaciones_activas = db.query(Operacion).filter(Operacion.is_active == True).all()

        if len(operaciones_activas) == 0:
            logger.error("WAHA_FREE_TIER=True pero no hay operaciones activas en la base de datos.")
            return None

        if len(operaciones_activas) > 1:
            logger.error(
                "WAHA_FREE_TIER=True pero hay %d operaciones activas. "
                "En free tier solo puede haber una. Establece WAHA_FREE_TIER=false "
                "para enrutamiento multi-tenant por waha_session.",
                len(operaciones_activas),
            )
            return None

        return operaciones_activas[0]

    # Modo multi-tenant: enrutamiento por waha_session
    operacion = (
        db.query(Operacion)
        .filter(Operacion.waha_session == waha_session, Operacion.is_active == True)
        .first()
    )
    if not operacion:
        logger.warning("No se encontró operación activa para waha_session='%s'.", waha_session)
    return operacion
