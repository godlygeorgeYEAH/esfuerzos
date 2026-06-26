import logging
from sqlalchemy.orm import Session

from app.models.conductor import Conductor
from app.models.orden import Orden, EstadoOrden
from app.models.notificacion import TipoNotificacion
from app.core.notificaciones import crear_notificacion

logger = logging.getLogger(__name__)


def es_respuesta_conductor(message_text: str) -> bool:
    """Retorna True si el mensaje contiene 'acepto', 'rechazo' o 'entregado' (con o sin '/' inicial)."""
    texto = message_text.lstrip("/").lower()
    return "acepto" in texto or "rechazo" in texto or "entregado" in texto


def _parsear_respuestas(message_text: str) -> dict[int, str]:
    """
    Parsea el mensaje del conductor y retorna un dict {orden_id: accion}.

    Ejemplos válidos:
      "acepto 42 55 rechazo 49"  → {42: "aceptada", 55: "aceptada", 49: "rechazada"}
      "rechazo 77"               → {77: "rechazada"}
      "entregado 42"             → {42: "entregado_conductor"}
    """
    tokens = message_text.lstrip("/").lower().strip().split()
    resultados: dict[int, str] = {}
    accion_activa: str | None = None

    for token in tokens:
        if token == "acepto":
            accion_activa = "aceptada"
        elif token == "rechazo":
            accion_activa = "rechazada"
        elif token == "entregado":
            accion_activa = "entregado_conductor"
        elif token.isdigit() and accion_activa:
            resultados[int(token)] = accion_activa

    return resultados


async def procesar_respuesta_conductor(
    db: Session,
    conductor: Conductor,
    message_text: str,
    session: str,
) -> str:
    """
    Procesa la respuesta del conductor (acepto/rechazo de órdenes).
    Actualiza estado_conductor en cada orden válida.
    Retorna el mensaje de respuesta a enviar al conductor.
    """
    from app.services.waha import send_message as waha_send

    respuestas = _parsear_respuestas(message_text)
    if not respuestas:
        return ""

    errores: list[str] = []
    procesadas: list[str] = []

    for orden_id, accion in respuestas.items():
        orden: Orden | None = db.query(Orden).filter(
            Orden.id == orden_id,
            Orden.conductor_id == conductor.id,
            Orden.negocio_id == conductor.negocio_id,
        ).first()

        if not orden:
            errores.append(f"La orden #{orden_id} no existe o no ha sido asignada a ti.")
            continue

        if accion == "entregado_conductor":
            if orden.estado != EstadoOrden.EN_CAMINO:
                errores.append(f"La orden #{orden_id} no está en camino (estado actual: {orden.estado.value}).")
                continue
            orden.estado = EstadoOrden.ENTREGADA
            crear_notificacion(
                db,
                negocio_id=conductor.negocio_id,
                tipo=TipoNotificacion.ORDEN_ENTREGADA,
                titulo=f"Conductor {conductor.nombre} marcó la orden #{orden_id} como entregada",
                ruta_destino=f"/dashboard/ordenes/{orden_id}",
                referencia_id=orden_id,
            )
            procesadas.append(f"#{orden_id} entregada")
            logger.info(
                "Conductor %d (%s) → orden #%d → entregada",
                conductor.id, conductor.nombre, orden_id,
            )
            continue

        orden.estado_conductor = accion

        tipo = TipoNotificacion.CONDUCTOR_ACEPTO if accion == "aceptada" else TipoNotificacion.CONDUCTOR_RECHAZO
        titulo = (
            f"Conductor {conductor.nombre} aceptó la orden #{orden_id}"
            if accion == "aceptada"
            else f"Conductor {conductor.nombre} rechazó la orden #{orden_id}"
        )
        crear_notificacion(
            db,
            negocio_id=conductor.negocio_id,
            tipo=tipo,
            titulo=titulo,
            ruta_destino=f"/dashboard/ordenes/{orden_id}",
            referencia_id=orden_id,
        )

        procesadas.append(f"#{orden_id} {'aceptada' if accion == 'aceptada' else 'rechazada'}")
        logger.info(
            "Conductor %d (%s) → orden #%d → %s",
            conductor.id, conductor.nombre, orden_id, accion,
        )

    try:
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error("Error al guardar respuestas del conductor %d: %s", conductor.id, e)
        return ""

    # Construir respuesta al conductor
    partes: list[str] = []
    if procesadas:
        partes.append("✅ Procesado: " + ", ".join(procesadas))
    if errores:
        partes.append("\n".join(errores))

    return "\n".join(partes)
