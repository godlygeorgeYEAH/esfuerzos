import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.core.waha_resolver import resolve_negocio
from app.bot.orchestrator import Orchestrator

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter()


@router.post("/waha")
@limiter.limit("60/minute")
async def waha_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Recibe todos los eventos entrantes desde WAHA.

    Resuelve el negocio destinatario a partir del campo "session" del payload,
    respetando el modo WAHA_FREE_TIER para desarrollo con una sola sesión.
    Solo procesa eventos de tipo "message" con texto.
    """
    _settings = get_settings()
    if _settings.waha_webhook_secret:
        token = request.headers.get("X-WAHA-Token", "")
        if not secrets.compare_digest(token, _settings.waha_webhook_secret):
            logger.warning("Webhook rechazado: token inválido desde %s", request.client.host)
            raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()

    session_name = payload.get("session", "default")
    negocio = resolve_negocio(session_name, db)

    if negocio is None:
        logger.warning(
            "Mensaje descartado: no se pudo resolver negocio para session='%s'.", session_name
        )
        return {"status": "ignored"}

    event = payload.get("event", "unknown")
    logger.info(
        "WAHA webhook recibido | negocio=%s (id=%d) | session=%s | event=%s",
        negocio.slug,
        negocio.id,
        session_name,
        event,
    )

    payload_data = payload.get("payload", {})
    is_from_me = payload_data.get("fromMe", False)
    body_preview = (payload_data.get("body") or "").strip()

    # WAHA emite dos eventos por cada mensaje entrante: "message" (terceros) y
    # "message.any" (todos, incluido fromMe). Procesar ambos causaría respuestas dobles.
    # Regla: solo se procesa "message.any" cuando es fromMe + modo testing activo.
    # Los mensajes de clientes reales entran únicamente por "message".
    if event == "message.any":
        from app.config import get_settings as _get_settings
        _settings = _get_settings()
        if not (is_from_me and _settings.bot_self_message_testing and body_preview.startswith("/")):
            return {"status": "ignored", "reason": "message.any_not_testing"}
    elif event != "message":
        return {"status": "ignored", "reason": f"event={event}"}

    # Ignorar mensajes propios que no sean de testing
    if is_from_me:
        from app.config import get_settings as _get_settings
        _settings = _get_settings()
        if not (_settings.bot_self_message_testing and body_preview.startswith("/")):
            return {"status": "ignored", "reason": "fromMe"}

    message_text = (payload_data.get("body") or "").strip()

    # Extraer media (comprobante de pago u otro archivo adjunto)
    media_url: str | None = None
    if payload_data.get("hasMedia") or payload_data.get("mediaUrl"):
        media_url = payload_data.get("mediaUrl") or payload_data.get("media", {}).get("url")

    # Ignorar si no hay texto ni media
    if not message_text and not media_url:
        return {"status": "ignored", "reason": "empty_body"}

    # Extraer número de teléfono del cliente (formato: 584121234567@c.us → 584121234567)
    chat_id = payload_data.get("from", "")
    client_phone = chat_id.split("@")[0] if "@" in chat_id else chat_id

    if not client_phone:
        return {"status": "ignored", "reason": "no_phone"}

    # @lid es un identificador de dispositivo WAHA distinto al número de teléfono.
    # Lo resolvemos a E.164 para que el lookup de conversaciones funcione correctamente.
    if chat_id.endswith("@lid"):
        from app.services.waha import resolve_lid_phone
        resolved = await resolve_lid_phone(chat_id, session_name)
        if resolved:
            client_phone = resolved

    # Verificar si el remitente es un conductor activo del negocio
    from app.models.conductor import Conductor
    from app.core.conductores import es_respuesta_conductor, procesar_respuesta_conductor
    from app.core.phone import normalize_phone

    conductor = db.query(Conductor).filter(
        Conductor.telefono == normalize_phone(client_phone),
        Conductor.negocio_id == negocio.id,
        Conductor.is_active == True,
    ).first()

    if conductor and message_text and es_respuesta_conductor(message_text):
        respuesta = await procesar_respuesta_conductor(db, conductor, message_text, session_name)
        if respuesta:
            from app.services.waha import send_message as waha_send
            await waha_send(phone=chat_id, message=respuesta, session=session_name)
        return {"status": "processed_conductor"}

    from app.core.clientes import get_or_create_cliente
    get_or_create_cliente(db, negocio.id, client_phone)
    db.commit()

    orchestrator = Orchestrator(db)
    response, should_send = await orchestrator.process_message(
        negocio_id=negocio.id,
        client_phone=client_phone,
        message_text=message_text,
        media_url=media_url,
        waha_chat_id=chat_id or None,
    )

    if should_send and response:
        from app.services.waha import send_message as waha_send
        sent = await waha_send(phone=chat_id, message=response, session=session_name)
        logger.warning("WAHA send → chat_id=%s session=%s result=%s", chat_id, session_name, sent)
    else:
        logger.warning("WAHA send omitido → should_send=%s response_len=%d", should_send, len(response or ""))

    return {"status": "processed", "sent": should_send and bool(response)}
