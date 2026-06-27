import logging
import secrets
import time
from collections import OrderedDict

from fastapi import APIRouter, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from app.config import get_settings
from app.database import get_db
from app.core.waha_resolver import resolve_operacion
from app.bot.orchestrator import Orchestrator

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter()

# Deduplicación de eventos por event.id — TTL 30s, máximo 500 entradas
_SEEN_EVENTS: OrderedDict[str, float] = OrderedDict()
_DEDUP_TTL = 30
_DEDUP_MAX = 500


def _is_duplicate(event_id: str) -> bool:
    if not event_id:
        return False
    now = time.monotonic()
    # Limpiar entradas expiradas
    while _SEEN_EVENTS and next(iter(_SEEN_EVENTS.values())) < now - _DEDUP_TTL:
        _SEEN_EVENTS.popitem(last=False)
    if event_id in _SEEN_EVENTS:
        return True
    if len(_SEEN_EVENTS) >= _DEDUP_MAX:
        _SEEN_EVENTS.popitem(last=False)
    _SEEN_EVENTS[event_id] = now
    return False


@router.post("/waha")
@limiter.limit("60/minute")
async def waha_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Recibe todos los eventos entrantes desde WAHA.

    Resuelve la operación destinataria a partir del campo "session" del payload,
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

    # Dedup por tipo_evento:id_mensaje_WA — evita procesar el mismo mensaje dos
    # veces si WAHA tiene webhooks duplicados, sin bloquear message vs message.any
    # que comparten el mismo WA message ID pero son eventos distintos.
    event_type = payload.get("event", "")
    wa_msg_id = (payload.get("payload") or {}).get("id") or payload.get("id", "")
    dedup_key = f"{event_type}:{wa_msg_id}" if wa_msg_id else ""
    if _is_duplicate(dedup_key):
        return {"status": "ignored", "reason": "duplicate_event_id"}

    session_name = payload.get("session", "default")
    operacion = resolve_operacion(session_name, db)

    if operacion is None:
        logger.warning(
            "Mensaje descartado: no se pudo resolver operación para session='%s'.", session_name
        )
        return {"status": "ignored"}

    event = payload.get("event", "unknown")
    payload_data = payload.get("payload", {})
    is_from_me = payload_data.get("fromMe", False)
    body_preview = (payload_data.get("body") or "").strip()

    logger.debug(
        "WAHA webhook recibido | operacion=%s (id=%d) | session=%s | event=%s | from=%s | fromMe=%s | body=%s",
        operacion.slug,
        operacion.id,
        session_name,
        event,
        payload_data.get("from", "?"),
        is_from_me,
        body_preview[:60] or "[media]",
    )

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

    # Respuesta de lista interactiva — WAHA NOWEB no expone rowId en el top-level del payload.
    # El body contiene el título de la fila ("Hospital o refugio") en lugar del rowId ("3").
    # Extraemos el rowId con tres estrategias en orden de prioridad:
    #   1. _data.listResponseMessage.singleSelectReply.selectedRowId  (Baileys directo)
    #   2. _data.message.listResponseMessage.singleSelectReply.selectedRowId  (envuelto)
    #   3. Matching del título en replyTo._data.listMessage.sections  (fallback robusto)
    _row_id: str | None = None
    _raw_data = payload_data.get("_data") or {}

    # Estrategia 1 y 2 — listResponseMessage en _data
    _list_resp_msg = _raw_data.get("listResponseMessage") or (
        (_raw_data.get("message") or {}).get("listResponseMessage")
    )
    if _list_resp_msg:
        _row_id = ((_list_resp_msg.get("singleSelectReply") or {}).get("selectedRowId")
                   or _list_resp_msg.get("selectedRowId"))

    # Estrategia 3 — match por título en la lista original del replyTo
    if not _row_id and message_text:
        _reply_data = (payload_data.get("replyTo") or {}).get("_data") or {}
        _list_msg = _reply_data.get("listMessage") or {}
        for _section in (_list_msg.get("sections") or []):
            for _row in (_section.get("rows") or []):
                if _row.get("title") == message_text:
                    _row_id = str(_row.get("rowId", ""))
                    break
            if _row_id:
                break

    if _row_id:
        logger.info("List response detectado → rowId=%r (body era %r)", _row_id, message_text)
        message_text = _row_id.strip()

    # Extraer media (comprobante de pago u otro archivo adjunto)
    media_url: str | None = None
    if payload_data.get("hasMedia") or payload_data.get("mediaUrl"):
        media_url = payload_data.get("mediaUrl") or payload_data.get("media", {}).get("url")

    # Ubicación GPS — tiene prioridad sobre el body (que puede ser thumbnail JPEG)
    if payload_data.get("location"):
        loc = payload_data["location"]
        lat = loc.get("latitude", "")
        lng = loc.get("longitude", "")
        desc = (loc.get("description") or loc.get("name") or "").strip()
        message_text = f"{desc} (GPS: {lat}, {lng})" if desc else f"GPS: {lat}, {lng}"
        media_url = None  # descartar el thumbnail, no es una foto útil
        logger.info("Ubicación GPS recibida → '%s'", message_text)

    # Ignorar si no hay texto ni media
    if not message_text and not media_url:
        logger.debug("DESCARTADO | reason=empty_body | from=%s", payload_data.get("from", "?"))
        return {"status": "ignored", "reason": "empty_body"}

    # Extraer número de teléfono del cliente (formato: 584121234567@c.us → 584121234567)
    chat_id = payload_data.get("from", "")

    # Ignorar Estados de WhatsApp (Stories) — llegan como status@broadcast o *@broadcast
    if chat_id == "status@broadcast" or chat_id.endswith("@broadcast"):
        logger.debug("DESCARTADO | reason=status_broadcast | chat_id=%s", chat_id)
        return {"status": "ignored", "reason": "status_broadcast"}

    client_phone = chat_id.split("@")[0] if "@" in chat_id else chat_id

    if not client_phone:
        logger.warning("DESCARTADO | reason=no_phone | from=%s", payload_data.get("from", "?"))
        return {"status": "ignored", "reason": "no_phone"}

    # @lid es un identificador de dispositivo WAHA distinto al número de teléfono.
    # Lo resolvemos a E.164 para que el lookup de conversaciones funcione correctamente.
    if chat_id.endswith("@lid"):
        from app.services.waha import resolve_lid_phone
        resolved = await resolve_lid_phone(chat_id, session_name)
        if resolved:
            client_phone = resolved

    orchestrator = Orchestrator(db)
    response, should_send = await orchestrator.process_message(
        operacion_id=operacion.id,
        client_phone=client_phone,
        message_text=message_text,
        media_url=media_url,
        waha_chat_id=chat_id or None,
    )

    sent = False
    if should_send:
        from app.services.waha import send_list as waha_send_list, send_message as waha_send
        list_payload = orchestrator._pending_list
        pre_text = orchestrator._pre_list_message

        if list_payload:
            # Texto informativo previo (solo flujo hospital) — se envía antes del menú
            if pre_text:
                await waha_send(phone=chat_id, message=pre_text, session=session_name)
            sent = await waha_send_list(chat_id=chat_id, session=session_name, message=list_payload)
            if not sent and response and not pre_text:
                # fallback a texto plano si sendList falla y no se envió el pre_text ya
                sent = bool(await waha_send(phone=chat_id, message=response, session=session_name))
            logger.info("WAHA sendList → chat_id=%s session=%s result=%s", chat_id, session_name, sent)
        elif response:
            sent = bool(await waha_send(phone=chat_id, message=response, session=session_name))
            logger.info("WAHA sendText → chat_id=%s session=%s result=%s", chat_id, session_name, sent)
        else:
            logger.info("WAHA send omitido → should_send=%s response_len=%d", should_send, len(response or ""))

    return {"status": "processed", "sent": sent}
