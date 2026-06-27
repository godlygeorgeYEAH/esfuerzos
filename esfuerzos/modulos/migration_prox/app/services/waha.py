import asyncio
import logging
from typing import Optional
import httpx
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def ensure_default_session(retries: int = 15, delay: float = 2.0) -> bool:
    """Crea/verifica la sesión WAHA con el webhook correcto al arrancar.

    Reintenta hasta `retries` veces con `delay` segundos entre intentos —
    WAHA puede tardar en estar listo tras `docker compose up`.
    """
    session_name = settings.waha_session
    webhook_url = settings.waha_webhook_url
    sessions_url = f"{settings.waha_url}/api/sessions"
    session_url = f"{settings.waha_url}/api/sessions/{session_name}"

    webhook_payload = {
        "url": webhook_url,
        "events": ["message", "message.any"],
    }

    for attempt in range(1, retries + 1):
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                # Intentar leer la sesión existente
                resp = await client.get(session_url, headers=_headers())

                if resp.status_code == 200:
                    data = resp.json()
                    existing_webhooks = (data.get("config") or {}).get("webhooks") or []
                    expected_events = set(webhook_payload["events"])
                    already_correct = any(
                        w.get("url") == webhook_url and set(w.get("events") or []) >= expected_events
                        for w in existing_webhooks
                    )
                    if already_correct:
                        logger.info("WAHA session '%s' already configured correctly.", session_name)
                        return True
                    # Sesión existe pero webhook incorrecto — actualizar vía PATCH
                    patch_resp = await client.patch(
                        session_url,
                        json={"config": {"webhooks": [webhook_payload]}},
                        headers=_headers(),
                    )
                    if patch_resp.status_code in (200, 201):
                        logger.info("WAHA session '%s' webhook updated to %s.", session_name, webhook_url)
                        return True
                    logger.warning("WAHA PATCH session returned %s.", patch_resp.status_code)

                elif resp.status_code == 404:
                    # Sesión no existe — crearla
                    create_resp = await client.post(
                        sessions_url,
                        json={
                            "name": session_name,
                            "config": {"webhooks": [webhook_payload]},
                            "start": True,
                        },
                        headers=_headers(),
                    )
                    if create_resp.status_code in (200, 201):
                        logger.info("WAHA session '%s' created with webhook %s.", session_name, webhook_url)
                        return True
                    logger.warning("WAHA POST session returned %s: %s", create_resp.status_code, create_resp.text)

                else:
                    logger.warning("WAHA session check returned %s (attempt %d/%d).", resp.status_code, attempt, retries)

        except httpx.HTTPError as e:
            logger.warning("WAHA not ready yet (attempt %d/%d): %s", attempt, retries, e)

        if attempt < retries:
            await asyncio.sleep(delay)

    logger.error("Could not configure WAHA session '%s' after %d attempts.", session_name, retries)
    return False


def _headers() -> dict:
    headers = {"Content-Type": "application/json"}
    if settings.waha_api_key:
        headers["X-Api-Key"] = settings.waha_api_key
    return headers


async def send_message(phone: str, message: str, session: str = "default") -> Optional[str]:
    """Envía un mensaje de texto vía WAHA.

    `phone` puede ser un número puro (se agrega @c.us) o un chatId completo
    como '24700877054119@lid' o '584121234567@c.us' (se usa tal cual).

    Retorna el chatId confirmado por WAHA (puede ser @lid o @c.us),
    o None si el envío falló. Usar verdad/falsedad para verificar éxito.
    """
    url = f"{settings.waha_url}/api/sendText"
    chat_id = phone if "@" in phone else f"{phone.lstrip('+')}@c.us"
    body = {
        "chatId": chat_id,
        "text": message,
        "session": session,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            response = await client.post(url, json=body, headers=_headers())
            response.raise_for_status()
            data = response.json()
            confirmed_chat_id = data.get("chatId") or chat_id
            logger.info("Message sent to %s (chatId=%s)", phone, confirmed_chat_id)
            return confirmed_chat_id
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to send message to %s: %s | response body: %s",
                phone, e, e.response.text,
            )
            return None
        except httpx.HTTPError as e:
            logger.error("Failed to send message to %s: %s", phone, e)
            return None


async def resolve_lid_phone(lid_chat_id: str, session: str = "default") -> Optional[str]:
    """Resuelve un chatId @lid al número de teléfono E.164 usando la API de contactos de WAHA.

    Retorna el número (ej: '584244107121') o None si falla.
    Solo tiene sentido llamar esto cuando lid_chat_id termina en '@lid'.
    """
    url = f"{settings.waha_url}/api/contacts"
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            response = await client.get(url, params={"contactId": lid_chat_id, "session": session}, headers=_headers())
            response.raise_for_status()
            data = response.json()
            number = data.get("number")
            if number:
                logger.info("Resolved @lid %s → %s", lid_chat_id, number)
            return number
        except Exception as e:
            logger.warning("Could not resolve @lid %s: %s", lid_chat_id, e)
            return None


async def send_location(
    phone: str,
    lat: float,
    lng: float,
    title: str = "",
    session: str = "default",
) -> bool:
    """Envía una ubicación nativa vía WAHA. El conductor la abre directamente en su app de mapas."""
    url = f"{settings.waha_url}/api/sendLocation"
    chat_id = phone if "@" in phone else f"{phone.lstrip('+')}@c.us"
    body = {
        "chatId": chat_id,
        "latitude": lat,
        "longitude": lng,
        "title": title,
        "session": session,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            response = await client.post(url, json=body, headers=_headers())
            response.raise_for_status()
            logger.info("Location sent to %s", phone)
            return True
        except httpx.HTTPError as e:
            logger.error("Failed to send location to %s: %s", phone, e)
            return False


async def send_match_notification(
    phone: str,
    missing_name: str,
    found_location: str,
    found_state: str = "unknown",
    coordinator_contact: str = "",
    session: str = "default",
) -> bool:
    """Notifica al reportero (familiar) que se encontró una posible coincidencia.

    La confirmación siempre requiere verificación humana — el mensaje lo deja
    explícito para evitar falsas esperanzas.
    """
    state_label = {
        "alive": "con vida",
        "injured": "con heridas",
        "deceased": "fallecida",
    }.get(found_state, "en estado desconocido")

    contact_line = f"\n📞 Contacto de verificación: {coordinator_contact}" if coordinator_contact else ""

    message = (
        f"🔔 *Posible coincidencia para {missing_name}*\n\n"
        f"Encontramos a una persona {state_label} en:\n"
        f"📍 {found_location}\n"
        f"{contact_line}\n\n"
        "⚠️ Esta es una coincidencia *preliminar*. "
        "Un coordinador deberá verificarla antes de confirmar.\n\n"
        "Responde *CONFIRMAR* si deseas que un coordinador te contacte."
    )
    result = await send_message(phone, message, session=session)
    if result:
        logger.info("Match notification sent to %s for '%s'", phone, missing_name)
    return bool(result)


async def send_image(phone: str, image_url: str, caption: str = "", session: str = "default") -> bool:
    """Envía una imagen vía WAHA."""
    url = f"{settings.waha_url}/api/sendImage"
    body = {
        "chatId": f"{phone.lstrip('+')}@c.us",
        "file": {"url": image_url},
        "caption": caption,
        "session": session,
    }
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            response = await client.post(url, json=body, headers=_headers())
            response.raise_for_status()
            logger.info("Image sent to %s", phone)
            return True
        except httpx.HTTPError as e:
            logger.error("Failed to send image to %s: %s", phone, e)
            return False
