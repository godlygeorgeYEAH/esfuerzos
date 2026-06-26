import logging
from typing import Optional
import httpx
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


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
