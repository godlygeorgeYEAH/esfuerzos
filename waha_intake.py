"""
waha_intake.py - WAHA WhatsApp webhook handler for Reune VE.

Receives WAHA `message` events, uses Groq (llama-3.3-70b) to extract
structured report data from the conversation, upserts to Supabase `reports`,
and triggers the face pipeline for any attached photos.

Conversation state is kept in-memory (keyed by phone number).
Resets on container restart — persistent state can be added later via
a `waha_sessions` Supabase table.

WAHA webhook payload shape (event=message):
{
  "event": "message",
  "session": "default",
  "payload": {
    "id": "msg_id",
    "timestamp": 1234567890,
    "from": "58XXXXXXXXX@c.us",
    "body": "text",
    "fromMe": false,
    "hasMedia": false,
    "mediaUrl": null
  }
}

Security: no sign verification by default (WAHA free tier). Add
WAHA_WEBHOOK_SECRET to enable X-Waha-Signature validation if needed.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import uuid
from collections import defaultdict, deque
from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request

from config import get_settings
from face_pipeline import process_photo_for_report

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()

# In-memory conversation state: phone -> deque of message dicts
_conv_state: dict[str, deque] = defaultdict(lambda: deque(maxlen=20))

# Groq / LLM client config
_LLM_URL = f"{settings.llm_base_url.rstrip('/')}/chat/completions"
_LLM_HEADERS = {
    "Authorization": f"Bearer {settings.llm_api_key}",
    "Content-Type": "application/json",
}

_SYSTEM_PROMPT = """Eres el asistente de Reune VE, un sistema para reunir familias venezolanas separadas durante emergencias.

Tu rol: recolectar información estructurada sobre personas desaparecidas o encontradas.

REGLAS ABSOLUTAS:
- NUNCA digas que alguien está muerto o falleció. Si hay duda, usa "estado desconocido".
- NUNCA confirmes una coincidencia. Usa siempre "posible coincidencia, en verificación".
- Habla en español venezolano, tuteo, tono cálido pero directo.
- Respuestas cortas (máx 3 líneas en WhatsApp).

Cuando el usuario reporte una persona, extrae estos campos:
  kind: "missing" (busca a alguien) | "found" (encontró a alguien)
  name: nombre completo
  age: edad aproximada (número o rango)
  location: último lugar visto / lugar donde fue encontrado
  description: marcas, ropa, características
  contact: teléfono o forma de contacto del reportante

Si falta información, pídela de forma natural. Cuando tengas al menos `kind` y `name`, confirma el reporte.

Responde SIEMPRE en este JSON (no incluyas nada más):
{
  "reply": "<texto para WhatsApp, máx 200 chars>",
  "extracted": {
    "kind": null,
    "name": null,
    "age": null,
    "location": null,
    "description": null,
    "contact": null,
    "report_ready": false
  }
}

`report_ready` = true cuando tienes kind + name confirmados por el usuario."""


def _sb_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }


async def _llm_extract(phone: str, new_message: str) -> dict:
    """Call Groq to extract report data and generate reply."""
    history = list(_conv_state[phone])
    messages = [{"role": "system", "content": _SYSTEM_PROMPT}]
    messages.extend(history)
    messages.append({"role": "user", "content": new_message})

    try:
        async with httpx.AsyncClient(timeout=15) as cl:
            resp = await cl.post(
                _LLM_URL,
                headers=_LLM_HEADERS,
                json={
                    "model": settings.llm_model,
                    "messages": messages,
                    "temperature": 0.3,
                    "max_tokens": 400,
                    "response_format": {"type": "json_object"},
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return json.loads(content)
    except Exception as exc:
        logger.error("LLM call failed for phone %s: %s", phone, exc)
        return {
            "reply": "Hubo un problema procesando tu mensaje. Intenta de nuevo en un momento.",
            "extracted": {"report_ready": False},
        }


async def _upsert_report(phone: str, data: dict, conv_key: str) -> str | None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    row = {
        "source": "waha_whatsapp",
        "source_url": f"waha:{conv_key}",
        "kind": data.get("kind") or "missing",
        "name": data.get("name", ""),
        "age": str(data.get("age", "")) if data.get("age") else None,
        "location": data.get("location"),
        "description": data.get("description"),
        "phone": phone,
        "raw_data": data,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            resp = await cl.post(
                f"{sb}/rest/v1/reports",
                headers=_sb_headers(key),
                json=row,
            )
            if resp.status_code in (200, 201):
                rows = resp.json()
                return rows[0]["id"] if rows else None
            logger.error("upsert_report %d: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.error("upsert_report exception: %s", exc)
    return None


async def _waha_send(phone: str, text: str) -> None:
    waha = settings.waha_url.rstrip("/")
    chat_id = phone if "@" in phone else f"{phone}@c.us"
    payload = {
        "chatId": chat_id,
        "text": text,
        "session": settings.waha_session,
    }
    headers = {}
    if settings.waha_api_key:
        headers["X-Api-Key"] = settings.waha_api_key
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            resp = await cl.post(f"{waha}/api/sendText", json=payload, headers=headers)
            if resp.status_code not in (200, 201):
                logger.warning("waha_send %d: %s", resp.status_code, resp.text[:100])
    except Exception as exc:
        logger.error("waha_send to %s failed: %s", phone, exc)


async def _handle_message(payload: dict, app: Any) -> None:
    phone = payload.get("from", "")
    body = (payload.get("body") or "").strip()
    has_media = payload.get("hasMedia", False)
    media_url = payload.get("mediaUrl") or ""
    from_me = payload.get("fromMe", False)

    if from_me or not phone:
        return

    logger.info("WAHA message from %s: has_media=%s body=%s", phone, has_media, body[:60])

    # Track message in conversation history
    if body:
        _conv_state[phone].append({"role": "user", "content": body})

    # If photo received: store and trigger face pipeline
    if has_media and media_url and media_url.startswith("https://"):
        _conv_state[phone].append({"role": "user", "content": f"[envio una foto: {media_url}]"})
        # Try to find an existing pending report for this phone
        sb = settings.supabase_url.rstrip("/")
        sb_key = settings.supabase_service_role_key
        try:
            async with httpx.AsyncClient(timeout=8) as cl:
                r = await cl.get(
                    f"{sb}/rest/v1/reports",
                    headers={
                        "apikey": sb_key,
                        "Authorization": f"Bearer {sb_key}",
                    },
                    params={"phone": f"eq.{phone}", "order": "created_at.desc", "limit": "1"},
                )
                rows = r.json() if r.status_code == 200 else []
                if rows:
                    report_id = rows[0]["id"]
                    # Upsert photo reference
                    await cl.post(
                        f"{sb}/rest/v1/photos",
                        headers={
                            "apikey": sb_key,
                            "Authorization": f"Bearer {sb_key}",
                            "Content-Type": "application/json",
                            "Prefer": "resolution=ignore-duplicates,return=minimal",
                        },
                        json={"id": str(uuid.uuid4()), "report_id": report_id, "storage_path": media_url},
                    )
                    # Trigger face pipeline in background
                    match_id = await process_photo_for_report(report_id, media_url, app)
                    if match_id:
                        await _waha_send(
                            phone,
                            "Revisamos la foto. Hay una posible coincidencia, en verificacion. "
                            "El equipo de Reune VE te contactara para confirmar."
                        )
                        return
        except Exception as exc:
            logger.error("Photo handling error: %s", exc)

        await _waha_send(phone, "Foto recibida. La procesaremos junto a tu reporte.")
        if not body:
            return

    if not body:
        return

    # LLM extraction
    result = await _llm_extract(phone, body)
    reply = result.get("reply", "")
    extracted = result.get("extracted", {})

    # Update conversation state with assistant reply
    if reply:
        _conv_state[phone].append({"role": "assistant", "content": reply})

    # Persist report when enough data collected
    if extracted.get("report_ready") and extracted.get("name"):
        conv_key = hashlib.md5(phone.encode()).hexdigest()[:12]
        report_id = await _upsert_report(phone, extracted, conv_key)
        if report_id:
            logger.info("Report upserted from WAHA: %s (phone=%s)", report_id, phone)
        else:
            logger.error("Failed to upsert report for phone %s", phone)

    if reply:
        await _waha_send(phone, reply)


@router.post("/webhook/waha")
async def waha_webhook(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Receive WAHA message events."""
    raw = await request.body()

    # Optional signature check
    if settings.waha_webhook_secret:
        sig = request.headers.get("x-waha-signature", "")
        expected = hmac.new(
            settings.waha_webhook_secret.encode(), raw, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            raise HTTPException(status_code=401, detail="Invalid WAHA signature")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": True}

    event = data.get("event", "")
    payload = data.get("payload", {})

    if event == "message" and not payload.get("fromMe", False):
        background_tasks.add_task(_handle_message, payload, request.app)

    return {"ok": True}
