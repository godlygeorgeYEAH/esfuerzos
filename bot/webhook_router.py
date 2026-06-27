"""
webhook_router.py - FastAPI router for the Reune WAHA webhook.

Endpoints:
  POST /webhook/waha  - receives WhatsApp messages via WAHA
  GET  /webhook/waha  - hub.challenge echo for future Meta migration

Token validation:
  HMAC-SHA256(WAHA_WEBHOOK_SECRET, raw_body).hexdigest() == X-WAHA-Token header
  If WAHA_WEBHOOK_SECRET is empty (local dev), validation is skipped.

All processing is offloaded to asyncio.create_task so the 200 OK is returned
before any Supabase or WAHA API calls are made.

To register this router in app/main.py:
    from api.bot.webhook_router import router as waha_router
    app.include_router(waha_router, prefix="")

Env vars required (in addition to existing .env):
  WAHA_URL             - WAHA base URL
  WAHA_API_TOKEN       - WAHA API key (optional, for WAHA auth)
  WAHA_SESSION         - WAHA session name (default: reune)
  WAHA_WEBHOOK_SECRET  - shared secret for X-WAHA-Token HMAC validation
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, Response

from api.bot.flows import (
    download_media,
    handle_found_flow,
    handle_idle,
    handle_missing_flow,
    handle_role,
    handle_search,
)
from api.bot.sessions import BotState, Session, store

logger = logging.getLogger(__name__)

router = APIRouter()

WAHA_WEBHOOK_SECRET: str = os.environ.get("WAHA_WEBHOOK_SECRET", "")

# States that belong to the missing-person intake branch.
_MISSING_STATES = frozenset(
    {
        BotState.MISSING_NAME,
        BotState.MISSING_AGE,
        BotState.MISSING_LOCATION,
        BotState.MISSING_MARKS,
        BotState.MISSING_PHOTO,
        BotState.MISSING_CONFIRM,
    }
)

# States that belong to the found-person intake branch.
_FOUND_STATES = frozenset(
    {
        BotState.FOUND_NAME,
        BotState.FOUND_AGE,
        BotState.FOUND_LOCATION,
        BotState.FOUND_STATE,
        BotState.FOUND_PHOTO,
        BotState.FOUND_CONFIRM,
    }
)


# ---------------------------------------------------------------------------
# Token validation
# ---------------------------------------------------------------------------

def _validate_hmac(raw_body: bytes, token_header: str) -> bool:
    """
    Return True if HMAC-SHA256(secret, raw_body) matches the header.
    If no secret is configured, skip validation (dev/local mode).
    """
    if not WAHA_WEBHOOK_SECRET:
        return True
    expected = hmac.new(
        WAHA_WEBHOOK_SECRET.encode(),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, token_header)


# ---------------------------------------------------------------------------
# Message dispatcher (runs in background)
# ---------------------------------------------------------------------------

async def _dispatch(payload: dict[str, Any]) -> None:
    """
    Route one inbound WhatsApp message to the correct flow handler.

    Skips:
      - outbound messages (fromMe=true)
      - group chats (@g.us suffix)
      - unsupported message types
    """
    phone: str = payload.get("from", "")
    from_me: bool = payload.get("fromMe", False)
    msg_type: str = payload.get("type", "")
    body: str = (payload.get("body") or "").strip()

    if from_me:
        return
    if "@g.us" in phone:
        return
    if msg_type not in ("chat", "image", "buttons_response", "interactive"):
        return

    # Download media bytes for image messages.
    media_bytes: bytes | None = None
    if msg_type == "image":
        media_url: str = (payload.get("_data") or {}).get("mediaUrl", "")
        if media_url:
            media_bytes = await download_media(media_url)

    # Resolve button_id for button-response messages.
    # WAHA may return selectedButtonId directly or embed it in body.
    button_id = ""
    if msg_type in ("buttons_response", "interactive"):
        button_id = (
            payload.get("selectedButtonId", "")
            or (payload.get("_data") or {}).get("selectedButtonId", "")
            or body
        )

    # Load or create session.
    session: Session = store.get(phone) or Session(phone=phone)
    state = session.state

    try:
        if state == BotState.IDLE:
            await handle_idle(phone, body)

        elif state == BotState.AWAITING_ROLE:
            await handle_role(phone, button_id, button_text=body)

        elif state in _MISSING_STATES:
            await handle_missing_flow(phone, state, body, media_bytes)

        elif state in _FOUND_STATES:
            await handle_found_flow(phone, state, body, media_bytes)

        elif state == BotState.SEARCH_QUERY:
            await handle_search(phone, body)

        else:
            # Unknown state: restart.
            logger.warning("Unknown session state %s for %s, resetting", state, phone)
            store.delete(phone)
            await handle_idle(phone, body)

    except Exception as exc:
        # Swallow exceptions to prevent the background task from raising
        # an unhandled error that would be silently dropped by asyncio.
        logger.exception("Unhandled error dispatching message from %s: %s", phone, exc)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/webhook/waha", status_code=200)
async def waha_webhook(request: Request) -> dict:
    """
    Receive an inbound WAHA webhook event.

    Returns 200 immediately; processing happens in a background task.
    Returns 403 if the X-WAHA-Token header fails HMAC validation.
    """
    raw_body = await request.body()

    token_header = request.headers.get("X-WAHA-Token", "")
    if not _validate_hmac(raw_body, token_header):
        raise HTTPException(status_code=403, detail="Invalid webhook token")

    try:
        body_json: dict = json.loads(raw_body)
    except json.JSONDecodeError:
        # Malformed body: acknowledge and drop.
        return {"ok": True}

    event: str = body_json.get("event", "")
    if event != "message":
        # Only handle message events; ignore session.status, etc.
        return {"ok": True}

    payload: dict = body_json.get("payload", {})

    # Fire-and-forget: return 200 before any downstream I/O.
    asyncio.create_task(_dispatch(payload))
    return {"ok": True}


@router.get("/webhook/waha", status_code=200)
async def waha_challenge(
    hub_challenge: str = Query(default="", alias="hub.challenge"),
    hub_verify_token: str = Query(default="", alias="hub.verify_token"),
) -> Response:
    """
    Meta webhook verification handshake (for future migration from WAHA to Meta Cloud API).
    Echoes hub.challenge if hub.verify_token matches WAHA_WEBHOOK_SECRET.
    """
    if hub_verify_token and WAHA_WEBHOOK_SECRET and hub_verify_token != WAHA_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid verify token")
    return Response(content=hub_challenge, media_type="text/plain")
