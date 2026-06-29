"""
telegram_intake.py - Telegram Bot intake channel (replaces WAHA WhatsApp).

A thin transport adapter: it reuses the entire channel-agnostic intake core in
waha_intake (the deterministic form, LLM extraction, matching, search, photo/face,
session persistence, rate-limit, per-chat lock). For each Telegram update it builds
a WAHA-shaped payload and calls waha_intake._handle_message(payload, app); replies
go back out through the channel dispatcher (waha_intake._waha_send routes "tg:<id>"
to the sender we register here).

Transport: python-telegram-bot v21, LONG-POLLING (no public webhook/HMAC needed,
works behind the firewall). Runs as an asyncio task started from main.py lifespan.
Chat key = "tg:<chat_id>". Photos are downloaded to bytes and passed in-memory (no
token-bearing URL is persisted). Public bot: any user may message it.

Enable by setting TELEGRAM_BOT_TOKEN (from @BotFather).
"""
from __future__ import annotations

import logging
from typing import Any

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import Application, ContextTypes, MessageHandler, filters

import waha_intake
from config import get_settings

logger = logging.getLogger("telegram_intake")
settings = get_settings()

_FASTAPI_APP: Any = None     # the FastAPI app (carries app.state.*); set on start
_BOT: Any = None             # telegram Bot instance, for the send dispatcher


async def _send(chat_id: str, text: str) -> bool:
    """Sender registered with waha_intake's dispatcher. chat_id is the bare numeric id."""
    if _BOT is None:
        return False
    try:
        await _BOT.send_message(chat_id=int(chat_id), text=text, parse_mode="Markdown")
        return True
    except BadRequest:
        # Markdown parse error (unbalanced * in a name etc.) -> resend as plain text.
        try:
            await _BOT.send_message(chat_id=int(chat_id), text=text)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("telegram plain send failed to %s: %s", chat_id, exc)
            return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("telegram send failed to %s: %s", chat_id, exc)
        return False


async def _on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.text:
        return
    payload = {
        "from": f"tg:{msg.chat_id}",
        "id": f"tg:{msg.message_id}",
        "body": msg.text,
        "fromMe": False,
        "hasMedia": False,
    }
    try:
        await waha_intake._handle_message(payload, _FASTAPI_APP)
    except Exception as exc:  # noqa: BLE001
        logger.error("telegram text handler error: %s", exc)


async def _on_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.photo:
        return
    try:
        photo = msg.photo[-1]  # largest resolution
        f = await context.bot.get_file(photo.file_id)
        buf = await f.download_as_bytearray()
        payload = {
            "from": f"tg:{msg.chat_id}",
            "id": f"tg:{msg.message_id}",
            "body": msg.caption or "",
            "fromMe": False,
            "hasMedia": True,
            "mediaUrl": f"telegram:{photo.file_unique_id}",  # stable key, not a URL
            "_image_bytes": bytes(buf),
        }
        await waha_intake._handle_message(payload, _FASTAPI_APP)
    except Exception as exc:  # noqa: BLE001
        logger.error("telegram photo handler error: %s", exc)


async def start_polling(fastapi_app: Any) -> Application | None:
    """Build the bot, register the send dispatcher, start long-polling. Returns the
    Application handle (for shutdown) or None when no token is configured."""
    global _FASTAPI_APP, _BOT
    if not settings.telegram_bot_token:
        logger.info("TELEGRAM_BOT_TOKEN not set; Telegram channel disabled")
        return None
    _FASTAPI_APP = fastapi_app
    application = Application.builder().token(settings.telegram_bot_token).build()
    application.add_handler(MessageHandler(filters.PHOTO, _on_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_text))
    _BOT = application.bot
    waha_intake.register_telegram_sender(_send)
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram intake polling started")
    return application


async def stop_polling(application: Application | None) -> None:
    if application is None:
        return
    try:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Telegram intake stopped")
    except Exception as exc:  # noqa: BLE001
        logger.warning("telegram stop error: %s", exc)
