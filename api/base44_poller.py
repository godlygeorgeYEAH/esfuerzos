"""
base44_poller.py - Background polling loop for Base44 Superagent.

Replaces the webhook approach (requires Builder plan) with a 30-second
polling loop that fetches all conversations and processes new user messages.

Flow:
  1. Every 30s: GET /conversations (list all)
  2. For each conversation: check last_seen_at in conversation_ids table
  3. If new user message found after last_seen_at:
     - Extract [PHONE:...] -> store in conversation_ids
     - Extract [REPORT:...] -> upsert report in Supabase
     - Process any file_urls (photos) -> face pipeline
     - If face match found -> notify both parties
  4. Update last_seen_at in conversation_ids
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_REPORT_RE = re.compile(r"\[REPORT:(\{.*?\})\]", re.DOTALL)
_PHONE_RE = re.compile(r"\[PHONE:(\+?[\d\s\-\(\)]{7,20})\]")


def _cfg(settings) -> tuple[str, dict]:
    url = f"https://app.base44.com/api/agents/{settings.base44_agent_id}"
    headers = {"api_key": settings.base44_api_key, "Content-Type": "application/json"}
    return url, headers


def _sb_hdrs(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

async def run_polling_loop(app: Any) -> None:
    """Run forever: poll Base44 every 30s for new messages."""
    from config import get_settings
    settings = get_settings()

    logger.info("Base44 polling loop started (interval=30s)")
    while True:
        try:
            await _poll_once(app, settings)
        except Exception as exc:
            logger.error("base44_poller error: %s", exc)
        await asyncio.sleep(30)


async def _poll_once(app: Any, settings) -> None:
    url, hdrs = _cfg(settings)
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key

    async with httpx.AsyncClient(timeout=20) as cl:
        # List all conversations
        r = await cl.get(f"{url}/conversations", headers=hdrs)
        if r.status_code != 200:
            logger.debug("poll: GET conversations %d", r.status_code)
            return

        data = r.json()
        convs = data if isinstance(data, list) else data.get("conversations", [])
        if not convs:
            return

        # Fetch last_seen_at for all known conversations
        known = await _get_known_convs(cl, sb, key)

        for conv_stub in convs:
            conv_id = conv_stub.get("id") or conv_stub.get("conversation_id")
            if not conv_id:
                continue

            # Only fetch full conversation if it might have new messages
            last_seen = known.get(conv_id, {}).get("last_seen_at", "")
            conv_updated = conv_stub.get("updated_at", conv_stub.get("last_message_at", ""))
            if last_seen and conv_updated and conv_updated <= last_seen:
                continue

            # Fetch full conversation
            r2 = await cl.get(f"{url}/conversations/{conv_id}", headers=hdrs)
            if r2.status_code != 200:
                continue
            conv = r2.json()
            messages = conv.get("messages", [])
            if not messages:
                continue

            await _process_conversation(cl, conv_id, messages, app, settings, sb, key)


async def _process_conversation(
    cl: httpx.AsyncClient,
    conv_id: str,
    messages: list[dict],
    app: Any,
    settings,
    sb: str,
    key: str,
) -> None:
    url, hdrs = _cfg(settings)

    # Extract phone from any message
    phone = _extract_phone(messages)
    if phone:
        await _store_conv(cl, sb, key, conv_id, phone)

    # Find the latest [REPORT:...] in assistant messages
    report_data = _extract_report(messages)
    report_id: str | None = None

    if report_data:
        # Normalize fields to what reports table expects
        report_data.setdefault("source", "base44_whatsapp")
        report_data["source_url"] = f"base44:{conv_id}"
        # Remove keys not in reports schema
        for bad in ("reporter_phone", "phone"):
            report_data.pop(bad, None)

        report_id = await _upsert_report(cl, sb, key, report_data)
        if report_id:
            logger.info("Report upserted: %s (conv=%s)", report_id, conv_id)
            await _store_conv_report(cl, sb, key, conv_id, report_id)

    if not report_id:
        # Try to find existing report for this conv
        report_id = await _get_report_for_conv(cl, sb, key, conv_id)

    # Process new photo URLs
    if report_id:
        photo_urls = _extract_photo_urls(messages)
        already_queued = await _get_queued_photos(cl, sb, key, report_id)
        new_photos = [u for u in photo_urls if u not in already_queued]

        for url_photo in new_photos:
            await _upsert_photo(cl, sb, key, report_id, url_photo)
            if hasattr(app.state, "face_model"):
                from face_pipeline import process_photo_for_report
                try:
                    match_id = await process_photo_for_report(report_id, url_photo, app)
                    if match_id:
                        logger.info("Match: %s for conv %s", match_id, conv_id)
                        await _notify_match(cl, match_id, conv_id, report_id, settings, sb, key)
                except Exception as exc:
                    logger.error("face pipeline error conv=%s photo=%s: %s", conv_id, url_photo, exc)

    # Update last_seen_at
    await _update_last_seen(cl, sb, key, conv_id)


# ---------------------------------------------------------------------------
# Notification
# ---------------------------------------------------------------------------

async def _notify_match(
    cl: httpx.AsyncClient,
    match_id: str,
    reporter_conv_id: str,
    reporter_report_id: str,
    settings,
    sb: str,
    key: str,
) -> None:
    url, hdrs = _cfg(settings)

    r = await cl.get(
        f"{sb}/rest/v1/matches",
        headers=_sb_hdrs(key),
        params={"id": f"eq.{match_id}", "select": "missing_id,found_id"},
    )
    if r.status_code != 200 or not r.json():
        return
    match = r.json()[0]

    missing_id = match["missing_id"]
    found_id = match["found_id"]
    other_id = found_id if reporter_report_id == missing_id else missing_id

    msg = (
        "SISTEMA: Posible coincidencia encontrada para tu reporte. "
        "El equipo de verificacion fue notificado. "
        "Por favor espera la confirmacion oficial."
    )
    await cl.post(
        f"{url}/conversations/{reporter_conv_id}/messages",
        headers=hdrs,
        json={"role": "user", "content": msg},
    )

    # Notify other party
    other_conv = await _get_conv_for_report(cl, sb, key, other_id)
    if other_conv and other_conv != reporter_conv_id:
        await cl.post(
            f"{url}/conversations/{other_conv}/messages",
            headers=hdrs,
            json={"role": "user", "content": msg},
        )


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

async def _get_known_convs(cl: httpx.AsyncClient, sb: str, key: str) -> dict:
    r = await cl.get(
        f"{sb}/rest/v1/conversation_ids",
        headers=_sb_hdrs(key),
        params={"select": "conv_id,last_seen_at"},
    )
    if r.status_code == 200:
        return {row["conv_id"]: row for row in r.json()}
    return {}


async def _store_conv(cl: httpx.AsyncClient, sb: str, key: str, conv_id: str, phone: str) -> None:
    await cl.post(
        f"{sb}/rest/v1/conversation_ids",
        headers={**_sb_hdrs(key), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={"conv_id": conv_id, "phone": phone},
    )


async def _store_conv_report(cl: httpx.AsyncClient, sb: str, key: str, conv_id: str, report_id: str) -> None:
    await cl.patch(
        f"{sb}/rest/v1/conversation_ids",
        headers={**_sb_hdrs(key), "Prefer": "return=minimal"},
        params={"conv_id": f"eq.{conv_id}"},
        json={"report_id": report_id},
    )


async def _update_last_seen(cl: httpx.AsyncClient, sb: str, key: str, conv_id: str) -> None:
    import datetime
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    # Upsert by conv_id (PK). Creates row if missing, updates last_seen_at if exists.
    await cl.post(
        f"{sb}/rest/v1/conversation_ids",
        headers={**_sb_hdrs(key), "Prefer": "resolution=merge-duplicates,return=minimal"},
        json={"conv_id": conv_id, "last_seen_at": now},
        params={"on_conflict": "conv_id"},
    )


async def _upsert_report(cl: httpx.AsyncClient, sb: str, key: str, data: dict) -> str | None:
    r = await cl.post(
        f"{sb}/rest/v1/reports",
        headers={**_sb_hdrs(key), "Prefer": "resolution=merge-duplicates,return=representation"},
        json=data,
    )
    if r.status_code in (200, 201) and r.json():
        return r.json()[0]["id"]
    logger.debug("upsert_report %d: %s", r.status_code, r.text[:150])
    return None


async def _upsert_photo(cl: httpx.AsyncClient, sb: str, key: str, report_id: str, photo_url: str) -> None:
    await cl.post(
        f"{sb}/rest/v1/photos",
        headers={**_sb_hdrs(key), "Prefer": "resolution=ignore-duplicates,return=minimal"},
        json={"id": str(uuid.uuid4()), "report_id": report_id, "storage_path": photo_url},
    )


async def _get_queued_photos(cl: httpx.AsyncClient, sb: str, key: str, report_id: str) -> set:
    r = await cl.get(
        f"{sb}/rest/v1/photos",
        headers=_sb_hdrs(key),
        params={"report_id": f"eq.{report_id}", "select": "storage_path"},
    )
    if r.status_code == 200:
        return {row["storage_path"] for row in r.json() if row.get("storage_path")}
    return set()


async def _get_report_for_conv(cl: httpx.AsyncClient, sb: str, key: str, conv_id: str) -> str | None:
    r = await cl.get(
        f"{sb}/rest/v1/conversation_ids",
        headers=_sb_hdrs(key),
        params={"conv_id": f"eq.{conv_id}", "select": "report_id"},
    )
    if r.status_code == 200 and r.json():
        return r.json()[0].get("report_id")
    return None


async def _get_conv_for_report(cl: httpx.AsyncClient, sb: str, key: str, report_id: str) -> str | None:
    r = await cl.get(
        f"{sb}/rest/v1/conversation_ids",
        headers=_sb_hdrs(key),
        params={"report_id": f"eq.{report_id}", "select": "conv_id"},
    )
    if r.status_code == 200 and r.json():
        return r.json()[0].get("conv_id")
    return None


# ---------------------------------------------------------------------------
# Parsers (shared logic with base44_webhook_router)
# ---------------------------------------------------------------------------

def _extract_report(messages: list[dict]) -> dict | None:
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            m = _REPORT_RE.search(msg.get("content", ""))
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass
    return None


def _extract_phone(messages: list[dict]) -> str | None:
    for msg in messages:
        m = _PHONE_RE.search(msg.get("content", ""))
        if m:
            raw = re.sub(r"[\s\-\(\)]", "", m.group(1))
            return raw if raw.startswith("+") else f"+{raw}"
    return None


def _extract_photo_urls(messages: list[dict]) -> list[str]:
    urls: list[str] = []
    for msg in messages:
        if msg.get("role") == "user":
            for url in msg.get("file_urls", []):
                if url and url not in urls:
                    urls.append(url)
    return urls
