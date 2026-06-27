"""
base44_webhook_router.py - FastAPI router for Base44 Superagent webhook events.

Architecture:
  Base44 WhatsApp (user texts) -> Base44 agent converses
  -> message.completed webhook -> this router
  -> face_pipeline (if photo) -> match -> inject notification via Base44 API

Report extraction:
  The Base44 agent is instructed to output structured JSON when a report is
  complete: [REPORT:{...}] in the assistant message.
  This router parses it, upserts the report to Supabase, and processes photos.

Notification:
  When a match is found, inject a system message into both conversations via
  POST /conversations/{id}/messages so Base44 delivers them over WhatsApp.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import uuid
from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from config import get_settings
from face_pipeline import process_photo_for_report

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()

BASE44_URL = f"https://app.base44.com/api/agents/{settings.base44_agent_id}"
BASE44_KEY = settings.base44_api_key
_B44_HEADERS = {"api_key": BASE44_KEY, "Content-Type": "application/json"}

_REPORT_RE = re.compile(r"\[REPORT:(\{.*?\})\]", re.DOTALL)
_PHONE_RE = re.compile(r"\[PHONE:(\+?[\d\s\-\(\)]{7,20})\]")


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def _sb_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


async def _sb_upsert_report(data: dict) -> str | None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    async with httpx.AsyncClient(timeout=10) as cl:
        resp = await cl.post(
            f"{sb}/rest/v1/reports",
            headers={**_sb_headers(key), "Prefer": "resolution=merge-duplicates,return=representation"},
            json=data,
        )
        if resp.status_code not in (200, 201):
            logger.error("upsert_report failed %d: %s", resp.status_code, resp.text[:300])
            return None
        rows = resp.json()
        return rows[0]["id"] if rows else None


async def _sb_upsert_photo(report_id: str, photo_url: str) -> None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    async with httpx.AsyncClient(timeout=10) as cl:
        await cl.post(
            f"{sb}/rest/v1/photos",
            headers={**_sb_headers(key), "Prefer": "resolution=ignore-duplicates,return=minimal"},
            json={"id": str(uuid.uuid4()), "report_id": report_id, "storage_path": photo_url},
        )


async def _sb_store_conv(conv_id: str, phone: str) -> None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    async with httpx.AsyncClient(timeout=5) as cl:
        await cl.post(
            f"{sb}/rest/v1/conversation_ids",
            headers={**_sb_headers(key), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json={"conv_id": conv_id, "phone": phone},
        )


async def _sb_get_conv_for_report(report_id: str) -> str | None:
    """Find conv_id for the phone linked to a report."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    async with httpx.AsyncClient(timeout=5) as cl:
        r = await cl.get(
            f"{sb}/rest/v1/reports",
            headers=_sb_headers(key),
            params={"id": f"eq.{report_id}", "select": "phone"},
        )
        if r.status_code != 200 or not r.json():
            return None
        phone = r.json()[0].get("phone", "")
        if not phone:
            return None
        r2 = await cl.get(
            f"{sb}/rest/v1/conversation_ids",
            headers=_sb_headers(key),
            params={"phone": f"eq.{phone}", "select": "conv_id"},
        )
        if r2.status_code == 200 and r2.json():
            return r2.json()[0].get("conv_id")
    return None


# ---------------------------------------------------------------------------
# Base44 client helpers
# ---------------------------------------------------------------------------

async def _b44_get_conversation(conv_id: str) -> dict | None:
    async with httpx.AsyncClient(timeout=15) as cl:
        r = await cl.get(f"{BASE44_URL}/conversations/{conv_id}", headers=_B44_HEADERS)
        if r.status_code == 200:
            return r.json()
    return None


async def _b44_send_message(conv_id: str, content: str) -> bool:
    async with httpx.AsyncClient(timeout=20) as cl:
        r = await cl.post(
            f"{BASE44_URL}/conversations/{conv_id}/messages",
            headers=_B44_HEADERS,
            json={"role": "user", "content": content},
        )
        return r.status_code in (200, 201)


# ---------------------------------------------------------------------------
# Webhook endpoint
# ---------------------------------------------------------------------------

@router.post("/hooks/base44")
async def base44_webhook(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Receive Base44 message.completed events and trigger matching pipeline."""
    raw = await request.body()

    # C2: Fail-closed: reject all webhook calls if secret is not configured
    if not settings.base44_webhook_secret:
        logger.warning("base44_webhook: BASE44_WEBHOOK_SECRET not set; rejecting request")
        raise HTTPException(status_code=403, detail="Webhook secret not configured")

    sig = request.headers.get("x-base44-signature", "")
    expected = "sha256=" + hmac.new(
        settings.base44_webhook_secret.encode(), raw, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(sig.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {"ok": True}

    event = payload.get("event", "")
    conv_id = payload.get("conversation_id", "")

    if not conv_id:
        return {"ok": True}

    # Deduplicate using X-Base44-Delivery header
    delivery = request.headers.get("x-base44-delivery", "")
    logger.info("base44 event=%s conv=%s delivery=%s", event, conv_id, delivery)

    if event == "message.completed":
        background_tasks.add_task(_handle_completed, conv_id, request.app)

    return {"ok": True}


# ---------------------------------------------------------------------------
# Background processing
# ---------------------------------------------------------------------------

async def _handle_completed(conv_id: str, app: Any) -> None:
    """Fetch conversation, extract report data, process photos, trigger matching."""
    conv = await _b44_get_conversation(conv_id)
    if not conv:
        logger.warning("Could not fetch conversation %s", conv_id)
        return

    messages = conv.get("messages", [])
    if not messages:
        return

    # Extract phone from any message (agent formats [PHONE:+XX...])
    phone = _extract_phone(messages)
    if phone:
        await _sb_store_conv(conv_id, phone)

    # Extract report JSON from last assistant message
    report_data = _extract_report(messages)
    if not report_data:
        return

    # Inject conv metadata
    if phone:
        report_data.setdefault("phone", phone)
    report_data.setdefault("source", "base44_whatsapp")
    report_data["source_url"] = f"base44:{conv_id}"

    # Upsert report
    report_id = await _sb_upsert_report(report_data)
    if not report_id:
        logger.error("Failed to upsert report for conv %s", conv_id)
        return

    logger.info("Report upserted: %s (conv=%s)", report_id, conv_id)

    # Collect all photo URLs from user messages
    photo_urls = _extract_photo_urls(messages)
    if not photo_urls:
        logger.info("No photos in conv %s", conv_id)
        return

    # Process each photo through face pipeline
    match_found = False
    for url in photo_urls:
        await _sb_upsert_photo(report_id, url)
        match_id = await process_photo_for_report(report_id, url, app)
        if match_id:
            match_found = True
            logger.info("Face match found: match_id=%s conv=%s", match_id, conv_id)
            await _notify_match(match_id, conv_id, report_id)
            break

    if not match_found:
        logger.info("No face match above threshold for report %s", report_id)


async def _notify_match(match_id: str, reporter_conv_id: str, reporter_report_id: str) -> None:
    """Notify both parties of a possible match."""
    # Determine match roles from Supabase
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    async with httpx.AsyncClient(timeout=10) as cl:
        r = await cl.get(
            f"{sb}/rest/v1/matches",
            headers=_sb_headers(key),
            params={"id": f"eq.{match_id}", "select": "missing_id,found_id"},
        )
        if r.status_code != 200 or not r.json():
            return
        match = r.json()[0]

    missing_id = match["missing_id"]
    found_id = match["found_id"]
    other_id = found_id if reporter_report_id == missing_id else missing_id

    # Notify the current reporter
    await _b44_send_message(
        reporter_conv_id,
        "SISTEMA: Posible coincidencia encontrada para tu reporte. "
        "El equipo de verificacion fue notificado. "
        "No confirmes la identidad hasta recibir validacion oficial.",
    )

    # Notify the other party (if we have their conv_id)
    other_conv_id = await _sb_get_conv_for_report(other_id)
    if other_conv_id and other_conv_id != reporter_conv_id:
        await _b44_send_message(
            other_conv_id,
            "SISTEMA: Posible coincidencia encontrada para tu reporte. "
            "El equipo de verificacion fue notificado. "
            "No confirmes la identidad hasta recibir validacion oficial.",
        )
        logger.info("Notified other party conv=%s report=%s", other_conv_id, other_id)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _extract_report(messages: list[dict]) -> dict | None:
    for msg in reversed(messages):
        if msg.get("role") == "assistant":
            content = msg.get("content", "")
            m = _REPORT_RE.search(content)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    logger.warning("Malformed [REPORT:...] JSON: %s", m.group(1)[:200])
    return None


def _extract_phone(messages: list[dict]) -> str | None:
    for msg in messages:
        content = msg.get("content", "")
        m = _PHONE_RE.search(content)
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
