"""
flows.py - Conversation flow handlers for the Reune WhatsApp bot.

All user-facing text is Spanish. No em dashes.
Messages are kept under 160 chars where practical.

Requires env vars:
  WAHA_URL            - WAHA base URL (e.g. http://localhost:3000)
  WAHA_API_TOKEN      - optional WAHA API key (X-Api-Key header)
  WAHA_SESSION        - WAHA session name (default: reune)
  SUPABASE_URL        - Supabase project URL
  SUPABASE_SERVICE_ROLE_KEY - Supabase service role key (bypasses RLS)

Optional dependency:
  rapidfuzz           - for fuzzy name search (pip install rapidfuzz)
                        Search degrades gracefully if not installed.

Supabase table expected:
  reunion_reports (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at     TIMESTAMPTZ DEFAULT now(),
    kind           TEXT CHECK (kind IN ('missing', 'found')),
    reporter_wa_hash TEXT,
    name           TEXT,
    age            TEXT,
    location       TEXT,
    marks          TEXT,
    found_state    TEXT,   -- 'alive' | 'injured' | 'unknown'
    photo_url      TEXT,
    raw_data       JSONB,
    verified       BOOLEAN DEFAULT false
  )

Storage bucket: reunion-photos (public read, service-role write)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Optional

import httpx
from supabase import Client, create_client

from bot.sessions import BotState, Session, store
from scrapers.base import SearchQuery, parse_age_int

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rapidfuzz (optional)
# ---------------------------------------------------------------------------

try:
    from rapidfuzz import fuzz
    from rapidfuzz import process as rfprocess
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False
    logger.warning("rapidfuzz not installed -- search will be disabled. pip install rapidfuzz")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WAHA_URL: str = os.environ.get("WAHA_URL", "http://localhost:3000")
WAHA_TOKEN: str = os.environ.get("WAHA_API_TOKEN", "")
WAHA_SESSION: str = os.environ.get("WAHA_SESSION", "reune")

_sb_client: Optional[Client] = None


def _get_supabase() -> Client:
    global _sb_client
    if _sb_client is None:
        _sb_client = create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_SERVICE_ROLE_KEY"],
        )
    return _sb_client


def _hash_phone(phone: str) -> str:
    """One-way hash for reporter privacy. Returns first 32 hex chars of SHA-256."""
    return hashlib.sha256(phone.encode()).hexdigest()[:32]


def _waha_headers() -> dict:
    return {"X-Api-Key": WAHA_TOKEN} if WAHA_TOKEN else {}


# ---------------------------------------------------------------------------
# WAHA send helpers
# ---------------------------------------------------------------------------

async def send_text(chat_id: str, text: str) -> None:
    url = f"{WAHA_URL}/api/sendText"
    payload = {"chatId": chat_id, "text": text, "session": WAHA_SESSION}
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.post(url, json=payload, headers=_waha_headers())
            r.raise_for_status()
        except Exception as exc:
            logger.error("send_text to %s failed: %s", chat_id, exc)


async def send_buttons(
    chat_id: str,
    content_text: str,
    buttons: list[dict],
    footer_text: str = "",
) -> None:
    url = f"{WAHA_URL}/api/sendButtons"
    payload = {
        "chatId": chat_id,
        "contentText": content_text,
        "footerText": footer_text,
        "buttons": buttons,
        "session": WAHA_SESSION,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            r = await client.post(url, json=payload, headers=_waha_headers())
            r.raise_for_status()
        except Exception as exc:
            logger.error("send_buttons to %s failed: %s", chat_id, exc)


async def download_media(media_url: str) -> Optional[bytes]:
    """Download media bytes from a WAHA-provided media URL."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(media_url, headers=_waha_headers())
            r.raise_for_status()
            return r.content
        except Exception as exc:
            logger.error("download_media %s failed: %s", media_url, exc)
            return None


# ---------------------------------------------------------------------------
# Welcome / role selection
# ---------------------------------------------------------------------------

async def handle_idle(phone: str, msg_body: str) -> None:  # noqa: ARG001
    """Send welcome message with role selection buttons and move to AWAITING_ROLE."""
    session = store.get(phone) or Session(phone=phone)
    session.state = BotState.AWAITING_ROLE
    session.data = {}
    store.set(phone, session)

    await send_buttons(
        chat_id=phone,
        content_text=(
            "Hola. Soy el asistente de Reune. "
            "Te ayudo a reportar desaparecidos, registrar personas encontradas "
            "o buscar a alguien."
        ),
        buttons=[
            {"buttonId": "1", "buttonText": {"displayText": "Reporto un desaparecido"}},
            {"buttonId": "2", "buttonText": {"displayText": "Encontre a alguien"}},
            {"buttonId": "3", "buttonText": {"displayText": "Buscar persona"}},
        ],
        footer_text="Selecciona una opcion",
    )


async def handle_role(phone: str, button_id: str, button_text: str = "") -> None:
    """Route to the correct flow based on the button the user pressed."""
    session = store.get(phone) or Session(phone=phone)

    # Resolve choice: prefer button_id, fall back to keyword scan on button_text.
    choice = button_id.strip()
    if choice not in ("1", "2", "3"):
        lower = button_text.lower()
        if "desaparec" in lower:
            choice = "1"
        elif "encontr" in lower or "hall" in lower:
            choice = "2"
        elif "buscar" in lower or "busco" in lower:
            choice = "3"
        else:
            choice = ""

    if choice == "1":
        session.state = BotState.MISSING_NAME
        session.data = {}
        store.set(phone, session)
        await send_text(phone, "Nombre completo de la persona desaparecida:")

    elif choice == "2":
        session.state = BotState.FOUND_NAME
        session.data = {}
        store.set(phone, session)
        await send_text(phone, "Nombre de la persona encontrada (escribe 'desconocido' si no lo sabes):")

    elif choice == "3":
        session.state = BotState.SEARCH_QUERY
        store.set(phone, session)
        await send_text(phone, "Escribe el nombre de la persona que buscas:")

    else:
        # Unrecognized input: re-send menu.
        await handle_idle(phone, "")


# ---------------------------------------------------------------------------
# Missing person flow
# ---------------------------------------------------------------------------

async def handle_missing_flow(
    phone: str,
    state: BotState,
    body: str,
    media_bytes: Optional[bytes] = None,
) -> None:
    """Collect intake data for a missing person report, step by step."""
    session = store.get(phone)
    if session is None:
        await handle_idle(phone, "")
        return

    text = body.strip()

    if state == BotState.MISSING_NAME:
        session.data["name"] = text
        session.state = BotState.MISSING_AGE
        store.set(phone, session)
        await send_text(phone, "Edad aproximada (o 'no sabe'):")

    elif state == BotState.MISSING_AGE:
        session.data["age"] = text
        session.state = BotState.MISSING_LOCATION
        store.set(phone, session)
        await send_text(phone, "Ultima ubicacion conocida:")

    elif state == BotState.MISSING_LOCATION:
        session.data["location"] = text
        session.state = BotState.MISSING_MARKS
        store.set(phone, session)
        await send_text(
            phone,
            "Senales particulares (ropa que llevaba, cicatrices, tatuajes). "
            "Escribe 'ninguna' si no sabes:",
        )

    elif state == BotState.MISSING_MARKS:
        session.data["marks"] = text
        session.state = BotState.MISSING_PHOTO
        store.set(phone, session)
        await send_text(phone, "Envia una foto de la persona (o escribe 'sin foto'):")

    elif state == BotState.MISSING_PHOTO:
        if media_bytes:
            session.data["photo_bytes"] = media_bytes
            session.state = BotState.MISSING_CONFIRM
        elif text.lower() in _SKIP_WORDS:
            session.state = BotState.MISSING_CONFIRM
        else:
            await send_text(phone, "Envia la foto o escribe 'sin foto' para continuar.")
            return

        store.set(phone, session)
        d = session.data
        summary = (
            f"Nombre: {d.get('name')}\n"
            f"Edad: {d.get('age')}\n"
            f"Ultima ubicacion: {d.get('location')}\n"
            f"Senales: {d.get('marks')}\n"
            f"Foto: {'si' if d.get('photo_bytes') else 'no'}\n\n"
            "Confirmas este reporte? Responde SI o NO."
        )
        await send_text(phone, summary)

    elif state == BotState.MISSING_CONFIRM:
        if text.upper() in _CONFIRM_WORDS:
            photo = session.data.pop("photo_bytes", None)
            await create_report(phone, "missing", dict(session.data), photo)
            store.delete(phone)
        else:
            store.delete(phone)
            await send_text(
                phone,
                "Reporte cancelado. Escribe cualquier mensaje para empezar de nuevo.",
            )


# ---------------------------------------------------------------------------
# Found person flow
# ---------------------------------------------------------------------------

_FOUND_STATE_MAP = {
    "1": "alive",
    "2": "injured",
    "3": "unknown",
    "vivo": "alive",
    "estable": "alive",
    "herido": "injured",
    "herida": "injured",
    "desconocido": "unknown",
    "desconocida": "unknown",
    "no sabe": "unknown",
}

_FOUND_STATE_LABELS = {
    "alive": "Vivo y estable",
    "injured": "Herido/a",
    "unknown": "Desconocido",
}

_SKIP_WORDS = {"sin foto", "no", "omitir", "saltar", "skip"}
_CONFIRM_WORDS = {"SI", "S", "YES", "SII", "SIII", "OK", "CONFIRMO"}


async def handle_found_flow(
    phone: str,
    state: BotState,
    body: str,
    media_bytes: Optional[bytes] = None,
) -> None:
    """Collect intake data for a found person report, step by step."""
    session = store.get(phone)
    if session is None:
        await handle_idle(phone, "")
        return

    text = body.strip()

    if state == BotState.FOUND_NAME:
        session.data["name"] = text
        session.state = BotState.FOUND_AGE
        store.set(phone, session)
        await send_text(phone, "Edad aproximada (o 'no sabe'):")

    elif state == BotState.FOUND_AGE:
        session.data["age"] = text
        session.state = BotState.FOUND_LOCATION
        store.set(phone, session)
        await send_text(phone, "Donde se encuentra ahora mismo esta persona (lugar exacto):")

    elif state == BotState.FOUND_LOCATION:
        session.data["location"] = text
        session.state = BotState.FOUND_STATE
        store.set(phone, session)
        await send_buttons(
            chat_id=phone,
            content_text="Estado de la persona:",
            buttons=[
                {"buttonId": "1", "buttonText": {"displayText": "Vivo y estable"}},
                {"buttonId": "2", "buttonText": {"displayText": "Herido/a"}},
                {"buttonId": "3", "buttonText": {"displayText": "Estado desconocido"}},
            ],
        )

    elif state == BotState.FOUND_STATE:
        found_state = _FOUND_STATE_MAP.get(text.lower()) or _FOUND_STATE_MAP.get(text, "unknown")
        session.data["found_state"] = found_state
        session.state = BotState.FOUND_PHOTO
        store.set(phone, session)
        await send_text(phone, "Envia una foto (o escribe 'sin foto'):")

    elif state == BotState.FOUND_PHOTO:
        if media_bytes:
            session.data["photo_bytes"] = media_bytes
            session.state = BotState.FOUND_CONFIRM
        elif text.lower() in _SKIP_WORDS:
            session.state = BotState.FOUND_CONFIRM
        else:
            await send_text(phone, "Envia la foto o escribe 'sin foto' para continuar.")
            return

        store.set(phone, session)
        d = session.data
        state_label = _FOUND_STATE_LABELS.get(d.get("found_state", "unknown"), "Desconocido")
        summary = (
            f"Nombre: {d.get('name')}\n"
            f"Edad: {d.get('age')}\n"
            f"Ubicacion: {d.get('location')}\n"
            f"Estado: {state_label}\n"
            f"Foto: {'si' if d.get('photo_bytes') else 'no'}\n\n"
            "Confirmas este reporte? Responde SI o NO."
        )
        await send_text(phone, summary)

    elif state == BotState.FOUND_CONFIRM:
        if text.upper() in _CONFIRM_WORDS:
            photo = session.data.pop("photo_bytes", None)
            await create_report(phone, "found", dict(session.data), photo)
            store.delete(phone)
        else:
            store.delete(phone)
            await send_text(
                phone,
                "Reporte cancelado. Escribe cualquier mensaje para empezar de nuevo.",
            )


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

async def handle_search(phone: str, query: str) -> None:
    """Fuzzy-search recent reunion_reports by name, return top 3."""
    store.delete(phone)  # Search is one-shot; reset session now.

    if not _HAS_RAPIDFUZZ:
        await send_text(
            phone,
            "Busqueda no disponible en este momento. Intenta mas tarde.",
        )
        return

    try:
        sb = _get_supabase()
        resp = (
            sb.table("reunion_reports")
            .select("id, kind, name, age, location, found_state, created_at")
            .order("created_at", desc=True)
            .limit(300)
            .execute()
        )
        reports = resp.data or []
    except Exception as exc:
        logger.error("search query failed: %s", exc)
        await send_text(phone, "Error al buscar. Intenta de nuevo en unos minutos.")
        return

    if not reports:
        await send_text(phone, "No hay reportes registrados aun.")
        return

    names = [r.get("name") or "" for r in reports]
    matches = rfprocess.extract(  # type: ignore[union-attr]
        query,
        names,
        scorer=fuzz.WRatio,  # type: ignore[union-attr]
        limit=3,
        score_cutoff=50,
    )

    if not matches:
        await send_text(
            phone,
            f"Sin coincidencias para '{query}'. Verifica el nombre e intenta de nuevo.",
        )
        return

    lines = [f"Posibles coincidencias para '{query}':"]
    for _match_name, _score, idx in matches:
        r = reports[idx]
        kind_label = (
            "Desaparecido/a"
            if r.get("kind") == "missing"
            else "Posible coincidencia, en verificacion"
        )
        loc = r.get("location") or "ubicacion no indicada"
        age = r.get("age") or "edad no indicada"
        lines.append(f"- {r.get('name')} | {age} | {loc} | {kind_label}")

    lines.append("\nResultados en verificacion. No confirmados.")
    await send_text(phone, "\n".join(lines))


# ---------------------------------------------------------------------------
# Report creation
# ---------------------------------------------------------------------------

async def _upload_photo(photo_bytes: bytes, report_id: str, kind: str) -> Optional[str]:
    """Upload photo to Supabase storage bucket 'reunion-photos'. Returns public URL."""
    sb = _get_supabase()
    bucket = "reunion-photos"
    path = f"{kind}/{report_id}.jpg"
    try:
        sb.storage.from_(bucket).upload(
            path,
            photo_bytes,
            {"content-type": "image/jpeg"},
        )
        return sb.storage.from_(bucket).get_public_url(path)
    except Exception as exc:
        logger.error("photo upload failed for report %s: %s", report_id, exc)
        return None


async def create_report(
    phone: str,
    kind: str,
    data: dict,
    photo_bytes: Optional[bytes] = None,
) -> None:
    """Insert a missing/found report into Supabase and send confirmation."""
    reporter_hash = _hash_phone(phone)
    sb = _get_supabase()

    row: dict = {
        "kind": kind,
        "reporter_wa_hash": reporter_hash,
        "name": data.get("name"),
        "age": data.get("age"),
        "location": data.get("location"),
        "raw_data": data,
    }

    if kind == "missing":
        row["marks"] = data.get("marks")
    else:
        row["found_state"] = data.get("found_state")

    try:
        result = sb.table("reunion_reports").insert(row).execute()
        report_id: str = (result.data or [{}])[0].get("id", "---")
    except Exception as exc:
        logger.error("insert report failed: %s", exc)
        await send_text(
            phone,
            "Error al guardar el reporte. Intenta de nuevo en unos minutos.",
        )
        return

    if photo_bytes:
        photo_url = await _upload_photo(photo_bytes, report_id, kind)
        if photo_url:
            try:
                sb.table("reunion_reports").update({"photo_url": photo_url}).eq("id", report_id).execute()
            except Exception as exc:
                logger.error("photo_url update failed for %s: %s", report_id, exc)

    short_id = str(report_id)[:8]

    if kind == "missing":
        msg = (
            f"Reporte registrado. ID: {short_id}\n"
            "Lo compartiremos con los equipos de busqueda. "
            "Te avisamos si hay coincidencias."
        )
    else:
        msg = (
            f"Reporte registrado. ID: {short_id}\n"
            "Posible coincidencia en verificacion. "
            "Un coordinador revisara este caso a la brevedad."
        )

    await send_text(phone, msg)

    # Fire exploratory search as a background task -- non-blocking.
    # Searches all external sources in parallel (hospitales_ve, red_ayuda, etc.)
    # and sends a WhatsApp follow-up if strong leads (score > 0.6) are found.
    # Only fires for 'missing' reports; 'found' reports are already an answer.
    if kind == "missing" and report_id != "---":
        try:
            from api.search_orchestrator import run_exploratory_search  # lazy import avoids circular
            query = SearchQuery(
                full_name=data.get("name", ""),
                age=parse_age_int(data.get("age")),
                last_seen_location=data.get("location"),
                kind=kind,
                report_id=report_id,
                reporter_phone=phone,
            )
            asyncio.create_task(run_exploratory_search(query))
            logger.info("Exploratory search task queued for report %s", report_id)
        except Exception as exc:
            # Never block report confirmation because of orchestrator failure
            logger.error("Could not queue exploratory search for %s: %s", report_id, exc)
