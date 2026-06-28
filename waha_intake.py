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

import asyncio
import hashlib
import hmac
import json
import logging
import re
import time
import uuid
from collections import defaultdict, deque
from typing import Any

import httpx
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request

from config import get_settings
from consolidation_pipeline import embed_and_match_report
from face_pipeline import process_photo_for_report
from scrapers.base import age_match_score
from text_normalize import (
    deaccent as _deaccent_shared,
    location_score,
    phonetic_token,
    phonetic_token_set,
)

logger = logging.getLogger(__name__)
router = APIRouter()
settings = get_settings()

# In-memory conversation state: phone -> deque of message dicts
_conv_state: dict[str, deque] = defaultdict(lambda: deque(maxlen=20))

# Running accumulated report fields per phone (so the LLM never re-asks).
_collected: dict[str, dict] = defaultdict(dict)

# Phones we have already sent a proactive (pre-report) match preview to.
# Prevents spamming the same person on every turn before the form completes.
_proactive_searched: set[str] = set()

# Deduplication: msg_id -> timestamp. Clears entries older than 60s.
_seen_msg_ids: dict[str, float] = {}
_DEDUP_TTL = 60.0

# Human-readable labels for each scraper source. Shown to families so a match
# result reads "via Venezuela Reporta" instead of the raw "venezreporta" token.
SOURCE_LABELS: dict[str, str] = {
    "sos_laguaira": "SOS La Guaira",
    "venezreporta": "Venezuela Reporta",
    "sos_venezuela": "SOS Venezuela",
    "terremotove": "TerremotoVE",
    "pacientes_terremoto": "Pacientes en Hospitales",
    "localizados_venezuela": "Localizados Venezuela",
    "venezuela_te_busca": "Venezuela Te Busca",
    "reconexion": "Reconexión",
    "google_drive_hospital": "Directorio Hospitalario",
    "red_solidaria_venezuela": "Red Solidaria Venezuela",
    "hospitales_ve": "Hospitales VE",
    "redayuda_ve": "Red Ayuda VE",
    "tuayudave": "Tu Ayuda VE",
    "waha_whatsapp": "Reúne VE (WhatsApp)",
}


def _source_label(source: str) -> str:
    """Map a raw source token to a human-readable label."""
    return SOURCE_LABELS.get(source, source or "fuente externa")


def _format_match_line(m: dict) -> str:
    """Format one candidate as a WhatsApp bullet: name, age, location [Source]."""
    name = m.get("full_name") or "Desconocido"
    age = m.get("age") or "?"
    loc = m.get("last_seen_location") or "ubicación por confirmar"
    label = _source_label(m.get("source", ""))
    return f"• {name}, {age} años — {loc} [{label}]"


def _dedup_candidates(rows: list, limit: int = 3) -> list:
    """Collapse rows that are the same person (first OR last name token + location)."""
    seen_keys: set[str] = set()
    unique: list = []
    for m in rows:
        tokens = (m.get("full_name") or "").lower().split()
        loc_tok = re.sub(r"\s+", "", (m.get("last_seen_location") or "").lower())[:30]
        first_tok = tokens[0] if tokens else ""
        last_tok = tokens[-1] if tokens else ""
        key_first = f"{first_tok}|{loc_tok}"
        key_last = f"{last_tok}|{loc_tok}"
        if key_first in seen_keys or key_last in seen_keys:
            continue
        seen_keys.add(key_first)
        seen_keys.add(key_last)
        unique.append(m)
        if len(unique) >= limit:
            break
    return unique


try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_FUZZ = True
except ImportError:  # pragma: no cover
    _HAS_FUZZ = False

# A candidate must clear this NAME score before age is even considered. WRatio is
# too lenient (a single shared first name scores ~0.9), so we use token overlap +
# token-sort ratio: this rejects 'Ramirez Arantza' for 'Arantza Bastidas Dias'
# while keeping partial DB records like 'Arantza Bastidas'.
_NAME_FLOOR = 0.60


def _name_score(query: str, cand: str) -> float:
    """0..1 name similarity that requires real token overlap, not just one
    shared given name. Accent-insensitive, with a Spanish-phonetic channel so
    homophones (José/Hose, González/Gonsales) still match. Blends bidirectional
    token overlap with token-sort ratio."""
    q = _deaccent_shared(query)
    c = _deaccent_shared(cand)
    qt = [t for t in q.split() if len(t) >= 3]
    ct = [t for t in c.split() if len(t) >= 3]
    if not qt or not ct:
        return 0.0
    cand_phon = phonetic_token_set(cand)
    matched = 0
    for t in qt:
        fuzzy_hit = _HAS_FUZZ and any(_fuzz.ratio(t, u) >= 85 for u in ct)
        phon_hit = phonetic_token(t) in cand_phon          # homophone channel
        if fuzzy_hit or phon_hit or t in ct:
            matched += 1
    overlap = matched / min(len(qt), len(ct))      # fraction of smaller name matched
    tsr = (_fuzz.token_sort_ratio(q, c) / 100.0) if _HAS_FUZZ else overlap
    return 0.6 * overlap + 0.4 * tsr


def _rank_candidates(query_name: str, query_age, rows: list, query_location: str | None = None) -> list:
    """Drop candidates whose name doesn't really match, then rank survivors by
    name (dominant) + age proximity + location agreement (canonicalized, so
    'Vargas'/'Maiquetía'/'Litoral Central' all count as 'La Guaira')."""
    try:
        q_age = int(str(query_age).strip()) if query_age not in (None, "") else None
    except (ValueError, TypeError):
        q_age = None

    scored: list[tuple[float, dict]] = []
    for m in rows:
        cand_name = m.get("full_name") or ""
        if not cand_name:
            continue
        ns = _name_score(query_name, cand_name)
        if ns < _NAME_FLOOR:
            continue
        ag = age_match_score(q_age, m.get("age"))                       # 0..1, 0.5 if unknown
        loc = location_score(query_location, m.get("last_seen_location"))  # 0..1, 0.5 if unknown
        score = 0.7 * ns + 0.15 * ag + 0.15 * loc
        scored.append((score, {**m, "_score": round(score, 3)}))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored]


def _is_duplicate(msg_id: str) -> bool:
    now = time.monotonic()
    # Purge old entries
    expired = [k for k, t in _seen_msg_ids.items() if now - t > _DEDUP_TTL]
    for k in expired:
        _seen_msg_ids.pop(k, None)
    if msg_id in _seen_msg_ids:
        return True
    _seen_msg_ids[msg_id] = now
    return False

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
- Respuestas cortas (máx 2 líneas en WhatsApp).

FLUJO DE CONVERSACIÓN (crítico):
- Te paso "ESTADO ACTUAL" con lo que YA recolectaste. NUNCA vuelvas a preguntar un dato que ya está ahí.
- Pregunta UN solo dato faltante por mensaje, el más importante primero (nombre → relación/kind → ubicación → edad).
- En cuanto tengas `kind` + `name`, pon report_ready=true y confirma en una línea. No sigas pidiendo datos opcionales.
- Si el usuario dice "busco a X" o "a ella/él", eso ya define kind=missing. No preguntes "¿buscas o encontraste?" si ya lo dijo.
- No repitas la confirmación ni vuelvas a saludar.

Cuando el usuario reporte una persona, extrae estos campos:
  kind: "missing" (busca a alguien) | "found" (encontró a alguien)
  name: nombre completo (incluye TODOS los apellidos que mencione)
  age: edad aproximada (solo el número)
  gender: "F" (femenino) | "M" (masculino) | null si no se sabe. Infiere del nombre/pronombres si es claro (ej. "a ella", "mi hija" → F).
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
    "gender": null,
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


_FIELD_LABELS = {
    "kind": "tipo (busca/encontró)", "name": "nombre", "age": "edad",
    "gender": "género", "location": "ubicación", "description": "descripción",
    "contact": "contacto",
}


def _format_state(state: dict) -> str:
    known = [f"{_FIELD_LABELS.get(k, k)}={v}" for k, v in state.items()
             if v not in (None, "") and k in _FIELD_LABELS]
    if not known:
        return "ESTADO ACTUAL DEL REPORTE: (vacío, aún no hay datos)."
    return "ESTADO ACTUAL DEL REPORTE (no vuelvas a pedir estos): " + "; ".join(known)


async def _llm_extract(phone: str, new_message: str) -> dict:
    """Call Groq to extract report data and generate reply. Injects the running
    accumulated state so the LLM never re-asks known fields, and merges the new
    extraction into that state so downstream always sees the full report."""
    history = list(_conv_state[phone])
    state = _collected[phone]
    system = _SYSTEM_PROMPT + "\n\n" + _format_state(state)
    messages = [{"role": "system", "content": system}]
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
            result = json.loads(content)
    except Exception as exc:
        logger.error("LLM call failed for phone %s: %s", phone, exc)
        return {
            "reply": "Hubo un problema procesando tu mensaje. Intenta de nuevo en un momento.",
            "extracted": {"report_ready": False},
        }

    # Merge new non-null fields into the running state; downstream sees the union
    ext = result.get("extracted") or {}
    for k, v in ext.items():
        if k != "report_ready" and v not in (None, ""):
            _collected[phone][k] = v
    result["extracted"] = {**_collected[phone], "report_ready": bool(ext.get("report_ready"))}
    return result


async def _search_existing_matches(name: str, kind: str, exclude_id: str | None = None) -> list:
    """Recall step: pull reports (ANY kind) matching ANY name token — handles
    inverted order and partial surnames. Searches both kinds because the same
    person may be listed as missing (another searcher) or found (located);
    missing↔missing is a valid connection. Broad on purpose — _rank_candidates
    then scores by name similarity + age and drops weak hits."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    # Rank tokens longest-first (surnames/distinctive names beat short given names)
    tokens = sorted({t for t in name.strip().split() if len(t) >= 3}, key=len, reverse=True)
    if not tokens:
        return []

    seen_ids: set = set()
    results: list = []
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            for token in tokens[:3]:  # up to 3 most distinctive tokens
                params = {
                    "select": "id,full_name,age,last_seen_location,source,kind",
                    "full_name": f"ilike.*{token}*",
                    "limit": "15",
                    "order": "created_at.desc",
                }
                if exclude_id:
                    params["id"] = f"neq.{exclude_id}"
                r = await cl.get(
                    f"{sb}/rest/v1/reports",
                    headers={"apikey": key, "Authorization": f"Bearer {key}"},
                    params=params,
                )
                if r.status_code == 200:
                    for row in r.json():
                        if row["id"] not in seen_ids:
                            seen_ids.add(row["id"])
                            results.append(row)
                if len(results) >= 40:
                    break
    except Exception as exc:
        logger.error("search_matches: %s", exc)
    return results[:40]


async def _lookup_match_details(match_id: str, source_report_id: str) -> tuple:
    """Given a face match_id, return (name, location, source) of the OTHER
    report in the match (the matched person, not the one who sent the photo).
    Returns (None, None, None) on any failure."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    hdr = {"apikey": key, "Authorization": f"Bearer {key}"}
    try:
        async with httpx.AsyncClient(timeout=6) as cl:
            mr = await cl.get(
                f"{sb}/rest/v1/matches",
                headers=hdr,
                params={"id": f"eq.{match_id}", "select": "missing_id,found_id"},
            )
            if mr.status_code != 200 or not mr.json():
                return (None, None, None)
            row = mr.json()[0]
            # The matched person is whichever side is not the photo sender's report
            other_id = row["found_id"] if row.get("missing_id") == source_report_id else row["missing_id"]
            if not other_id:
                return (None, None, None)
            rr = await cl.get(
                f"{sb}/rest/v1/reports",
                headers=hdr,
                params={"id": f"eq.{other_id}", "select": "full_name,last_seen_location,source"},
            )
            if rr.status_code == 200 and rr.json():
                rep = rr.json()[0]
                return (rep.get("full_name"), rep.get("last_seen_location"), rep.get("source"))
    except Exception as exc:
        logger.warning("lookup_match_details failed: %s", exc)
    return (None, None, None)


async def _upsert_report(phone: str, data: dict, conv_key: str) -> str | None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    age_raw = data.get("age")
    try:
        age_int = int(str(age_raw).strip()) if age_raw else None
    except (ValueError, TypeError):
        age_int = None
    row = {
        "source": "waha_whatsapp",
        "source_url": f"waha:{conv_key}",
        "kind": data.get("kind") or "missing",
        "full_name": (data.get("name") or "").strip(),
        "age": age_int,
        "last_seen_location": data.get("location"),
        "distinguishing_marks": data.get("description"),
    }
    try:
        async with httpx.AsyncClient(timeout=10) as cl:
            resp = await cl.post(
                f"{sb}/rest/v1/reports",
                headers=_sb_headers(key),
                json=row,
                params={"on_conflict": "source,source_url"},
            )
            if resp.status_code in (200, 201):
                rows = resp.json()
                return rows[0]["id"] if rows else None
            logger.error("upsert_report %d: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        logger.error("upsert_report exception: %s", exc)
    return None


async def _register_subscriber(report_id: str, phone: str, data: dict) -> None:
    """Map this report_id to the reporter's phone so a background match found
    later can notify them. Upsert on report_id (one row per phone/report)."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    row = {
        "report_id": report_id,
        "phone": phone,
        "full_name": (data.get("name") or "").strip() or None,
        "kind": data.get("kind") or "missing",
    }
    try:
        async with httpx.AsyncClient(timeout=8) as cl:
            resp = await cl.post(
                f"{sb}/rest/v1/bot_subscribers",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=row,
                params={"on_conflict": "report_id"},
            )
            if resp.status_code not in (200, 201, 204):
                logger.warning("register_subscriber %d: %s", resp.status_code, resp.text[:120])
    except Exception as exc:
        logger.warning("register_subscriber failed: %s", exc)


async def _waha_send(phone: str, text: str) -> bool:
    """Send a WhatsApp text via WAHA. Returns True on success, False otherwise."""
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
                return False
            return True
    except Exception as exc:
        logger.error("waha_send to %s failed: %s", phone, exc)
        return False


def _extract_media_url(payload: dict) -> str:
    """WAHA's media URL location varies by engine/version. NOWEB nests it under
    payload.media.url; older/other shapes use a flat mediaUrl. Check all."""
    media = payload.get("media")
    if isinstance(media, dict) and media.get("url"):
        return media["url"]
    return payload.get("mediaUrl") or payload.get("mediaURL") or ""


async def _handle_message(payload: dict, app: Any) -> None:
    phone = payload.get("from", "")
    body = (payload.get("body") or "").strip()
    media_url = _extract_media_url(payload)
    has_media = payload.get("hasMedia", False) or bool(media_url)
    from_me = payload.get("fromMe", False)

    if from_me or not phone:
        return

    logger.info("WAHA message from %s: has_media=%s media_url=%s body=%s",
                phone, has_media, media_url or "None", body[:60])

    # Track message in conversation history
    if body:
        _conv_state[phone].append({"role": "user", "content": body})

    # Rewrite WAHA media URL host → internal docker hostname so the API can
    # reach it. WAHA reports files on its own public host (localhost/127.0.0.1).
    if media_url:
        waha_host = settings.waha_url.rstrip("/")
        for pub in ("http://localhost:3000", "https://localhost:3000",
                    "http://127.0.0.1:3000", "https://127.0.0.1:3000"):
            media_url = media_url.replace(pub, waha_host)

    # If photo received (always handle, even if media_url is missing)
    if has_media:
        _conv_state[phone].append({"role": "user", "content": "[envio una foto]"})
        face_match_found = False
        photo_analyzed = False

        if media_url:
            conv_key_photo = hashlib.md5(phone.encode()).hexdigest()[:12]
            sb = settings.supabase_url.rstrip("/")
            sb_key = settings.supabase_service_role_key
            try:
                async with httpx.AsyncClient(timeout=8) as cl:
                    r = await cl.get(
                        f"{sb}/rest/v1/reports",
                        headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"},
                        params={"source_url": f"eq.waha:{conv_key_photo}", "order": "created_at.desc", "limit": "1"},
                    )
                    rows = r.json() if r.status_code == 200 else []
                    if rows:
                        report_id = rows[0]["id"]
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
                        photo_analyzed = True
                        match_id = await process_photo_for_report(report_id, media_url, app)
                        if match_id:
                            found_name_hint, found_loc_hint, found_src_hint = \
                                await _lookup_match_details(match_id, report_id)
                            if found_loc_hint:
                                src_txt = f" (via {_source_label(found_src_hint)})" if found_src_hint else ""
                                face_reply = (
                                    f"Analicé la foto. Hay una posible coincidencia facial: "
                                    f"*{found_name_hint or 'persona registrada'}*, ubicación: "
                                    f"*{found_loc_hint}*{src_txt}. Reúne VE verificará y te contactará."
                                )
                            elif found_name_hint:
                                face_reply = (
                                    f"Analicé la foto. Hay una posible coincidencia facial: "
                                    f"*{found_name_hint}*, ubicación por confirmar. "
                                    "Reúne VE verificará y te contactará."
                                )
                            else:
                                face_reply = (
                                    "Analicé la foto. Hay una posible coincidencia facial en verificación. "
                                    "Reúne VE confirmará la ubicación y te contactará."
                                )
                            await _waha_send(phone, face_reply)
                            face_match_found = True
            except Exception as exc:
                logger.error("Photo handling error: %s", exc)

        if not face_match_found:
            if photo_analyzed:
                # Photo ran through face recognition but matched nothing yet.
                # Say so explicitly — otherwise it looks like nothing happened.
                msg = (
                    "Analicé la foto con reconocimiento facial y la guardé. "
                    "Por ahora no hay coincidencias visuales, pero la comparo "
                    "automáticamente con cada nuevo registro y te aviso si aparece."
                )
                _conv_state[phone].append({"role": "assistant", "content": msg})
                await _waha_send(phone, msg)
            else:
                # No report yet / couldn't fetch media — guide the user with Groq.
                photo_prompt = body if body else (
                    "El usuario envió una foto pero aún no hay un reporte. "
                    "Pídele de forma cálida el nombre y datos de la persona para poder buscarla."
                )
                result = await _llm_extract(phone, photo_prompt)
                photo_reply = result.get("reply", "Recibí la foto. Para buscar, dime el nombre de la persona.")
                _conv_state[phone].append({"role": "assistant", "content": photo_reply})
                await _waha_send(phone, photo_reply)

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
        kind = extracted.get("kind") or "missing"
        name = extracted.get("name", "")
        if report_id:
            logger.info("Report upserted from WAHA: %s (phone=%s)", report_id, phone)
            # Register phone↔report so a background match found later can notify them
            await _register_subscriber(report_id, phone, extracted)
            report_for_embed = {
                "full_name": name,
                "age": extracted.get("age"),
                "last_seen_location": extracted.get("location"),
                "distinguishing_marks": extracted.get("description"),
                "kind": kind,
            }
            asyncio.create_task(embed_and_match_report(report_id, report_for_embed, app))

            # Send the LLM confirmation, then the live DB search result
            if reply:
                await _waha_send(phone, reply)
            candidates = await _search_existing_matches(name, kind, exclude_id=report_id)
            ranked = _rank_candidates(name, extracted.get("age"), candidates, extracted.get("location"))
            unique = _dedup_candidates(ranked, limit=3)
            if unique:
                match_text = (
                    "Busqué en nuestra base y hay posibles coincidencias:\n"
                    + "\n".join(_format_match_line(m) for m in unique)
                    + "\nSon preliminares — el equipo Reúne VE los verificará."
                )
                await _waha_send(phone, match_text)
            else:
                await _waha_send(
                    phone,
                    "Busqué en nuestra base ahora mismo y no hay coincidencias aún. "
                    "Tu reporte queda activo — seguimos buscando y te avisamos si algo aparece."
                )
            # Reset conversation so the next message starts a fresh report
            _conv_state.pop(phone, None)
            _collected.pop(phone, None)
            _proactive_searched.discard(phone)
            return
        else:
            logger.error("Failed to upsert report for phone %s", phone)

    # Proactive search: a name was detected but the report isn't complete yet.
    # Fire once per phone so a panicked user gets an early preview without spam.
    extracted_name = (extracted.get("name") or "").strip()
    if extracted_name and len(extracted_name) >= 3 and phone not in _proactive_searched:
        _proactive_searched.add(phone)  # mark before await so it can't double-fire
        prov_kind = extracted.get("kind") or "missing"
        prov_candidates = await _search_existing_matches(extracted_name, prov_kind)
        prov_ranked = _rank_candidates(extracted_name, extracted.get("age"), prov_candidates, extracted.get("location"))
        prov_unique = _dedup_candidates(prov_ranked, limit=3)
        if prov_unique:
            if reply:
                await _waha_send(phone, reply)
            await _waha_send(
                phone,
                "Mientras registramos el reporte, encontré esto en nuestra base:\n"
                + "\n".join(_format_match_line(m) for m in prov_unique)
                + "\n¿Es alguna de estas personas?"
            )
            return

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
        msg_id = payload.get("id", "")
        if msg_id and _is_duplicate(msg_id):
            return {"ok": True}
        background_tasks.add_task(_handle_message, payload, request.app)

    return {"ok": True}
