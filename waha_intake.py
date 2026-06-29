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
from datetime import datetime, timezone
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

# Per-REPORT dedup key (P0-b): a phone can report several people. Each active
# report gets a unique source_url so a second person does NOT overwrite the first.
# Rotated when a new person is detected (see _llm_extract reset).
_report_keys: dict[str, str] = {}

# Placeholder name for a FOUND person whose name is unknown (P0-a): rescuers /
# hospitals reporting unconscious/unidentified people — the highest-value case.
_UNIDENTIFIED = "No identificado"

# Phones we have already shown DB match results to (search once per report, not
# every turn). Cleared when the user starts reporting a different person.
_searched_shown: set[str] = set()

# Deduplication: msg_id -> timestamp. Clears entries older than 60s.
_seen_msg_ids: dict[str, float] = {}
_DEDUP_TTL = 60.0

# B1: per-phone rate limit (sliding window). Generous for a panicked reporter,
# blocks one phone from spamming Groq/DB. Self-purges old timestamps.
_phone_hits: dict[str, deque] = defaultdict(lambda: deque(maxlen=64))
_RATE_MAX = 20
_RATE_WINDOW = 60.0


_rate_calls = 0


def _phone_rate_limited(phone: str) -> bool:
    global _rate_calls
    now = time.monotonic()
    _rate_calls += 1
    if _rate_calls % 1000 == 0:  # B4: sweep idle phones so _phone_hits stays bounded
        for p in [p for p, dq in _phone_hits.items() if not dq or now - dq[-1] > _RATE_WINDOW]:
            _phone_hits.pop(p, None)
    dq = _phone_hits[phone]
    while dq and now - dq[0] > _RATE_WINDOW:
        dq.popleft()
    if len(dq) >= _RATE_MAX:
        return True
    dq.append(now)
    return False


# ---------------------------------------------------------------------------
# B3/B4: durable per-phone session (Supabase waha_sessions) so a restart never
# loses in-flight reports, and in-memory state is evicted after each message.
# ---------------------------------------------------------------------------
async def _load_session(phone: str) -> None:
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    try:
        async with httpx.AsyncClient(timeout=6) as cl:
            r = await cl.get(f"{sb}/rest/v1/waha_sessions",
                             headers={"apikey": key, "Authorization": f"Bearer {key}"},
                             params={"phone": f"eq.{phone}", "select": "state", "limit": "1"})
            rows = r.json() if r.status_code == 200 else []
    except Exception as exc:
        logger.warning("load_session: %s", exc)
        return
    if not rows:
        return
    st = rows[0].get("state") or {}
    dq = _conv_state[phone]
    dq.clear()
    for m in (st.get("conv") or [])[-20:]:
        dq.append(m)
    if st.get("collected"):
        _collected[phone] = dict(st["collected"])
    if st.get("rkey"):
        _report_keys[phone] = st["rkey"]
    if st.get("searched"):
        _searched_shown.add(phone)


async def _save_session(phone: str) -> bool:
    """Persist session to Supabase. Returns True on success. If the table is
    absent (migration 013 not applied), returns False → caller keeps in-memory
    state (graceful degradation to the pre-B3 behavior)."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    st = {
        "conv": list(_conv_state.get(phone, [])),
        "collected": _collected.get(phone, {}),
        "rkey": _report_keys.get(phone),
        "searched": phone in _searched_shown,
    }
    try:
        async with httpx.AsyncClient(timeout=6) as cl:
            resp = await cl.post(
                f"{sb}/rest/v1/waha_sessions",
                headers={"apikey": key, "Authorization": f"Bearer {key}",
                         "Content-Type": "application/json",
                         "Prefer": "resolution=merge-duplicates,return=minimal"},
                params={"on_conflict": "phone"},
                json={"phone": phone, "state": st,
                      "updated_at": datetime.now(timezone.utc).isoformat()},
            )
            if resp.status_code in (200, 201, 204):
                return True
            logger.warning("save_session %d: %s", resp.status_code, resp.text[:120])
    except Exception as exc:
        logger.warning("save_session: %s", exc)
    return False


def _evict_memory(phone: str) -> None:
    # State is now durable in Supabase; drop the in-memory copies so memory only
    # holds phones being actively processed (B4 — prevents unbounded growth/OOM).
    _conv_state.pop(phone, None)
    _collected.pop(phone, None)
    _report_keys.pop(phone, None)
    _searched_shown.discard(phone)

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


def _resolve_source_url(source: str, source_url: str | None) -> str | None:
    """Turn a stored source_url into a real, openable URL when we know how."""
    if not source_url:
        return None
    if source_url.startswith("http"):
        return source_url
    # source_url is "<scheme>:<id>" for several scrapers
    ident = source_url.split(":", 1)[1] if ":" in source_url else source_url
    if source == "venezuela_te_busca":
        return f"https://venezuelatebusca.com/?person={ident}"
    if source == "venezreporta":
        return "https://venezuelareporta.org"
    if source == "tuayudave":
        return "https://tuayudave.com"
    if source == "sos_laguaira":
        return "https://soslaguaira.lat"
    return None


def _hp(phone: str) -> str:
    """Hashed phone for logs — never log raw phone numbers (PII)."""
    return "ph_" + hashlib.sha256((phone or "").encode()).hexdigest()[:10]


def _report_key(phone: str) -> str:
    """Per-report dedup key (P0-b). Stable during one person's intake, unique per
    person, so one phone can report multiple relatives without overwriting."""
    if phone not in _report_keys:
        _report_keys[phone] = f"{hashlib.md5(phone.encode()).hexdigest()[:8]}:{uuid.uuid4().hex[:8]}"
    return _report_keys[phone]


def _format_match_line(m: dict) -> str:
    """Format one candidate: name, age, location [Source] + URL if resolvable."""
    name = m.get("full_name") or "Desconocido"
    age = m.get("age") or "?"
    loc = m.get("last_seen_location") or "ubicación por confirmar"
    label = _source_label(m.get("source", ""))
    line = f"• {name}, {age} años — {loc} [{label}]"
    url = _resolve_source_url(m.get("source", ""), m.get("source_url"))
    if url:
        line += f"\n  {url}"
    return line


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


def _same_person(a: str, b: str) -> bool:
    """True if two names plausibly refer to the SAME person: one is a token-subset
    of the other (a refinement like 'Maria Perez' → 'Maria Perez Gomez') or very
    high similarity. False for 'Maria Perez' vs 'Carlos Perez' (shared surname,
    different person) → triggers a new report (no overwrite)."""
    ta = {t for t in _deaccent_shared(a).split() if len(t) >= 3}
    tb = {t for t in _deaccent_shared(b).split() if len(t) >= 3}
    if not ta or not tb:
        return True  # can't tell → don't split
    if ta <= tb or tb <= ta:
        return True
    return _name_score(a, b) >= 0.85


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

# Outbound safety guard (audit blocker): the LLM reply is free text. A jailbreak
# or hallucination must NEVER tell a family someone died or assert a confirmed
# match. This deterministic filter runs on every LLM-derived reply before send.
_DEATH_TOKENS = ("fallec", "muert", "occiso", "difunt", "deceased", "deces")
_UNHEDGED_CONFIRM = ("encontram", "localizam", "es el mismo", "es la misma",
                     "es tu familiar", "lo encontr", "la encontr", "dado de baja")
_SAFE_HEDGE = ("Gracias por la información, la registré. Cualquier coincidencia es "
               "preliminar y la verifica el equipo Reúne VE antes de confirmarte nada.")


def _sanitize_reply(text: str) -> str:
    """Replace any LLM reply that states death or an unhedged confirmation with a
    safe hedge. Conservative: false hope / false grief is the worst-case harm."""
    if not text:
        return text
    d = _deaccent_shared(text)
    if any(t in d for t in _DEATH_TOKENS):
        return _SAFE_HEDGE
    if "posible" not in d and any(t in d for t in _UNHEDGED_CONFIRM):
        return _SAFE_HEDGE
    return text


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
- PERSONA ENCONTRADA SIN NOMBRE (ej. "un señor sin identificar", "paciente no identificada"): NO insistas en el nombre. Deja name vacío, pide ubicación y descripción (o foto), y con kind="found" + (ubicación o descripción) pon report_ready=true.
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

`report_ready` = true cuando tienes kind + name; O cuando kind="found" + ubicación/descripción aunque no haya nombre (persona no identificada)."""


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
        logger.error("LLM call failed for phone %s: %s", _hp(phone), exc)
        return {
            "reply": "Hubo un problema procesando tu mensaje. Intenta de nuevo en un momento.",
            "extracted": {"report_ready": False},
        }

    # Merge new non-null fields into the running state; downstream sees the union
    ext = result.get("extracted") or {}
    # New-person detection: start fresh unless the new name is the SAME person.
    # Same = one name's tokens are a subset of the other (a refinement) or very
    # high similarity. This catches 'Maria Perez' → 'Carlos Perez' (shared
    # surname, different person) which a plain score threshold missed.
    new_name = (ext.get("name") or "").strip()
    cur_name = (_collected[phone].get("name") or "").strip()
    if new_name and cur_name and not _same_person(new_name, cur_name):
        _collected[phone].clear()
        _searched_shown.discard(phone)
        _report_keys.pop(phone, None)  # new person → new report key (P0-b)
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
                    "select": "id,full_name,age,last_seen_location,source,source_url,kind",
                    "full_name": f"ilike.*{token}*",
                    # F7/V6: never surface other families' PRIVATE WhatsApp reports to
                    # a stranger. Only public scraped sources are shown as candidates.
                    "source": "neq.waha_whatsapp",
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


# Minimum face similarity to DISCLOSE an identity synchronously (P0-c). Matches
# are still recorded for human review at the lower FACE_MATCH_THRESHOLD (0.50),
# but the bot only reveals a name/location inline above this stricter bar.
_FACE_DISCLOSE_THRESHOLD = 0.65


async def _lookup_match_details(match_id: str, source_report_id: str) -> dict:
    """Details of the OTHER report in a face match, ONLY when safe to disclose
    synchronously (P0-c): face_score >= threshold AND the matched record is a
    PUBLIC source (never reveal another family's private WhatsApp report to a
    stranger). Returns {} when disclosure is not allowed → caller sends a generic
    'in verification' message instead."""
    sb = settings.supabase_url.rstrip("/")
    key = settings.supabase_service_role_key
    hdr = {"apikey": key, "Authorization": f"Bearer {key}"}
    try:
        async with httpx.AsyncClient(timeout=6) as cl:
            mr = await cl.get(
                f"{sb}/rest/v1/matches",
                headers=hdr,
                params={"id": f"eq.{match_id}", "select": "missing_id,found_id,face_score"},
            )
            if mr.status_code != 200 or not mr.json():
                return {}
            row = mr.json()[0]
            if float(row.get("face_score") or 0) < _FACE_DISCLOSE_THRESHOLD:
                return {}  # too weak to disclose inline; leave to human review
            other_id = row["found_id"] if row.get("missing_id") == source_report_id else row["missing_id"]
            if not other_id:
                return {}
            rr = await cl.get(
                f"{sb}/rest/v1/reports",
                headers=hdr,
                params={"id": f"eq.{other_id}", "select": "full_name,last_seen_location,source,source_url"},
            )
            if rr.status_code == 200 and rr.json():
                rep = rr.json()[0]
                # F7 on the FACE path: never reveal a private WhatsApp report.
                if rep.get("source") == "waha_whatsapp":
                    return {}
                return {
                    "name": rep.get("full_name"),
                    "location": rep.get("last_seen_location"),
                    "source": rep.get("source"),
                    "url": _resolve_source_url(rep.get("source", ""), rep.get("source_url")),
                }
    except Exception as exc:
        logger.warning("lookup_match_details failed: %s", exc)
    return {}


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
        logger.error("waha_send to %s failed: %s", _hp(phone), exc)
        return False


def _extract_media_url(payload: dict) -> str:
    """WAHA's media URL location varies by engine/version. NOWEB nests it under
    payload.media.url; older/other shapes use a flat mediaUrl. Check all."""
    media = payload.get("media")
    if isinstance(media, dict) and media.get("url"):
        return media["url"]
    return payload.get("mediaUrl") or payload.get("mediaURL") or ""


async def _handle_photo(phone: str, media_url: str, report_id: str | None,
                        app: Any, has_body: bool) -> None:
    """Attach a photo to the report and run face matching. B2: report_id is the
    report just created from this message's caption (if any); falls back to the
    phone's active report key for photo-after-report. Sends its own reply."""
    _conv_state[phone].append({"role": "user", "content": "[envio una foto]"})
    if not media_url:
        if not has_body:
            await _waha_send(phone, "Recibí la foto pero no pude descargarla. ¿Puedes reenviarla?")
        return
    sb = settings.supabase_url.rstrip("/")
    sb_key = settings.supabase_service_role_key
    hdr = {"apikey": sb_key, "Authorization": f"Bearer {sb_key}"}
    rid = report_id
    try:
        async with httpx.AsyncClient(timeout=8) as cl:
            if not rid:  # photo without a freshly-created report → active report
                r = await cl.get(f"{sb}/rest/v1/reports", headers=hdr,
                                 params={"source_url": f"eq.waha:{_report_key(phone)}",
                                         "order": "created_at.desc", "limit": "1"})
                rows = r.json() if r.status_code == 200 else []
                rid = rows[0]["id"] if rows else None
            if rid:
                await cl.post(
                    f"{sb}/rest/v1/photos",
                    headers={**hdr, "Content-Type": "application/json",
                             "Prefer": "resolution=ignore-duplicates,return=minimal"},
                    json={"id": str(uuid.uuid4()), "report_id": rid, "storage_path": media_url},
                )
        if rid:
            match_id = await process_photo_for_report(rid, media_url, app)
            if match_id:
                d = await _lookup_match_details(match_id, rid)
                if d.get("name") or d.get("location"):
                    nm = d.get("name") or "persona registrada"
                    src_txt = f" (via {_source_label(d['source'])})" if d.get("source") else ""
                    url_txt = f"\n{d['url']}" if d.get("url") else ""
                    locp = f", ubicación: *{d['location']}*" if d.get("location") else ", ubicación por confirmar"
                    await _waha_send(
                        phone,
                        f"Analicé la foto. Hay una *posible* coincidencia facial: *{nm}*{locp}{src_txt}.{url_txt}\n"
                        "En verificación — Reúne VE confirmará y te contactará.")
                else:
                    await _waha_send(
                        phone,
                        "Analicé la foto. Hay una posible coincidencia en verificación. "
                        "El equipo Reúne VE la revisará y te contactará si se confirma.")
            else:
                await _waha_send(
                    phone,
                    "Analicé la foto con reconocimiento facial y la guardé. Por ahora no hay "
                    "coincidencias visuales, pero la comparo con cada nuevo registro y te aviso si aparece.")
            return
    except Exception as exc:
        logger.error("Photo handling error: %s", exc)
    if not has_body:
        await _waha_send(phone, "Recibí la foto. Para buscarla, dime el nombre o los datos de la persona.")


async def _handle_message(payload: dict, app: Any) -> None:
    phone = payload.get("from", "")
    if payload.get("fromMe", False) or not phone:
        return
    # B1: per-phone rate limit — drop abusive bursts from a single number.
    if _phone_rate_limited(phone):
        logger.warning("rate-limited phone %s (>%d/%ds)", _hp(phone), _RATE_MAX, int(_RATE_WINDOW))
        return
    # B3: hydrate durable session → process → persist + evict from memory (B4).
    await _load_session(phone)
    try:
        await _process_message(payload, phone, app)
    finally:
        # Evict from memory ONLY if the state was durably saved (B4). If the
        # waha_sessions table isn't there yet, keep in-memory (degrade safely).
        if await _save_session(phone):
            _evict_memory(phone)


async def _process_message(payload: dict, phone: str, app: Any) -> None:
    body = (payload.get("body") or "").strip()
    media_url = _extract_media_url(payload)
    has_media = payload.get("hasMedia", False) or bool(media_url)

    logger.info("WAHA message from %s: has_media=%s media_url=%s body_len=%d",
                _hp(phone), has_media, media_url or "None", len(body))

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

    # B2: extract from the caption/text and create the report FIRST, so a photo
    # in the SAME message attaches to it. Media is handled after, below.
    extracted: dict = {}
    reply = ""
    if body:
        result = await _llm_extract(phone, body)
        reply = _sanitize_reply(result.get("reply", ""))  # outbound safety guard
        extracted = result.get("extracted", {})
        if reply:
            _conv_state[phone].append({"role": "assistant", "content": reply})

    name = (extracted.get("name") or "").strip()
    kind = extracted.get("kind") or "missing"
    loc = extracted.get("location")
    desc = extracted.get("description")

    # P0-a: a FOUND person may be UNIDENTIFIED (no name) — rescuer/hospital
    # reporting an unconscious person. Register with a placeholder; matching then
    # relies on photo + location, not name. This is the highest-value case.
    found_unident = (extracted.get("kind") == "found") and (not name) and bool(loc or desc)
    effective_name = name or (_UNIDENTIFIED if found_unident else "")

    # Register / update the report. P0-b: per-report key (unique per person), so a
    # phone reporting several relatives never overwrites a prior one.
    report_id = None
    if extracted.get("kind") and (name or found_unident):
        conv_key = _report_key(phone)
        upsert_data = {**extracted, "name": effective_name}
        report_id = await _upsert_report(phone, upsert_data, conv_key)
        if report_id:
            logger.info("Report upserted from WAHA: %s (phone=%s)", report_id, _hp(phone))
            await _register_subscriber(report_id, phone, upsert_data)
            asyncio.create_task(embed_and_match_report(report_id, {
                "full_name": effective_name,
                "age": extracted.get("age"),
                "last_seen_location": loc,
                "distinguishing_marks": desc,
                "kind": kind,
            }, app))

    # B2: handle any photo now that the report (if any) exists, so a lead-with-photo
    # message attaches the image to the report just created from its caption.
    if has_media:
        await _handle_photo(phone, media_url, report_id, app, bool(body))
        if not body:
            return

    # Name search only for REAL names. An unidentified found person can't be
    # text-searched usefully; it relies on the face + background cross-match.
    have_enough = bool(name) and bool(loc or extracted.get("age"))
    if have_enough and phone not in _searched_shown:
        _searched_shown.add(phone)
        if reply:
            await _waha_send(phone, reply)
        candidates = await _search_existing_matches(name, kind, exclude_id=report_id)
        ranked = _rank_candidates(name, extracted.get("age"), candidates, extracted.get("location"))
        unique = _dedup_candidates(ranked, limit=3)
        if unique:
            await _waha_send(
                phone,
                "Busqué en nuestra base y hay posibles coincidencias:\n"
                + "\n".join(_format_match_line(m) for m in unique)
                + "\nSon preliminares — el equipo Reúne VE los verificará."
            )
        else:
            await _waha_send(
                phone,
                "Busqué en nuestra base y no hay coincidencias claras aún. "
                "Tu reporte queda activo — te avisamos si algo aparece."
            )
        return

    if reply:
        await _waha_send(phone, reply)


@router.post("/webhook/waha")
async def waha_webhook(request: Request, background_tasks: BackgroundTasks) -> dict:
    """Receive WAHA message events."""
    raw = await request.body()

    # B5/F1/V1 — WAHA webhook HMAC. WAHA signs the RAW body with HMAC-SHA512 (hex)
    # and sends it in the X-Webhook-Hmac header (algorithm always sha512). When
    # WAHA_WEBHOOK_SECRET is set this is fail-closed (bad/missing signature → 401).
    # Activation: set WAHA_WEBHOOK_SECRET here AND WHATSAPP_HOOK_HMAC_KEY (same
    # value) on the WAHA container. Until set, the host firewall (F3) is the active
    # protection (webhook reachable only from the internal docker network).
    if settings.waha_webhook_secret:
        sig = request.headers.get("x-webhook-hmac", "")
        expected = hmac.new(
            settings.waha_webhook_secret.encode(), raw, hashlib.sha512
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
