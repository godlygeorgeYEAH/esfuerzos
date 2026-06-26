"""
Bot de WhatsApp via WAHA — búsqueda de personas en reconexion.db

Recibe mensajes de cualquier número, busca el texto como nombre en reconexion.db
con coincidencia aproximada, y responde con los resultados por WhatsApp.

Requisitos:
    pip install fastapi uvicorn rapidfuzz aiohttp python-dotenv

Variables de entorno (.env):
    WAHA_URL        URL del servidor WAHA    (default: http://localhost:3000)
    WAHA_SESSION    Nombre de sesión WAHA    (default: default)
    WAHA_API_KEY    API key WAHA (opcional)

Uso:
    python scripts/waha_bot.py
    python scripts/waha_bot.py --port 8000 --threshold 75

Configura el webhook en WAHA apuntando a:
    http://<ip-del-servidor>:<puerto>/webhook
    WHATSAPP_HOOK_EVENTS=message
"""
import argparse
import asyncio
import logging
import os
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import aiohttp
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

WAHA_URL     = os.getenv("WAHA_URL", "http://localhost:3000")
WAHA_SESSION = os.getenv("WAHA_SESSION", "default")
WAHA_API_KEY = os.getenv("WAHA_API_KEY", "")

RECONEXION_DB_PATH = "reconexion.db"

_THRESHOLD   = 75
_MAX_RESULTS = 5
_MIN_QUERY   = 3


# ──────────────────────── BÚSQUEDA ───────────────────────────────────────────

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s.lower().strip())
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _search(query: str) -> list[dict]:
    if not Path(RECONEXION_DB_PATH).exists():
        logger.warning("reconexion.db no encontrada en %s", Path(RECONEXION_DB_PATH).absolute())
        return []

    conn = sqlite3.connect(RECONEXION_DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM personas WHERE nombre IS NOT NULL"
    ).fetchall()
    conn.close()

    candidates  = {r["id"]: _norm(r["nombre"]) for r in rows}
    rows_by_id  = {r["id"]: dict(r) for r in rows}

    matches = process.extract(
        _norm(query), candidates,
        scorer=fuzz.token_sort_ratio,
        limit=_MAX_RESULTS,
        score_cutoff=_THRESHOLD,
    )

    results = []
    for _val, score, key in matches:
        r = rows_by_id[key]
        r["_score"] = score
        results.append(r)
    return results


# ──────────────────────── FORMATO ────────────────────────────────────────────

def _fmt_persona(r: dict) -> str:
    lines = [f"*{r['nombre']}* — {r['_score']}% de coincidencia"]
    if r.get("estado"):
        lines.append(f"Estado: {r['estado']}")
    if r.get("edad"):
        lines.append(f"Edad: {r['edad']}")
    if r.get("ubicacion"):
        lines.append(f"Ubicación: {r['ubicacion']}")
    if r.get("contacto"):
        lines.append(f"Contacto: {r['contacto']}")
    if r.get("localizado_por"):
        lines.append(f"Localizado por: {r['localizado_por']}")
    if r.get("localizado_contacto"):
        lines.append(f"Contacto localizado: {r['localizado_contacto']}")
    if r.get("descripcion"):
        lines.append(f"Descripción: {r['descripcion']}")
    if r.get("foto_url"):
        lines.append(f"Foto: {r['foto_url']}")
    return "\n".join(lines)


def _build_reply(query: str, results: list[dict]) -> str:
    if not results:
        return (
            f"No encontré resultados para *{query}*.\n\n"
            "Intenta con el nombre completo o verifica la ortografía."
        )

    header = f"Encontré {len(results)} resultado(s) para *{query}*:\n\n"
    blocks = [_fmt_persona(r) for r in results]
    return header + "\n\n─────────────\n\n".join(blocks)


# ──────────────────────── WAHA CLIENT ────────────────────────────────────────

async def _send_message(chat_id: str, text: str) -> None:
    url = f"{WAHA_URL}/api/sendText"
    headers = {"Content-Type": "application/json"}
    if WAHA_API_KEY:
        headers["X-Api-Key"] = WAHA_API_KEY

    payload = {"session": WAHA_SESSION, "chatId": chat_id, "text": text}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    logger.warning("WAHA %d: %s", resp.status, body[:200])
    except Exception as exc:
        logger.error("Error enviando mensaje a %s: %s", chat_id, exc)


# ──────────────────────── FASTAPI APP ────────────────────────────────────────

app = FastAPI(title="Crisis WhatsApp Bot")


@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False}, status_code=400)

    if body.get("event") != "message":
        return JSONResponse({"ok": True})

    payload = body.get("payload", {})

    # Ignorar mensajes propios, con media o de grupos
    if payload.get("fromMe", False):
        return JSONResponse({"ok": True})
    if payload.get("hasMedia", False):
        return JSONResponse({"ok": True})

    chat_id = payload.get("from", "")
    text    = (payload.get("body") or "").strip()

    if not text or "@g.us" in chat_id or len(text) < _MIN_QUERY:
        return JSONResponse({"ok": True})

    logger.info("Consulta de %s: %s", chat_id, text[:80])

    loop    = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, _search, text)
    reply   = _build_reply(text, results)
    await _send_message(chat_id, reply)

    return JSONResponse({"ok": True})


@app.get("/health")
async def health():
    return {
        "ok": True,
        "db_exists": Path(RECONEXION_DB_PATH).exists(),
        "db_path":   str(Path(RECONEXION_DB_PATH).absolute()),
    }


# ──────────────────────── ENTRY POINT ────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WAHA WhatsApp bot — búsqueda Reconexión")
    parser.add_argument("--host",      default="0.0.0.0",     help="Host (default: 0.0.0.0)")
    parser.add_argument("--port",      type=int, default=8000, help="Puerto (default: 8000)")
    parser.add_argument("--threshold", type=int, default=75,   help="Umbral de coincidencia 0-100 (default: 75)")
    parser.add_argument("--log-level", default="info",         help="Nivel de log (default: info)")
    args = parser.parse_args()

    _THRESHOLD = args.threshold

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info(
        "WAHA bot iniciando → %s:%d | threshold: %d%% | WAHA: %s [sesión: %s]",
        args.host, args.port, _THRESHOLD, WAHA_URL, WAHA_SESSION,
    )

    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
