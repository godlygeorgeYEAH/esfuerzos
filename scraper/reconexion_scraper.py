"""
Scraper para https://desaparecidos-terremoto-api.theempire.tech/api/personas

Dos tareas concurrentes:
  [FULL]  Pagina desde page=1 hasta agotar resultados. Re-sweep cada hora.
  [POLL]  Revisa nuevas entradas cada N segundos desde page=1 hacia adelante.

Arquitectura: pipeline productor-consumidor.
  - Productor:   fetcha _FETCH_CONCURRENCY páginas en paralelo, mete items en queue.
  - Consumidores: _PROCESS_CONCURRENCY workers procesan items mientras el productor
                  ya está jalando el siguiente bloque.
"""
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from db.reconexion_repository import persona_exists, upsert_persona, update_foto_local

logger = logging.getLogger(__name__)

API_URL              = "https://desaparecidos-terremoto-api.theempire.tech/api/personas"
IMAGES_DIR           = "reconexion_images"
PAGE_SIZE            = 100  # items por página
_FETCH_CONCURRENCY   = 3    # páginas en paralelo (bajo para no saturar la API)
_PROCESS_CONCURRENCY = 20   # workers procesando items simultáneamente
_PHOTO_CONCURRENCY   = 10   # foto-downloads simultáneos
_FULL_RESWEEP        = 3600
_MAX_IMG_BYTES       = 10 * 1024 * 1024
_QUEUE_SIZE          = 2000  # buffer máximo de items pendientes
_MAX_RETRIES         = 6     # reintentos por página antes de rendirse

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; ReconexionScraper/1.0)",
}

# Semáforo global para foto-downloads — se inicializa en scrape_reconexion_dual
_photo_sem: asyncio.Semaphore | None = None


# ─────────────────────── UTILS ───────────────────────────────────────

def _normalize(raw: dict, scraped_at: str) -> dict:
    foto_url = raw.get("foto") or None
    if foto_url == "":
        foto_url = None
    return {
        "id":                   raw.get("id"),
        "nombre":               raw.get("nombre"),
        "edad":                 raw.get("edad"),
        "ubicacion":            raw.get("ubicacion"),
        "fecha":                raw.get("fecha"),
        "descripcion":          raw.get("descripcion") or None,
        "contacto":             raw.get("contacto"),
        "foto_url":             foto_url,
        "foto_local":           None,
        "estado":               raw.get("estado"),
        "localizado_por":       raw.get("localizadoPor"),
        "localizado_contacto":  raw.get("localizadoContacto"),
        "localizado_relacion":  raw.get("localizadoRelacion"),
        "localizado_nota":      raw.get("localizadoNota"),
        "reportada":            int(raw.get("reportada") or 0),
        "reportes":             raw.get("reportes") or 0,
        "created_at":           raw.get("createdAt"),
        "updated_at":           raw.get("updatedAt"),
        "scraped_at":           scraped_at,
    }


async def _download_foto(
    session: aiohttp.ClientSession,
    persona_id: str,
    foto_url: str,
) -> str | None:
    ext  = Path(foto_url.split("?")[0]).suffix or ".jpg"
    path = Path(IMAGES_DIR) / persona_id / f"{persona_id}{ext}"
    if path.exists():
        return str(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with _photo_sem:
            async with session.get(foto_url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
        if len(data) > _MAX_IMG_BYTES:
            return None
        path.write_bytes(data)
        return str(path)
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None


async def _fetch_page(session: aiohttp.ClientSession, page: int) -> list[dict]:
    params = {"page": page, "pageSize": PAGE_SIZE}
    delay = 2.0
    for attempt in range(_MAX_RETRIES):
        try:
            async with session.get(
                API_URL, params=params, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 429:
                    wait = float(resp.headers.get("Retry-After", delay))
                    wait = max(wait, delay)
                    logger.warning(
                        "rate-limit página %d → esperando %.0fs (intento %d/%d)",
                        page, wait, attempt + 1, _MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    delay = min(delay * 2, 60)
                    continue
                resp.raise_for_status()
                body = await resp.json()
                return body.get("items") or []
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt < _MAX_RETRIES - 1:
                logger.warning(
                    "Error página %d (intento %d/%d): %s — reintentando en %.0fs",
                    page, attempt + 1, _MAX_RETRIES, exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error("Página %d — agotados %d intentos: %s", page, _MAX_RETRIES, exc)
    return []


async def _process_persona(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    raw: dict,
    label: str,
) -> bool:
    pid = raw.get("id")
    if not pid:
        return False

    is_new = not persona_exists(conn, pid)
    now    = datetime.now(timezone.utc).isoformat()
    data   = _normalize(raw, now)

    # Siempre upsert: captura cambios de estado (desaparecido → localizado, etc.)
    upsert_persona(conn, data)

    if is_new:
        logger.info(
            "[%s] +persona [%s] %s | %s | %s",
            label, pid[:8],
            raw.get("nombre") or "?",
            raw.get("ubicacion") or "—",
            raw.get("estado") or "—",
        )
        if data["foto_url"]:
            local = await _download_foto(session, pid, data["foto_url"])
            if local:
                update_foto_local(conn, pid, local)
                logger.debug("[%s]   ↳ foto → %s", label, local)

    return is_new


# ─────────────────────── PIPELINE ────────────────────────────────────

_SENTINEL = object()


async def _producer_full(
    session: aiohttp.ClientSession,
    queue: asyncio.Queue,
) -> int:
    """Fetcha todas las páginas y las vierte en la queue. Retorna total de items."""
    page = 1
    total = 0
    while True:
        pages_data = await asyncio.gather(*[
            _fetch_page(session, page + i) for i in range(_FETCH_CONCURRENCY)
        ])

        done = False
        for page_items in pages_data:
            if not page_items:
                done = True
                break
            for item in page_items:
                await queue.put(item)
            total += len(page_items)
            if len(page_items) < PAGE_SIZE:
                done = True
                break

        if done:
            break
        page += _FETCH_CONCURRENCY

    return total


async def _producer_poll(
    session: aiohttp.ClientSession,
    queue: asyncio.Queue,
) -> int:
    """Como _producer_full pero para POLL: solo jala hasta que no haya nuevos."""
    page = 1
    total = 0
    while True:
        pages_data = await asyncio.gather(*[
            _fetch_page(session, page + i) for i in range(_FETCH_CONCURRENCY)
        ])

        done = False
        batch_items: list[dict] = []
        for page_items in pages_data:
            if not page_items:
                done = True
                break
            batch_items.extend(page_items)
            if len(page_items) < PAGE_SIZE:
                done = True
                break

        for item in batch_items:
            await queue.put(item)
        total += len(batch_items)

        if done or not batch_items:
            break
        page += _FETCH_CONCURRENCY

    return total


async def _run_pipeline(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    label: str,
    poll_mode: bool = False,
) -> int:
    """
    Lanza productor + _PROCESS_CONCURRENCY consumidores en paralelo.
    Retorna cantidad de personas nuevas insertadas.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_SIZE)
    total_new = 0
    lock = asyncio.Lock()

    async def consumer():
        nonlocal total_new
        while True:
            item = await queue.get()
            if item is _SENTINEL:
                queue.task_done()
                break
            added = await _process_persona(session, conn, item, label)
            if added:
                async with lock:
                    total_new += 1
            queue.task_done()

    producer_fn = _producer_poll if poll_mode else _producer_full
    consumers = [asyncio.create_task(consumer()) for _ in range(_PROCESS_CONCURRENCY)]

    fetched = await producer_fn(session, queue)

    # Señal de fin para cada consumer
    for _ in range(_PROCESS_CONCURRENCY):
        await queue.put(_SENTINEL)

    await asyncio.gather(*consumers)
    logger.debug("[%s] Fetched %d items → %d nuevos", label, fetched, total_new)
    return total_new


# ─────────────────────── TASK: FULL SWEEP ────────────────────────────

async def _task_full_sweep(conn: sqlite3.Connection) -> None:
    sweep = 0
    async with aiohttp.ClientSession() as session:
        while True:
            sweep += 1
            logger.info("[FULL] Sweep %d iniciando", sweep)
            total_new = await _run_pipeline(session, conn, "FULL")
            logger.info(
                "[FULL] Sweep %d completado — %d nuevas. Re-sweep en %ds.",
                sweep, total_new, _FULL_RESWEEP,
            )
            await asyncio.sleep(_FULL_RESWEEP)


# ─────────────────────── TASK: POLL ──────────────────────────────────

async def _task_poll(conn: sqlite3.Connection, interval_secs: int) -> None:
    cycle = 0
    async with aiohttp.ClientSession() as session:
        while True:
            cycle += 1
            total_new = await _run_pipeline(session, conn, "POLL", poll_mode=True)
            if total_new:
                logger.info("[POLL] Ciclo %d: %d nuevas personas", cycle, total_new)
            else:
                logger.debug("[POLL] Ciclo %d: sin novedades", cycle)
            await asyncio.sleep(interval_secs)


# ─────────────────────── ENTRY POINT ─────────────────────────────────

async def scrape_reconexion_dual(
    conn: sqlite3.Connection,
    poll_interval_secs: int = 60,
) -> None:
    global _photo_sem
    _photo_sem = asyncio.Semaphore(_PHOTO_CONCURRENCY)

    logger.info(
        "Reconexión scraper iniciado → pageSize=%d | fetch=%d | process=%d | foto=%d | [POLL] cada %ds",
        PAGE_SIZE, _FETCH_CONCURRENCY, _PROCESS_CONCURRENCY, _PHOTO_CONCURRENCY, poll_interval_secs,
    )
    task_full = asyncio.create_task(_task_full_sweep(conn))
    task_poll = asyncio.create_task(_task_poll(conn, poll_interval_secs))
    try:
        await asyncio.gather(task_full, task_poll)
    finally:
        task_full.cancel()
        task_poll.cancel()
        await asyncio.gather(task_full, task_poll, return_exceptions=True)
