"""
Scraper para https://venezuelareporta.org/buscar

Sitio Next.js con SSR — el HTML ya viene renderizado del servidor.
Pagina por status=buscando y status=encontrado hasta agotar resultados.

Dos tareas concurrentes:
  [FULL]  Barre todos los status/páginas. Re-sweep cada hora.
  [POLL]  Revisa page=1 de cada status cada N segundos.
"""
import asyncio
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from db.venezreporta_repository import reporte_exists, upsert_reporte, update_foto_local

logger = logging.getLogger(__name__)

BASE_URL        = "https://venezuelareporta.org"
BUSCAR_URL      = f"{BASE_URL}/buscar"
IMAGES_DIR      = "venezreporta_images"
STATUSES        = ["buscando", "encontrado"]
_FETCH_CONCURRENCY  = 3
_PROCESS_CONCURRENCY = 20
_PHOTO_CONCURRENCY  = 10
_FULL_RESWEEP       = 3600
_MAX_IMG_BYTES      = 10 * 1024 * 1024
_MAX_RETRIES        = 6
_QUEUE_SIZE         = 2000

_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

_photo_sem: asyncio.Semaphore | None = None

# UUID regex
_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# ─────────────────────── PARSING ─────────────────────────────────────

def _parse_page(html: str) -> list[dict]:
    """
    Extrae tarjetas de personas del HTML renderizado por Next.js.

    Cada tarjeta es un <a href="/reporte/{uuid}"> con:
      - <img alt="Foto de {nombre}" src="{foto_url}">
      - <h3>{nombre}</h3>
      - <p class="...text-ink-soft">{ubicacion}</p>
      - <span class="chip bg-buscando-soft ...">Se busca</span>
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for card in soup.find_all("a", href=re.compile(r"^/reporte/")):
        href = card.get("href", "")
        m = _UUID_RE.search(href)
        if not m:
            continue
        uid = m.group(0)

        # Nombre: desde el alt de la imagen (más fiable que el h3 con truncado)
        img = card.find("img")
        nombre = None
        foto_url = None
        if img:
            alt = img.get("alt", "")
            if alt.startswith("Foto de "):
                nombre = alt[len("Foto de "):]
            foto_url = img.get("src") or None

        # Fallback: h3
        if not nombre:
            h3 = card.find("h3")
            if h3:
                nombre = h3.get_text(strip=True) or None

        # Ubicación: primer <p> con text-ink-soft
        ubicacion = None
        for p in card.find_all("p"):
            cls = " ".join(p.get("class", []))
            if "text-ink-soft" in cls:
                ubicacion = p.get_text(strip=True) or None
                break

        # Estado: chip span
        estado = None
        chip = card.find("span", class_=re.compile(r"chip"))
        if chip:
            estado = chip.get_text(strip=True) or None

        if not uid or not nombre:
            continue

        items.append({
            "id":         uid,
            "nombre":     nombre,
            "ubicacion":  ubicacion,
            "estado":     estado,
            "foto_url":   foto_url,
            "detail_url": f"{BASE_URL}/reporte/{uid}",
        })

    return items


# ─────────────────────── HTTP ─────────────────────────────────────────

async def _fetch_page(
    session: aiohttp.ClientSession,
    status: str,
    page: int,
) -> list[dict]:
    params = {"status": status, "page": page}
    delay = 2.0
    for attempt in range(_MAX_RETRIES):
        try:
            async with session.get(
                BUSCAR_URL, params=params, headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=True,
            ) as resp:
                if resp.status == 429:
                    wait = float(resp.headers.get("Retry-After", delay))
                    wait = max(wait, delay)
                    logger.warning(
                        "rate-limit [%s] page %d → esperando %.0fs (intento %d/%d)",
                        status, page, wait, attempt + 1, _MAX_RETRIES,
                    )
                    await asyncio.sleep(wait)
                    delay = min(delay * 2, 60)
                    continue
                if resp.status != 200:
                    logger.warning("[%s] page %d → HTTP %d", status, page, resp.status)
                    return []
                html = await resp.text()
                items = _parse_page(html)
                logger.debug("[%s] page %d → %d tarjetas", status, page, len(items))
                return items
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt < _MAX_RETRIES - 1:
                logger.warning(
                    "Error [%s] page %d (intento %d/%d): %s — reintentando en %.0fs",
                    status, page, attempt + 1, _MAX_RETRIES, exc, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error("[%s] page %d — agotados %d intentos: %s", status, page, _MAX_RETRIES, exc)
    return []


async def _download_foto(
    session: aiohttp.ClientSession,
    reporte_id: str,
    foto_url: str,
) -> str | None:
    ext  = Path(foto_url.split("?")[0]).suffix or ".jpg"
    path = Path(IMAGES_DIR) / reporte_id / f"{reporte_id}{ext}"
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


# ─────────────────────── PROCESS ─────────────────────────────────────

async def _process_reporte(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    raw: dict,
    label: str,
) -> bool:
    uid = raw.get("id")
    if not uid:
        return False

    is_new = not reporte_exists(conn, uid)
    now    = datetime.now(timezone.utc).isoformat()

    upsert_reporte(conn, {**raw, "foto_local": None, "scraped_at": now})

    if is_new:
        logger.info(
            "[%s] +reporte [%s] %s | %s | %s",
            label, uid[:8],
            raw.get("nombre") or "?",
            raw.get("ubicacion") or "—",
            raw.get("estado") or "—",
        )
        if raw.get("foto_url"):
            local = await _download_foto(session, uid, raw["foto_url"])
            if local:
                update_foto_local(conn, uid, local)
                logger.debug("[%s]   ↳ foto → %s", label, local)

    return is_new


# ─────────────────────── PIPELINE ────────────────────────────────────

_SENTINEL = object()


async def _run_pipeline(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    label: str,
    poll_mode: bool = False,
) -> int:
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
            added = await _process_reporte(session, conn, item, label)
            if added:
                async with lock:
                    total_new += 1
            queue.task_done()

    consumers = [asyncio.create_task(consumer()) for _ in range(_PROCESS_CONCURRENCY)]

    # Productor: itera status × páginas
    total_fetched = 0
    for status in STATUSES:
        page = 1
        while True:
            batch_pages = await asyncio.gather(*[
                _fetch_page(session, status, page + i)
                for i in range(_FETCH_CONCURRENCY)
            ])

            done = False
            for page_items in batch_pages:
                if not page_items:
                    done = True
                    break
                for item in page_items:
                    item["estado"] = item.get("estado") or status
                    await queue.put(item)
                total_fetched += len(page_items)

                if poll_mode and page_items:
                    # En modo poll paramos cuando ya no haya nuevos
                    pass

            if done:
                break
            page += _FETCH_CONCURRENCY

    for _ in range(_PROCESS_CONCURRENCY):
        await queue.put(_SENTINEL)

    await asyncio.gather(*consumers)
    logger.debug("[%s] Fetched %d items → %d nuevos", label, total_fetched, total_new)
    return total_new


# ─────────────────────── TASKS ───────────────────────────────────────

async def _task_full_sweep(conn: sqlite3.Connection) -> None:
    sweep = 0
    async with aiohttp.ClientSession() as session:
        while True:
            sweep += 1
            logger.info("[FULL] Sweep %d iniciando — statuses: %s", sweep, STATUSES)
            total_new = await _run_pipeline(session, conn, "FULL")
            logger.info(
                "[FULL] Sweep %d completado — %d nuevos. Re-sweep en %ds.",
                sweep, total_new, _FULL_RESWEEP,
            )
            await asyncio.sleep(_FULL_RESWEEP)


async def _task_poll(conn: sqlite3.Connection, interval_secs: int) -> None:
    cycle = 0
    async with aiohttp.ClientSession() as session:
        while True:
            cycle += 1
            total_new = await _run_pipeline(session, conn, "POLL", poll_mode=True)
            if total_new:
                logger.info("[POLL] Ciclo %d: %d nuevos reportes", cycle, total_new)
            else:
                logger.debug("[POLL] Ciclo %d: sin novedades", cycle)
            await asyncio.sleep(interval_secs)


# ─────────────────────── ENTRY POINT ─────────────────────────────────

async def scrape_venezreporta_dual(
    conn: sqlite3.Connection,
    poll_interval_secs: int = 60,
) -> None:
    global _photo_sem
    _photo_sem = asyncio.Semaphore(_PHOTO_CONCURRENCY)

    logger.info(
        "VenezReporta scraper iniciado → statuses=%s | fetch=%d | process=%d | [POLL] cada %ds",
        STATUSES, _FETCH_CONCURRENCY, _PROCESS_CONCURRENCY, poll_interval_secs,
    )
    task_full = asyncio.create_task(_task_full_sweep(conn))
    task_poll = asyncio.create_task(_task_poll(conn, poll_interval_secs))
    try:
        await asyncio.gather(task_full, task_poll)
    finally:
        task_full.cancel()
        task_poll.cancel()
        await asyncio.gather(task_full, task_poll, return_exceptions=True)
