"""
Scraper para la API de SOS Venezuela 2026 — personas desaparecidas/buscadas.

Dos tareas concurrentes:
  [FULL]  Pagina desde offset=0 hasta agotar los datos. Re-sweep cada hora.
  [POLL]  Revisa nuevas entradas cada N segundos.

Ambas tareas fetean _CONCURRENCY páginas en paralelo por batch y procesan
todas las personas de un batch concurrentemente (I/O de fotos en paralelo).
"""
import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from db.sos_repository import person_exists, upsert_person, update_person_photo

logger = logging.getLogger(__name__)

API_BASE = "https://sosvenezuela2026.com/api/persons/list"
SUPABASE_BASE = (
    "https://ihcnbvkwkiyxlkhuwapu.supabase.co"
    "/storage/v1/object/public/fotos-desaparecidos/"
)
SOS_IMAGES_DIR = "sos_images"

_PAGE_SIZE   = 100
_CONCURRENCY = 5      # páginas simultáneas por batch
_FULL_RESWEEP = 3600  # re-sweep completo cada 1 hora
_MAX_SIZE_BYTES = 10 * 1024 * 1024

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; SOSVenezuelaScraper/1.0)",
}


# ─────────────────────── UTILS ───────────────────────────────────────

def _build_photo_url(photo_path: str) -> str:
    if photo_path.startswith("http"):
        return photo_path
    return SUPABASE_BASE + photo_path


def _local_photo_path(person_id: str, photo_url: str) -> Path:
    ext = Path(photo_url.split("?")[0]).suffix or ".webp"
    return Path(SOS_IMAGES_DIR) / person_id / f"{person_id}{ext}"


async def _download_photo(
    session: aiohttp.ClientSession,
    person_id: str,
    photo_url: str,
) -> str | None:
    local_path = _local_photo_path(person_id, photo_url)
    if local_path.exists():
        return str(local_path)

    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with session.get(
            photo_url, headers=_HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                logger.debug("Photo HTTP %d for %s", resp.status, person_id)
                return None
            data = await resp.read()
            if len(data) > _MAX_SIZE_BYTES:
                return None
            local_path.write_bytes(data)
            return str(local_path)
    except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError) as exc:
        logger.warning("Photo download failed for %s: %s", person_id, exc)
        return None


async def _fetch_page(session: aiohttp.ClientSession, offset: int) -> list[dict]:
    url = f"{API_BASE}?offset={offset}&limit={_PAGE_SIZE}"
    try:
        async with session.get(
            url, headers=_HEADERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as exc:
        logger.warning("API fetch error at offset %d: %s", offset, exc)
        return []


async def _fetch_batch(
    session: aiohttp.ClientSession,
    base_offset: int,
) -> list[tuple[int, list[dict]]]:
    """Fetch _CONCURRENCY pages starting at base_offset, all in parallel."""
    offsets = [base_offset + i * _PAGE_SIZE for i in range(_CONCURRENCY)]
    pages = await asyncio.gather(*[_fetch_page(session, o) for o in offsets])
    return list(zip(offsets, pages))


async def _process_person(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    raw: dict,
    label: str,
) -> bool:
    """Persist + download photo for one person. Returns True if new."""
    pid = raw.get("id")
    if not pid or person_exists(conn, pid):
        return False

    photo_path = raw.get("photo_path") or ""
    photo_url = _build_photo_url(photo_path) if photo_path else None

    upsert_person(conn, {
        "id": pid,
        "status": raw.get("status"),
        "cedula_masked": raw.get("cedula_masked"),
        "display_name": raw.get("display_name"),
        "municipio": raw.get("municipio"),
        "parroquia": raw.get("parroquia"),
        "photo_path": photo_path,
        "photo_url": photo_url,
        "photo_local": None,
        "source_date": raw.get("source_date"),
        "fecha_scraped": datetime.now(timezone.utc).isoformat(),
    })

    logger.info(
        "[%s] +persona [%s] %s | %s",
        label, pid[:8],
        raw.get("display_name") or "?",
        raw.get("parroquia") or "—",
    )

    if photo_url:
        local = await _download_photo(session, pid, photo_url)
        if local:
            update_person_photo(conn, pid, local)
            logger.info("[%s]   ↳ foto → %s", label, local)

    return True


async def _process_batch_concurrent(
    session: aiohttp.ClientSession,
    conn: sqlite3.Connection,
    persons: list[dict],
    label: str,
) -> int:
    """Process all persons in a batch concurrently. Returns count of new ones."""
    results = await asyncio.gather(*[
        _process_person(session, conn, raw, label)
        for raw in persons
    ])
    return sum(results)


# ─────────────────────── TASK: FULL SWEEP ────────────────────────────

async def _task_full_sweep(conn: sqlite3.Connection) -> None:
    sweep = 0
    async with aiohttp.ClientSession() as session:
        while True:
            sweep += 1
            base_offset = 0
            total_new = 0
            logger.info("[FULL] Sweep %d — fetching %d páginas por batch", sweep, _CONCURRENCY)

            while True:
                batch = await _fetch_batch(session, base_offset)
                logger.debug(
                    "[FULL] Batch offsets %d–%d fetched",
                    base_offset, base_offset + (_CONCURRENCY - 1) * _PAGE_SIZE,
                )

                # Collect persons from all pages; stop collecting at first empty page
                persons: list[dict] = []
                reached_end = False
                for offset, page in batch:
                    if not page:
                        reached_end = True
                        break
                    persons.extend(page)
                    if len(page) < _PAGE_SIZE:
                        reached_end = True
                        break

                if persons:
                    new = await _process_batch_concurrent(session, conn, persons, "FULL")
                    total_new += new
                    logger.info(
                        "[FULL] Batch offset %d → %d personas, %d nuevas",
                        base_offset, len(persons), new,
                    )

                if reached_end:
                    logger.info(
                        "[FULL] Sweep %d completado — %d nuevas | último offset: %d",
                        sweep, total_new, base_offset,
                    )
                    break

                base_offset += _CONCURRENCY * _PAGE_SIZE

            logger.info("[FULL] Re-sweep en %ds.", _FULL_RESWEEP)
            await asyncio.sleep(_FULL_RESWEEP)


# ─────────────────────── TASK: POLL NUEVOS ───────────────────────────

async def _task_poll_new(conn: sqlite3.Connection, interval_secs: int) -> None:
    """
    Cada interval_secs, fetea batches de _CONCURRENCY páginas en paralelo
    desde offset=0 hacia adelante. Para cuando un batch completo no tenga
    ninguna entrada nueva (datos conocidos alcanzados).
    """
    cycle = 0
    async with aiohttp.ClientSession() as session:
        while True:
            cycle += 1
            base_offset = 0
            total_new = 0
            logger.info("[POLL] Ciclo %d — buscando nuevas entradas", cycle)

            while True:
                batch = await _fetch_batch(session, base_offset)

                persons: list[dict] = []
                reached_end = False
                for offset, page in batch:
                    if not page:
                        reached_end = True
                        break
                    persons.extend(page)
                    if len(page) < _PAGE_SIZE:
                        reached_end = True
                        break

                if not persons:
                    break

                new = await _process_batch_concurrent(session, conn, persons, "POLL")
                total_new += new

                logger.debug(
                    "[POLL] Ciclo %d offset %d → %d personas, %d nuevas",
                    cycle, base_offset, len(persons), new,
                )

                # Si el batch entero no tuvo nada nuevo, alcanzamos datos conocidos
                if new == 0 or reached_end:
                    break

                base_offset += _CONCURRENCY * _PAGE_SIZE

            if total_new:
                logger.info("[POLL] Ciclo %d: %d nuevas personas", cycle, total_new)
            else:
                logger.debug("[POLL] Ciclo %d: sin novedades", cycle)

            await asyncio.sleep(interval_secs)


# ─────────────────────── ENTRY POINT DUAL ────────────────────────────

async def scrape_sos_dual(
    conn: sqlite3.Connection,
    poll_interval_secs: int = 60,
) -> None:
    """
    Dos tareas concurrentes:
      [FULL] Barre toda la API en batches de _CONCURRENCY páginas paralelas.
      [POLL] Revisa nuevas entradas cada poll_interval_secs segundos.
    """
    logger.info(
        "SOS API scraper iniciado → concurrencia: %d páginas/batch | [POLL] cada %ds",
        _CONCURRENCY, poll_interval_secs,
    )
    task_full = asyncio.create_task(_task_full_sweep(conn))
    task_poll = asyncio.create_task(_task_poll_new(conn, poll_interval_secs))
    try:
        await asyncio.gather(task_full, task_poll)
    finally:
        task_full.cancel()
        task_poll.cancel()
        await asyncio.gather(task_full, task_poll, return_exceptions=True)
