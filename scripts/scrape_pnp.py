"""
Scraper de cédulas venezolanas — https://www.sistemaspnp.com/cedula/

Itera desde --start hasta --end consultando una cédula por request.
Resuelve automáticamente el CAPTCHA aritmético de cada página.
Guarda resultados en SQLite (pnp_cedulas.db) y puede exportar a CSV/JSON.

Uso:
    python scripts/scrape_pnp.py
    python scripts/scrape_pnp.py --start 10000 --end 1000000
    python scripts/scrape_pnp.py --workers 20
    python scripts/scrape_pnp.py --export cedulas.csv
"""
import argparse
import asyncio
import csv
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


async def run(start: int, end: int, workers: int, export: str | None, headed: bool, proxies_file: str | None) -> None:
    from db.pnp_models import init_pnp_db, PNP_DB_PATH
    from db.pnp_repository import count_cedulas, count_found
    from scraper.pnp_scraper import scrape_pnp, load_proxies, _WORKERS
    import scraper.pnp_scraper as _mod

    proxies = None
    if proxies_file:
        proxies = load_proxies(proxies_file)
        if not proxies:
            logger.error("No se encontraron proxies en %s", proxies_file)
            return
        logger.info("Proxies en pool: %d | Workers concurrentes: %d", len(proxies), workers)
    _mod._WORKERS = workers

    conn = init_pnp_db(PNP_DB_PATH)
    logger.info(
        "DB: %s | Cédulas en DB: %d | Encontradas: %d",
        PNP_DB_PATH, count_cedulas(conn), count_found(conn),
    )

    try:
        await scrape_pnp(conn, start=start, end=end, headless=not headed, proxies=proxies)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        logger.info(
            "Detenido. Cédulas en DB: %d | Encontradas: %d",
            count_cedulas(conn), count_found(conn),
        )
        if export:
            _export(conn, export)
        conn.close()


def _export(conn, path: str) -> None:
    rows = conn.execute(
        "SELECT cedula, nombre, status, scraped_at FROM cedulas ORDER BY cedula"
    ).fetchall()
    rows = [dict(r) for r in rows]

    if not rows:
        logger.info("Sin datos para exportar.")
        return

    p = Path(path)
    if p.suffix.lower() == ".csv":
        with open(p, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        p.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Exportadas %d cédulas → %s", len(rows), path)


if __name__ == "__main__":
    from scraper.pnp_scraper import START_CEDULA, END_CEDULA, _WORKERS

    parser = argparse.ArgumentParser(description="Scraper cédulas PNP Venezuela")
    parser.add_argument("--start",   type=int, default=START_CEDULA,
                        help=f"Cédula inicial (default: {START_CEDULA})")
    parser.add_argument("--end",     type=int, default=END_CEDULA,
                        help=f"Cédula final (default: {END_CEDULA})")
    parser.add_argument("--workers", type=int, default=_WORKERS,
                        help=f"Workers concurrentes (default: {_WORKERS})")
    parser.add_argument("--export",  metavar="FILE", default=None,
                        help="Exportar a CSV o JSON al detener (ej: cedulas.csv)")
    parser.add_argument("--headed", action="store_true",
                        help="Mostrar ventana del browser (no headless)")
    parser.add_argument("--proxies", metavar="FILE", default=None,
                        help="Archivo con proxies (una por línea). Cada proxy = 1 worker.")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.start, args.end, args.workers, args.export, args.headed, args.proxies))

    except KeyboardInterrupt:
        logger.info("Interrumpido.")
