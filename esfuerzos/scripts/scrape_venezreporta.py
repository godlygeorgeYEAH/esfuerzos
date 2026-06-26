"""
Scraper para https://venezuelareporta.org/buscar

Uso:
    python scripts/scrape_venezreporta.py
    python scripts/scrape_venezreporta.py --interval 30
    python scripts/scrape_venezreporta.py --export reportes.csv
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


async def run(interval: int, export: str | None) -> None:
    from db.venezreporta_models import init_venezreporta_db, VENEZREPORTA_DB_PATH
    from db.venezreporta_repository import count_reportes
    from scraper.venezreporta_scraper import scrape_venezreporta_dual

    conn = init_venezreporta_db(VENEZREPORTA_DB_PATH)
    logger.info("DB: %s | Reportes al inicio: %d", VENEZREPORTA_DB_PATH, count_reportes(conn))

    try:
        await scrape_venezreporta_dual(conn, poll_interval_secs=interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        total = count_reportes(conn)
        logger.info("Detenido. Total reportes en DB: %d", total)
        if export:
            _export(conn, export)
        conn.close()


def _export(conn, path: str) -> None:
    from db.venezreporta_repository import get_all
    rows = get_all(conn)
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

    logger.info("Exportados %d reportes → %s", len(rows), path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper VenezReporta — buscando/encontrado")
    parser.add_argument("--interval", type=int, default=60,
                        help="Segundos entre polls (default: 60)")
    parser.add_argument("--export", metavar="FILE", default=None,
                        help="Exportar a CSV o JSON al detener (ej: reportes.csv)")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.interval, args.export))
    except KeyboardInterrupt:
        logger.info("Interrumpido.")
