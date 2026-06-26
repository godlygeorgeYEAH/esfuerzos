"""
Scraper para https://desaparecidos-terremoto-api.theempire.tech/api/personas

Modo dual (default): dos tareas en paralelo —
  [FULL]  Pagina todos los registros de la API hasta el final. Re-sweep cada hora.
  [POLL]  Revisa nuevas entradas cada --interval segundos desde page=1 hacia adelante.

Uso:
    python scripts/scrape_reconexion.py
    python scripts/scrape_reconexion.py --interval 30
    python scripts/scrape_reconexion.py --export personas.json
    python scripts/scrape_reconexion.py --export personas.csv
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
    from db.reconexion_models import init_reconexion_db, RECONEXION_DB_PATH
    from db.reconexion_repository import count_personas
    from scraper.reconexion_scraper import scrape_reconexion_dual

    conn = init_reconexion_db(RECONEXION_DB_PATH)
    logger.info("DB: %s | Personas al inicio: %d", RECONEXION_DB_PATH, count_personas(conn))

    try:
        await scrape_reconexion_dual(conn, poll_interval_secs=interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        total = count_personas(conn)
        logger.info("Detenido. Total personas en DB: %d", total)

        if export:
            _export(conn, export)

        conn.close()


def _export(conn, path: str) -> None:
    from db.reconexion_repository import get_all
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

    logger.info("Exportadas %d personas → %s", len(rows), path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper Reconexión — desaparecidos terremoto")
    parser.add_argument(
        "--interval", type=int, default=60,
        help="Segundos entre polls de nuevas entradas (default: 60)",
    )
    parser.add_argument(
        "--export", metavar="FILE", default=None,
        help="Exportar a JSON o CSV al detener (ej: personas.csv)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(args.interval, args.export))
    except KeyboardInterrupt:
        logger.info("Interrumpido.")
