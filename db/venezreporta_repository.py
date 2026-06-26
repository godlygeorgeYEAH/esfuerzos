import sqlite3
import logging

logger = logging.getLogger(__name__)

_COLS = [
    "id", "nombre", "ubicacion", "estado",
    "foto_url", "foto_local", "detail_url", "scraped_at",
]


def reporte_exists(conn: sqlite3.Connection, reporte_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM reportes WHERE id = ?", (reporte_id,)
    ).fetchone() is not None


def upsert_reporte(conn: sqlite3.Connection, data: dict) -> None:
    placeholders = ", ".join("?" for _ in _COLS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in _COLS if c != "id")
    conn.execute(
        f"""
        INSERT INTO reportes ({', '.join(_COLS)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
        """,
        [data.get(c) for c in _COLS],
    )
    conn.commit()


def update_foto_local(conn: sqlite3.Connection, reporte_id: str, path: str) -> None:
    conn.execute("UPDATE reportes SET foto_local = ? WHERE id = ?", (path, reporte_id))
    conn.commit()


def count_reportes(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM reportes").fetchone()[0]


def get_all(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM reportes ORDER BY scraped_at").fetchall()
    return [dict(r) for r in rows]
