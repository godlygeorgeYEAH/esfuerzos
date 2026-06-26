import sqlite3
import logging

logger = logging.getLogger(__name__)

_COLS = [
    "id", "nombre", "edad", "ubicacion", "fecha", "descripcion",
    "contacto", "foto_url", "foto_local", "estado",
    "localizado_por", "localizado_contacto", "localizado_relacion", "localizado_nota",
    "reportada", "reportes", "created_at", "updated_at", "scraped_at",
]


def persona_exists(conn: sqlite3.Connection, persona_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM personas WHERE id = ?", (persona_id,)
    ).fetchone() is not None


def upsert_persona(conn: sqlite3.Connection, data: dict) -> None:
    placeholders = ", ".join("?" for _ in _COLS)
    updates = ", ".join(f"{c} = excluded.{c}" for c in _COLS if c != "id")
    conn.execute(
        f"""
        INSERT INTO personas ({', '.join(_COLS)})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
        """,
        [data.get(c) for c in _COLS],
    )
    conn.commit()


def update_foto_local(conn: sqlite3.Connection, persona_id: str, path: str) -> None:
    conn.execute("UPDATE personas SET foto_local = ? WHERE id = ?", (path, persona_id))
    conn.commit()


def count_personas(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM personas").fetchone()[0]


def get_max_updated_at(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(updated_at) FROM personas").fetchone()
    return row[0] or 0


def get_all(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM personas ORDER BY created_at").fetchall()
    return [dict(r) for r in rows]
