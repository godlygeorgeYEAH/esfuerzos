import sqlite3
import logging

logger = logging.getLogger(__name__)


def person_exists(conn: sqlite3.Connection, person_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sos_persons WHERE id = ?", (person_id,)
    ).fetchone() is not None


def upsert_person(conn: sqlite3.Connection, person: dict) -> bool:
    cols = [
        "id", "status", "cedula_masked", "display_name",
        "municipio", "parroquia", "photo_path", "photo_url",
        "photo_local", "source_date", "fecha_scraped",
    ]
    placeholders = ", ".join("?" for _ in cols)
    col_names = ", ".join(cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "id")

    conn.execute(
        f"""
        INSERT INTO sos_persons ({col_names})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
        """,
        [person.get(c) for c in cols],
    )
    conn.commit()
    return True


def update_person_photo(conn: sqlite3.Connection, person_id: str, local_path: str) -> None:
    conn.execute(
        "UPDATE sos_persons SET photo_local = ? WHERE id = ?",
        (local_path, person_id),
    )
    conn.commit()


def count_persons(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM sos_persons").fetchone()[0]


def get_all(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM sos_persons ORDER BY source_date DESC"
    ).fetchall()
    return [dict(r) for r in rows]
