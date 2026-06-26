import sqlite3
import logging

logger = logging.getLogger(__name__)


def cedula_exists(conn: sqlite3.Connection, cedula: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM cedulas WHERE cedula = ?", (cedula,)
    ).fetchone() is not None


def upsert_cedula(conn: sqlite3.Connection, data: dict) -> None:
    conn.execute(
        """
        INSERT INTO cedulas (
            cedula, rif, primer_apellido, segundo_apellido, nombres,
            estado, municipio, parroquia, centro_electoral,
            raw_html, status, scraped_at
        )
        VALUES (
            :cedula, :rif, :primer_apellido, :segundo_apellido, :nombres,
            :estado, :municipio, :parroquia, :centro_electoral,
            :raw_html, :status, :scraped_at
        )
        ON CONFLICT(cedula) DO UPDATE SET
            rif              = excluded.rif,
            primer_apellido  = excluded.primer_apellido,
            segundo_apellido = excluded.segundo_apellido,
            nombres          = excluded.nombres,
            estado           = excluded.estado,
            municipio        = excluded.municipio,
            parroquia        = excluded.parroquia,
            centro_electoral = excluded.centro_electoral,
            raw_html         = excluded.raw_html,
            status           = excluded.status,
            scraped_at       = excluded.scraped_at
        """,
        data,
    )
    conn.commit()


def get_max_cedula(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT MAX(cedula) FROM cedulas").fetchone()
    return row[0]


def count_cedulas(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM cedulas").fetchone()[0]


def count_found(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM cedulas WHERE status = 'found'"
    ).fetchone()[0]
