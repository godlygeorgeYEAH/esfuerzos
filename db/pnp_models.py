import sqlite3

PNP_DB_PATH = "pnp_cedulas.db"


def init_pnp_db(db_path: str = PNP_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cedulas (
            cedula            INTEGER PRIMARY KEY,
            rif               TEXT,
            primer_apellido   TEXT,
            segundo_apellido  TEXT,
            nombres           TEXT,
            estado            TEXT,
            municipio         TEXT,
            parroquia         TEXT,
            centro_electoral  TEXT,
            raw_html          TEXT,
            status            TEXT,
            scraped_at        TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_cedulas_primer_apellido ON cedulas(primer_apellido);
        CREATE INDEX IF NOT EXISTS idx_cedulas_nombres         ON cedulas(nombres);
        CREATE INDEX IF NOT EXISTS idx_cedulas_status          ON cedulas(status);
    """)

    conn.commit()
    return conn
