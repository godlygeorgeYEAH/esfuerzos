import sqlite3

VENEZREPORTA_DB_PATH = "venezreporta.db"


def init_venezreporta_db(db_path: str = VENEZREPORTA_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reportes (
            id          TEXT PRIMARY KEY,
            nombre      TEXT,
            ubicacion   TEXT,
            estado      TEXT,
            foto_url    TEXT,
            foto_local  TEXT,
            detail_url  TEXT,
            scraped_at  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_vr_nombre ON reportes(nombre);
        CREATE INDEX IF NOT EXISTS idx_vr_estado ON reportes(estado);
    """)

    conn.commit()
    return conn
