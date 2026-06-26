import sqlite3

SOS_DB_PATH = "sos_personas.db"


def init_sos_db(db_path: str = SOS_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sos_persons (
            id              TEXT PRIMARY KEY,
            status          TEXT,
            cedula_masked   TEXT,
            display_name    TEXT,
            municipio       TEXT,
            parroquia       TEXT,
            photo_path      TEXT,
            photo_url       TEXT,
            photo_local     TEXT,
            source_date     TEXT,
            fecha_scraped   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sos_source_date
            ON sos_persons(source_date);
        CREATE INDEX IF NOT EXISTS idx_sos_status
            ON sos_persons(status);
        CREATE INDEX IF NOT EXISTS idx_sos_display_name
            ON sos_persons(display_name);
    """)

    conn.commit()
    return conn
