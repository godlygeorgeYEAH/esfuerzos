import sqlite3

RECONEXION_DB_PATH = "reconexion.db"


def init_reconexion_db(db_path: str = RECONEXION_DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS personas (
            id                  TEXT PRIMARY KEY,
            nombre              TEXT,
            edad                INTEGER,
            ubicacion           TEXT,
            fecha               TEXT,
            descripcion         TEXT,
            contacto            TEXT,
            foto_url            TEXT,
            foto_local          TEXT,
            estado              TEXT,
            localizado_por      TEXT,
            localizado_contacto TEXT,
            localizado_relacion TEXT,
            localizado_nota     TEXT,
            reportada           INTEGER,
            reportes            INTEGER,
            created_at          INTEGER,
            updated_at          INTEGER,
            scraped_at          TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_reconexion_nombre ON personas(nombre);
        CREATE INDEX IF NOT EXISTS idx_reconexion_estado ON personas(estado);
        CREATE INDEX IF NOT EXISTS idx_reconexion_updated ON personas(updated_at);
    """)

    conn.commit()
    return conn
