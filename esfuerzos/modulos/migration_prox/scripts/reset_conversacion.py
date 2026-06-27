"""
Elimina todas las conversaciones de un número de teléfono en la base SQLite local.
Útil para resetear el estado del bot durante testing.

Uso (dentro del contenedor):
    python scripts/reset_conversacion.py 584121234567

Uso (desde el host, directorio migration_prox/):
    docker compose exec bot python scripts/reset_conversacion.py 584121234567
"""
import sys
import sqlite3
from pathlib import Path

DB_PATH = Path("/data/reune.db")


def reset_conversacion(phone: str) -> None:
    if not DB_PATH.exists():
        print(f"ERROR: No se encontró la base de datos en {DB_PATH}")
        sys.exit(1)

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT id FROM conversaciones WHERE client_phone = ?", (phone,)
        )
        conv_ids = [row[0] for row in cur.fetchall()]

        if not conv_ids:
            print(f"No se encontraron conversaciones para {phone!r}")
            return

        placeholders = ",".join("?" * len(conv_ids))

        cur.execute(
            f"DELETE FROM mensajes_conversacion WHERE conversacion_id IN ({placeholders})",
            conv_ids,
        )
        mensajes = cur.rowcount

        cur.execute(
            f"DELETE FROM eventos_conversacion WHERE conversacion_id IN ({placeholders})",
            conv_ids,
        )
        eventos = cur.rowcount

        cur.execute(
            "DELETE FROM conversaciones WHERE client_phone = ?", (phone,)
        )
        convs = cur.rowcount

        conn.commit()

    print(f"Reseteado {phone!r}: {convs} conversacion(es), {mensajes} mensaje(s), {eventos} evento(s) eliminados.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python scripts/reset_conversacion.py <phone>")
        print("Ejemplo: python scripts/reset_conversacion.py 584121234567")
        sys.exit(1)

    reset_conversacion(sys.argv[1])
