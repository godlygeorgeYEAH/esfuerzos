"""
Exporta todas las bases de datos a un único CSV unificado.

Uso:
    python scripts/export_all.py
    python scripts/export_all.py --out personas_unificadas.csv
    python scripts/export_all.py --exclude prueba test "sin nombre"
"""
import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

COLS = ["fuente", "nombre", "ubicacion", "estado", "contacto", "cedula", "edad", "extra"]


def _read_sos() -> list[dict]:
    if not Path("sos_personas.db").exists():
        return []
    conn = sqlite3.connect("sos_personas.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM sos_persons").fetchall()
    conn.close()
    result = []
    for r in rows:
        ubicacion = " / ".join(filter(None, [r["municipio"], r["parroquia"]]))
        result.append({
            "fuente":    "SOS Venezuela",
            "nombre":    r["display_name"] or "",
            "ubicacion": ubicacion,
            "estado":    r["status"] or "",
            "contacto":  "",
            "cedula":    r["cedula_masked"] or "",
            "edad":      "",
            "extra":     "",
        })
    return result


def _read_reconexion() -> list[dict]:
    if not Path("reconexion.db").exists():
        return []
    conn = sqlite3.connect("reconexion.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM personas").fetchall()
    conn.close()
    result = []
    for r in rows:
        extra_parts = []
        if r["localizado_por"]:
            extra_parts.append(f"Localizado por: {r['localizado_por']}")
        if r["localizado_nota"]:
            extra_parts.append(r["localizado_nota"])
        if r["descripcion"]:
            extra_parts.append(r["descripcion"])
        result.append({
            "fuente":    "Reconexión",
            "nombre":    r["nombre"] or "",
            "ubicacion": r["ubicacion"] or "",
            "estado":    r["estado"] or "",
            "contacto":  r["contacto"] or "",
            "cedula":    "",
            "edad":      str(r["edad"]) if r["edad"] else "",
            "extra":     " | ".join(extra_parts),
        })
    return result


def _read_pnp() -> list[dict]:
    if not Path("pnp_cedulas.db").exists():
        return []
    conn = sqlite3.connect("pnp_cedulas.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM cedulas WHERE status = 'found'"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        nombre = " ".join(filter(None, [
            r["primer_apellido"], r["segundo_apellido"], r["nombres"],
        ]))
        ubicacion = " / ".join(filter(None, [r["estado"], r["municipio"], r["parroquia"]]))
        result.append({
            "fuente":    "PNP / CNE",
            "nombre":    nombre,
            "ubicacion": ubicacion,
            "estado":    "",
            "contacto":  "",
            "cedula":    f"V-{r['cedula']}",
            "edad":      "",
            "extra":     r["centro_electoral"] or "",
        })
    return result


def _read_venezreporta() -> list[dict]:
    if not Path("venezreporta.db").exists():
        return []
    conn = sqlite3.connect("venezreporta.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM reportes").fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "fuente":    "VenezReporta",
            "nombre":    r["nombre"] or "",
            "ubicacion": r["ubicacion"] or "",
            "estado":    r["estado"] or "",
            "contacto":  "",
            "cedula":    "",
            "edad":      "",
            "extra":     r["detail_url"] or "",
        })
    return result


def _matches_keyword(nombre: str, keywords: list[str]) -> bool:
    lower = nombre.lower()
    return any(kw.lower() in lower for kw in keywords)


def export(out_path: str, exclude_keywords: list[str] | None = None) -> None:
    all_rows: list[dict] = []
    readers = [
        ("SOS Venezuela",  _read_sos),
        ("Reconexión",     _read_reconexion),
        ("PNP / CNE",      _read_pnp),
        ("VenezReporta",   _read_venezreporta),
    ]

    for name, fn in readers:
        rows = fn()
        print(f"{name}: {len(rows)} registros")
        all_rows.extend(rows)

    if exclude_keywords:
        before = len(all_rows)
        all_rows = [
            r for r in all_rows
            if not _matches_keyword(r["nombre"], exclude_keywords)
        ]
        removed = before - len(all_rows)
        print(f"Eliminados por keywords {exclude_keywords}: {removed} registros")

    print(f"\nTotal: {len(all_rows)} registros → {out_path}")

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLS)
        writer.writeheader()
        writer.writerows(all_rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta todas las DBs a CSV unificado")
    parser.add_argument("--out", default="personas_unificadas.csv",
                        help="Archivo de salida (default: personas_unificadas.csv)")
    parser.add_argument("--exclude", nargs="+", metavar="KEYWORD", default=None,
                        help="Eliminar filas cuyo nombre contenga alguna de estas palabras (sin distinción de mayúsculas)")
    args = parser.parse_args()
    export(args.out, exclude_keywords=args.exclude)
