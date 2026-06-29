"""
buscar_nombre.py — CLI para probar la cadena de matching de nombres contra la BD.

Uso — nombre único:
    python buscar_nombre.py "José Rodriguez"
    python buscar_nombre.py "Anahys" --edad 32 --ubicacion "La Guaira"

Uso — lista desde archivo (un nombre por línea):
    python buscar_nombre.py --archivo input.txt
    python buscar_nombre.py --archivo input.txt --ubicacion "La Guaira"

Salida: output.csv con los top-2 matches por nombre.
Requiere SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY en el entorno o en .env
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys

import httpx

# ---------------------------------------------------------------------------
# Cargar .env si existe (sin dependencia de python-dotenv)
# ---------------------------------------------------------------------------
_ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_ENV_FILE):
    with open(_ENV_FILE) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _, _v = _line.partition("=")
                os.environ.setdefault(_k.strip(), _v.strip())

# ---------------------------------------------------------------------------
# Importar lógica de scoring del proyecto
# ---------------------------------------------------------------------------
try:
    from text_normalize import (
        deaccent as _deaccent,
        location_score,
        phonetic_token,
        phonetic_token_set,
    )
except ImportError as e:
    sys.exit(f"[ERROR] No se pudo importar text_normalize.py: {e}\n"
             f"Ejecutá el script desde el directorio raíz del proyecto.")

try:
    from scrapers.base import age_match_score
except ImportError as e:
    sys.exit(f"[ERROR] No se pudo importar scrapers/base.py: {e}")

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_FUZZ = True
except ImportError:
    _HAS_FUZZ = False

_NAME_FLOOR = 0.60
_RECALL_LIMIT = 2
_CSV_COLUMNS = [
    "nombre_input",
    "nombre_match",
    "score",
    "name_s",
    "age_s",
    "loc_s",
    "tipo",
    "edad",
    "ubicacion",
    "fuente",
]


# ---------------------------------------------------------------------------
# Scoring (replicado de waha_intake.py para no arrastrar FastAPI)
# ---------------------------------------------------------------------------

def _name_score(query: str, cand: str) -> float:
    q = _deaccent(query)
    c = _deaccent(cand)
    qt = [t for t in q.split() if len(t) >= 3]
    ct = [t for t in c.split() if len(t) >= 3]
    if not qt or not ct:
        return 0.0
    cand_phon = phonetic_token_set(cand)
    matched = 0
    for t in qt:
        fuzzy_hit = _HAS_FUZZ and any(_fuzz.ratio(t, u) >= 85 for u in ct)
        phon_hit = phonetic_token(t) in cand_phon
        if fuzzy_hit or phon_hit or t in ct:
            matched += 1
    overlap = matched / min(len(qt), len(ct))
    tsr = (_fuzz.token_sort_ratio(q, c) / 100.0) if _HAS_FUZZ else overlap
    return 0.6 * overlap + 0.4 * tsr


def _rank_candidates(query_name: str, query_age, rows: list, query_location: str | None = None) -> list:
    try:
        q_age = int(str(query_age).strip()) if query_age not in (None, "") else None
    except (ValueError, TypeError):
        q_age = None

    scored: list[tuple[float, dict]] = []
    for m in rows:
        cand_name = m.get("full_name") or ""
        if not cand_name:
            continue
        ns = _name_score(query_name, cand_name)
        if ns < _NAME_FLOOR:
            continue
        ag = age_match_score(q_age, m.get("age"))
        loc = location_score(query_location, m.get("last_seen_location"))
        score = 0.7 * ns + 0.15 * ag + 0.15 * loc
        scored.append((score, {
            **m,
            "_score":  round(score, 3),
            "_name_s": round(ns, 3),
            "_age_s":  round(ag, 3),
            "_loc_s":  round(loc, 3),
        }))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored]


# ---------------------------------------------------------------------------
# Recall: ILIKE por token contra Supabase
# ---------------------------------------------------------------------------

async def _recall(name: str) -> list:
    sb_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    sb_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

    if not sb_url or not sb_key:
        sys.exit("[ERROR] Faltan SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY en el entorno.")

    headers = {
        "apikey": sb_key,
        "Authorization": f"Bearer {sb_key}",
    }

    tokens = sorted(
        {t for t in name.strip().split() if len(t) >= 3},
        key=len,
        reverse=True,
    )
    if not tokens:
        return []

    seen_ids: set = set()
    results: list = []

    async with httpx.AsyncClient(timeout=10) as cl:
        for token in tokens[:3]:
            r = await cl.get(
                f"{sb_url}/rest/v1/reports",
                headers=headers,
                params={
                    "select": "id,full_name,age,last_seen_location,source,kind",
                    "full_name": f"ilike.*{token}*",
                    "limit": str(_RECALL_LIMIT),
                    "order": "created_at.desc",
                },
            )
            if r.status_code != 200:
                print(f"  [WARN] Supabase {r.status_code} para token '{token}'", file=sys.stderr)
                continue
            for row in r.json():
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    results.append(row)

    return results


# ---------------------------------------------------------------------------
# Procesar un nombre → lista de filas CSV
# ---------------------------------------------------------------------------

async def _run_one(query_name: str, query_age, query_location: str | None) -> list[dict]:
    print(f"  → Recall...", end=" ", flush=True)
    candidates = await _recall(query_name)
    print(f"{len(candidates)} candidatos  |  Scoring...", end=" ", flush=True)

    ranked = _rank_candidates(query_name, query_age, candidates, query_location)
    top = ranked[:_RECALL_LIMIT]
    print(f"{len(ranked)} matches  |  top {len(top)} al CSV")

    if not top:
        return [{"nombre_input": query_name, "nombre_match": "", "score": "",
                 "name_s": "", "age_s": "", "loc_s": "", "tipo": "",
                 "edad": "", "ubicacion": "", "fuente": ""}]

    rows = []
    for r in top:
        rows.append({
            "nombre_input": query_name,
            "nombre_match": r.get("full_name") or "",
            "score":        r["_score"],
            "name_s":       r["_name_s"],
            "age_s":        r["_age_s"],
            "loc_s":        r["_loc_s"],
            "tipo":         r.get("kind") or "",
            "edad":         r.get("age") or "",
            "ubicacion":    r.get("last_seen_location") or "",
            "fuente":       r.get("source") or "",
        })
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    if not _HAS_FUZZ:
        print("[WARN] rapidfuzz no instalado — fuzzy scoring desactivado. "
              "Instalá con: pip install rapidfuzz", file=sys.stderr)

    # Construir lista de nombres a procesar
    if args.archivo:
        try:
            with open(args.archivo, encoding="utf-8") as f:
                nombres = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        except FileNotFoundError:
            sys.exit(f"[ERROR] No se encontró el archivo: {args.archivo}")
    else:
        nombres = [args.nombre]

    total = len(nombres)
    output_path = "output.csv"

    print(f"\n{'═' * 60}")
    print(f"  Nombres a procesar : {total}")
    print(f"  Recall límite      : {_RECALL_LIMIT} por token")
    print(f"  Output             : {output_path}")
    print(f"{'═' * 60}\n")

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_CSV_COLUMNS)
        writer.writeheader()

        for i, nombre in enumerate(nombres, 1):
            print(f"[{i}/{total}] {nombre}")
            rows = await _run_one(nombre, args.edad, args.ubicacion)
            writer.writerows(rows)
            csvfile.flush()

    print(f"\n✓ Listo → {output_path}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Buscar nombre(s) en la BD de Reune VE")
    grupo = parser.add_mutually_exclusive_group(required=True)
    grupo.add_argument("nombre", nargs="?", help="Nombre a buscar (ej: 'José Rodriguez')")
    grupo.add_argument("--archivo", metavar="ARCHIVO", help="Archivo .txt con un nombre por línea")
    parser.add_argument("--edad", type=int, default=None, help="Edad aproximada global (opcional)")
    parser.add_argument("--ubicacion", default=None, help="Ubicación global (opcional, ej: 'La Guaira')")
    args = parser.parse_args()

    if not args.nombre and not args.archivo:
        parser.error("Debés proveer un nombre o --archivo")

    asyncio.run(main(args))
