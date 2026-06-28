"""
buscar_nombre.py — CLI para probar la cadena de matching de nombres contra la BD.

Uso:
    python buscar_nombre.py "José Rodriguez"
    python buscar_nombre.py "Anahys" --edad 32 --ubicacion "La Guaira"
    python buscar_nombre.py "González" --limite 20

Requiere SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY en el entorno o en .env
"""
from __future__ import annotations

import argparse
import asyncio
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
            "_score":    round(score, 3),
            "_name_s":   round(ns, 3),
            "_age_s":    round(ag, 3),
            "_loc_s":    round(loc, 3),
        }))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored]


# ---------------------------------------------------------------------------
# Recall: ILIKE por token contra Supabase
# ---------------------------------------------------------------------------

async def _recall(name: str, limite: int) -> list:
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
                    "limit": str(limite),
                    "order": "created_at.desc",
                },
            )
            if r.status_code != 200:
                print(f"[WARN] Supabase respondió {r.status_code} para token '{token}'", file=sys.stderr)
                continue
            for row in r.json():
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    results.append(row)

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def _print_results(results: list, query_name: str, query_age, query_location) -> None:
    if not results:
        print(f"\nSin resultados para '{query_name}' (piso name_score >= {_NAME_FLOOR})\n")
        return

    col_name  = max(len(r.get("full_name") or "") for r in results)
    col_name  = max(col_name, 20)
    col_src   = max(len(r.get("source") or "") for r in results)
    col_src   = max(col_src, 10)
    col_loc   = max(len(r.get("last_seen_location") or "") for r in results)
    col_loc   = max(col_loc, 12)

    header = (
        f"{'#':<3} "
        f"{'SCORE':<7} "
        f"{'NAME_S':<7} "
        f"{'AGE_S':<6} "
        f"{'LOC_S':<6} "
        f"{'TIPO':<8} "
        f"{'NOMBRE':<{col_name}} "
        f"{'EDAD':<5} "
        f"{'UBICACION':<{col_loc}} "
        f"{'FUENTE':<{col_src}}"
    )

    print(f"\nQuery: '{query_name}'"
          + (f"  edad={query_age}" if query_age else "")
          + (f"  ubicacion='{query_location}'" if query_location else ""))
    print(f"Resultados: {len(results)}\n")
    print(header)
    print("─" * len(header))

    for i, r in enumerate(results, 1):
        print(
            f"{i:<3} "
            f"{r['_score']:<7} "
            f"{r['_name_s']:<7} "
            f"{r['_age_s']:<6} "
            f"{r['_loc_s']:<6} "
            f"{(r.get('kind') or ''):<8} "
            f"{(r.get('full_name') or ''):<{col_name}} "
            f"{str(r.get('age') or ''):<5} "
            f"{(r.get('last_seen_location') or ''):<{col_loc}} "
            f"{(r.get('source') or ''):<{col_src}}"
        )
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(query_name: str, query_age, query_location: str | None, limite: int) -> None:
    if not _HAS_FUZZ:
        print("[WARN] rapidfuzz no instalado — fuzzy scoring desactivado. "
              "Instalá con: pip install rapidfuzz", file=sys.stderr)

    print(f"[1/2] Recall ILIKE → Supabase...", end=" ", flush=True)
    candidates = await _recall(query_name, limite)
    print(f"{len(candidates)} candidatos")

    print(f"[2/2] Scoring + ranking...", end=" ", flush=True)
    ranked = _rank_candidates(query_name, query_age, candidates, query_location)
    print(f"{len(ranked)} pasan el piso (name_score >= {_NAME_FLOOR})")

    _print_results(ranked, query_name, query_age, query_location)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Buscar nombre en la BD de Reune VE")
    parser.add_argument("nombre", help="Nombre a buscar (ej: 'José Rodriguez')")
    parser.add_argument("--edad", type=int, default=None, help="Edad aproximada (opcional)")
    parser.add_argument("--ubicacion", default=None, help="Ubicación (opcional, ej: 'La Guaira')")
    parser.add_argument("--limite", type=int, default=50, help="Máx candidatos del recall (default: 50)")
    args = parser.parse_args()

    asyncio.run(main(args.nombre, args.edad, args.ubicacion, args.limite))
