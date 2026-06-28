"""
text_normalize.py - Shared Venezuelan-Spanish text normalization for matching.

Three clean-room utilities used across matching, dedup, and ingest:

  deaccent(s)              lowercase + strip accents (García -> garcia)
  normalize_location(raw)  map VE location variants to a canonical form
                           (Vargas / Maiquetía / Litoral Central -> "la guaira")
  spanish_phonetic(name)   phonetic key so homophones collapse
                           (José/Hose, González/Gonsales, Beltrán/Veltran)
  status_rank(kind, marks) priority ladder: deceased>found>injured>missing>unknown

Designed to be dependency-free (pure stdlib) so it can run inside any pipeline.
The location map is hand-built for the 2026 La Guaira/Vargas earthquake zone plus
all 24 states; extend _LOCATION_CANON as new variants appear in scraped data.
"""
from __future__ import annotations

import re
import unicodedata


def deaccent(s: str) -> str:
    """Lowercase and strip combining accents. 'García' -> 'garcia'."""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", (s or "").lower())
        if unicodedata.category(ch) != "Mn"
    )


# ---------------------------------------------------------------------------
# Location canonicalization
# ---------------------------------------------------------------------------
# Maps a deaccented substring -> canonical (deaccented) location. Matching is
# substring-based after deaccent, so "res caribe torre 2 la guaira" and
# "maiquetia, vargas" both canonicalize toward "la guaira". Order matters:
# more specific keys first is not required (we collect all hits and pick the
# canonical state/zone), but we keep the map flat and check membership.

# Zone/state synonyms -> canonical name. The 2026 quake hit La Guaira hardest.
_LOCATION_CANON: dict[str, str] = {
    # La Guaira (formerly Vargas) and its parishes / colloquialisms
    "la guaira": "la guaira",
    "vargas": "la guaira",
    "edo vargas": "la guaira",
    "estado vargas": "la guaira",
    "litoral central": "la guaira",
    "litoral": "la guaira",
    "maiquetia": "la guaira",
    "catia la mar": "la guaira",
    "macuto": "la guaira",
    "caraballeda": "la guaira",
    "naiguata": "la guaira",
    "carayaca": "la guaira",
    "tanaguarena": "la guaira",
    "los corales": "la guaira",
    # Caracas / Distrito Capital
    "caracas": "caracas",
    "ccs": "caracas",
    "distrito capital": "caracas",
    "dtto capital": "caracas",
    "dc": "caracas",
    "libertador": "caracas",
    "petare": "caracas",
    "catia": "caracas",
    "el valle": "caracas",
    "antimano": "caracas",
    # Miranda
    "miranda": "miranda",
    "edo miranda": "miranda",
    "los teques": "miranda",
    "guarenas": "miranda",
    "guatire": "miranda",
    "charallave": "miranda",
    "santa teresa": "miranda",
    "higuerote": "miranda",
    # Other 24 states (canonical = state name, deaccented)
    "amazonas": "amazonas",
    "anzoategui": "anzoategui",
    "puerto la cruz": "anzoategui",
    "barcelona": "anzoategui",
    "apure": "apure",
    "san fernando de apure": "apure",
    "aragua": "aragua",
    "maracay": "aragua",
    "la victoria": "aragua",
    "barinas": "barinas",
    "bolivar": "bolivar",
    "ciudad guayana": "bolivar",
    "ciudad bolivar": "bolivar",
    "puerto ordaz": "bolivar",
    "carabobo": "carabobo",
    "valencia": "carabobo",
    "puerto cabello": "carabobo",
    "cojedes": "cojedes",
    "san carlos": "cojedes",
    "delta amacuro": "delta amacuro",
    "tucupita": "delta amacuro",
    "falcon": "falcon",
    "coro": "falcon",
    "punto fijo": "falcon",
    "guarico": "guarico",
    "san juan de los morros": "guarico",
    "calabozo": "guarico",
    "lara": "lara",
    "barquisimeto": "lara",
    "carora": "lara",
    "merida": "merida",
    "el vigia": "merida",
    "monagas": "monagas",
    "maturin": "monagas",
    "nueva esparta": "nueva esparta",
    "margarita": "nueva esparta",
    "porlamar": "nueva esparta",
    "portuguesa": "portuguesa",
    "guanare": "portuguesa",
    "acarigua": "portuguesa",
    "sucre": "sucre",
    "cumana": "sucre",
    "carupano": "sucre",
    "tachira": "tachira",
    "san cristobal": "tachira",
    "trujillo": "trujillo",
    "valera": "trujillo",
    "yaracuy": "yaracuy",
    "san felipe": "yaracuy",
    "zulia": "zulia",
    "maracaibo": "zulia",
    "cabimas": "zulia",
}


def normalize_location(raw: str | None) -> str | None:
    """Map a raw location string to a canonical VE location, or return the
    deaccented original if no synonym matches. None/empty -> None."""
    if not raw:
        return None
    d = deaccent(raw)
    d = re.sub(r"[^a-z0-9 ]+", " ", d)
    d = re.sub(r"\s+", " ", d).strip()
    if not d:
        return None
    # Longest-key-first so "la guaira" wins before "guaira", "ciudad bolivar"
    # before "bolivar", etc.
    for key in sorted(_LOCATION_CANON, key=len, reverse=True):
        if key in d:
            return _LOCATION_CANON[key]
    return d


def location_score(a: str | None, b: str | None) -> float:
    """0..1 location agreement after canonicalization. 0.5 neutral if either
    is missing (so location never dominates when unknown, only confirms)."""
    if not a or not b:
        return 0.5
    ca, cb = normalize_location(a), normalize_location(b)
    if not ca or not cb:
        return 0.5
    if ca == cb:
        return 1.0
    # Partial: share a token (e.g. one is "la guaira", other free text incl. it)
    at, bt = set(ca.split()), set(cb.split())
    if at & bt:
        return 0.7
    return 0.0


# ---------------------------------------------------------------------------
# Spanish phonetic key
# ---------------------------------------------------------------------------
# Collapses Spanish orthographic homophones to a single key so name matching
# catches José/Hose, González/Gonsales, Beltrán/Veltran, Yajaira/Llajaira.

def phonetic_token(tok: str) -> str:
    t = deaccent(tok)
    t = re.sub(r"[^a-z]", "", t)
    if not t:
        return ""
    # Ordered substitutions (multi-char first)
    t = t.replace("ch", "x")          # treat ch as a unit
    t = t.replace("qu", "k")
    t = t.replace("gue", "ge").replace("gui", "gi")
    t = t.replace("ll", "y")
    t = t.replace("ñ", "ni")
    t = re.sub(r"h", "", t)            # silent h
    t = t.replace("v", "b")           # b/v merge
    t = re.sub(r"z|c(?=[ei])", "s", t)  # z and soft c -> s
    t = t.replace("c", "k")           # remaining hard c -> k
    t = re.sub(r"g(?=[ei])", "j", t)  # soft g -> j
    t = t.replace("w", "b")
    t = t.replace("y", "i")           # vowel-ish y
    t = re.sub(r"(.)\1+", r"\1", t)   # collapse doubles (rr->r, etc.)
    return t


def spanish_phonetic(name: str) -> str:
    """Space-joined phonetic key of all tokens, sorted so order doesn't matter."""
    toks = [phonetic_token(t) for t in (name or "").split()]
    toks = [t for t in toks if len(t) >= 2]
    return " ".join(sorted(toks))


def phonetic_token_set(name: str) -> set[str]:
    """Set of per-token phonetic keys (for token-level overlap)."""
    return {p for t in (name or "").split() if len(p := phonetic_token(t)) >= 2}


# ---------------------------------------------------------------------------
# Status priority ladder
# ---------------------------------------------------------------------------
# Higher = more definitive / better-news-first for a reunification bot.
_STATUS_RANK = {"deceased": 5, "found": 4, "injured": 3, "missing": 2, "unknown": 1}

_DECEASED_KW = ("fallecid", "muert", "deceased", "occiso")
_INJURED_KW = ("herid", "lesionad", "hospital", "injured", "rescatad", "atrapad")
_SAFE_KW = ("a salvo", "encontrad", "localizad", "found", "safe", "vivo", "con vida")


def status_rank(kind: str | None, marks: str | None = None) -> int:
    """Priority of a report's status. kind is missing/found; marks free text may
    sharpen it (fallecido/herido/a salvo)."""
    m = deaccent(marks or "")
    if any(k in m for k in _DECEASED_KW):
        return _STATUS_RANK["deceased"]
    if any(k in m for k in _SAFE_KW) or kind == "found":
        # injured still counts as located but flag if explicitly injured
        if any(k in m for k in _INJURED_KW):
            return _STATUS_RANK["injured"] if kind != "found" else _STATUS_RANK["found"]
        return _STATUS_RANK["found"]
    if any(k in m for k in _INJURED_KW):
        return _STATUS_RANK["injured"]
    if kind == "missing":
        return _STATUS_RANK["missing"]
    return _STATUS_RANK["unknown"]
