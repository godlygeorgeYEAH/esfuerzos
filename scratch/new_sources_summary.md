# New Data Sources - Sismo 2026

Added 2026-06-27. Covers all new scrapers and one-time importers for the Venezuela earthquake response.

---

## What was implemented

### Scrapers (periodic, registered in orchestrator)

| Scraper | Source | Kind | Records est. | Notes |
|---|---|---|---|---|
| `localizados_venezuela` | localizadosvenezuela.com/api/v1/localizados | found | growing | REST API, no auth. asyncio + 3-retry backoff. ignore-duplicates. |
| `venezuela_te_busca` | venezuelatebusca.com | missing/found | 28,040 raw | Turbo-stream JSON. `_resolve()` with bool-before-int guard. Already current. |
| `sos_laguaira` | api.soslaguaira.lat/api/personas | missing/found | growing | Single-page API, La Guaira focus. estado label prepended in marks. |
| `pacientes_terremoto` | pacientesterremotovzla.lovable.app | found | ~3,964 | Supabase anon key from SPA bundle. Shared httpx client. No-arg constructor. |

All 4 are registered in `scraper_orchestrator.py` as of this commit.

Optional (key-gated, pre-existing):
- `hospitales_ve` - 8 fixes applied (hashlib fallback key, RPC param, deceased handling, PII).
- `redayuda_ve` - pre-existing, unchanged.

### One-time importers (run manually from `scratch/`)

| File | Source | Table | Est. records | Run command |
|---|---|---|---|---|
| `import_tilores_vtb.py` | Tilores VTB deduplicated export | `reports` | 26,962 | See below |
| `laiguana_laguaira_import.py` | laiguana.tv article | `reports` | unknown | See below |
| `directorio_sismo_ve_importer.py` | Curated platform directory | `discovered_sources` | 20 curated | See below |

---

## How to run each importer

### import_tilores_vtb.py

Requires a Tilores export file (CSV or JSONL). Request it from https://tilores.io under their humanitarian program.

```bash
# Dry run (no DB writes):
python scratch/import_tilores_vtb.py --file /path/to/tilores_export.jsonl --dry-run

# Full import:
python scratch/import_tilores_vtb.py --file /path/to/tilores_export.jsonl

# Download from signed URL then import:
python scratch/import_tilores_vtb.py \
  --url "https://tilores-signed-url..." \
  --file tilores_export.jsonl
```

Safe to re-run: uses `ignore-duplicates` on `(source, source_url)`. Dedup key is cedula hash when available, otherwise entity_id, otherwise name hash.

### laiguana_laguaira_import.py

Scrapes the La Iguana TV earthquake victim article. Requires `beautifulsoup4 lxml`.

```bash
pip install beautifulsoup4 lxml

# Default URL:
python scratch/laiguana_laguaira_import.py --dry-run

# Custom URL (e.g. archived copy):
python scratch/laiguana_laguaira_import.py \
  --url "https://web.archive.org/web/2026/https://laiguana.tv/desaparecidos-terremoto-venezuela-2026/"

# Full import:
python scratch/laiguana_laguaira_import.py
```

Note: if laiguana.tv returns 403, use a web.archive.org URL. The scraper attempts 3 parsing strategies (tables > lists > paragraphs) and stops after tables if they yield >= 20 records.

### directorio_sismo_ve_importer.py

Populates `discovered_sources` table (must exist -- see DDL in the script if it does not).

```bash
# Dry run:
python scratch/directorio_sismo_ve_importer.py --dry-run

# Full import (curated list only):
python scratch/directorio_sismo_ve_importer.py

# Add sources from a JSON file:
python scratch/directorio_sismo_ve_importer.py --input my_sources.json

# Add sources from an HTML directory page:
python scratch/directorio_sismo_ve_importer.py --url "https://some-directory.ve"
```

If `discovered_sources` does not exist, the script logs the CREATE TABLE DDL and exits. Apply the migration, then re-run.

---

## Sources needing manual intervention

| Source | Blocker | Action needed |
|---|---|---|
| Tilores VTB export | Must request file from Tilores (no public API) | Email tilores.io, cite humanitarian use |
| MPPS government bulletins | PDFs, no API | Manual download + pdfplumber extraction |
| CICPC / Medicina Legal | No public API, press-release format | Human monitoring; contact forensic office |
| Instagram @desaparecidosve | Graph API requires app review | Not feasible short-term; monitor manually |
| Twitter/X hashtags | Paid API tier required | Consider nitter scraper as fallback |
| WhatsApp community groups | Requires group membership + WAHA | Contact group admins for data share agreement |
| SAIME / Registro Civil | Captcha blocks automation | Manual cedula lookup only |

---

## Dedup strategy summary

All scrapers use `on_conflict=source,source_url` with `ignore-duplicates` as the default (re-runs are safe). Exception: `red_solidaria_venezuela` uses `merge-duplicates` intentionally (mutable spreadsheet source).

Dedup key patterns:
- `localizados_venezuela:{slug}`
- `venezuela_te_busca:{id}`
- `sos_laguaira:{id}`
- `pacientes_terremoto:{uuid}`
- `tilores_vtb:cedula_{sha1_12}` or `tilores_vtb:{entity_id}` or `tilores_vtb:name_{sha1_12}`
- `laiguana_laguaira:cedula_{sha1_12}` or `laiguana_laguaira:name_{sha1_12}`
