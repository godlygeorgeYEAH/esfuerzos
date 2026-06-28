# Status — Reune VE
**Last updated:** 2026-06-27 (session 4)
**Phase:** Bot architecture complete. WAHA running, QR scan pending. Groq wired but needs API key on VPS.

---

## Live Components

| Component | URL | Status |
|-----------|-----|--------|
| reune-ve-api | http://13.140.166.72:8080 | RUNNING |
| WAHA | http://13.140.166.72:3000 | SCAN_QR_CODE — needs WhatsApp number |
| Supabase | https://bgebvwchqtrhvdhkpzgk.supabase.co | CONNECTED |

## Infrastructure

- VPS: Contabo 13.140.166.72, 7.8 GB RAM
- Docker Compose at /root/reune/
- API image: reune-ve-api (CPU-only: torch + insightface + sentence-transformers)
- Model volumes: insightface_cache, huggingface_cache
- Repo: https://github.com/godlygeorgeYEAH/esfuerzos (branch: main)

## Supabase

- URL: https://bgebvwchqtrhvdhkpzgk.supabase.co
- Display name: rename to "Reune VE" (Settings > General)
- Active tables: reports, photos, matches, audit_log, scraper_runs
- Legacy tables purged: damage_reports, safe_checkins (migration 004)
- RPC functions: match_reports_by_text, match_photos_by_face
- Migrations applied: 001_initial, 002_match_functions, 003_grants, 004_drop_crisis_tables

## Architecture (current)

```
WhatsApp user
     |
   WAHA (NOWEB engine, port 3000)
     |  webhook POST /webhook/waha
waha_intake.py
     |
   Groq llama-3.3-70b-versatile
     |  extracts: kind, name, age, location, description
     |
Supabase reports table
     |
consolidation_pipeline.py  ->  text embeddings + face cross-match + cedula exact match
```

Bot flows (handled by bot/flows.py via FSM):
- **Familiar**: reporta persona desaparecida
- **Hospital**: registra listas de ingresos hospitalarios
- **Rescatista**: reporta persona encontrada

## Active Scrapers (APScheduler — 5 min poll / 1 hr full)

- reconexion (reconexion.com)
- sos_venezuela (sos-venezuela.com)
- venezreporta (venezuelareporta.org)
- + 9 fuentes adicionales (commit a680f56): govt, forensics, shelters, etc.

## Code Layout

```
main.py                    FastAPI entrypoint + lifespan (models + scheduler)
waha_intake.py             WAHA webhook router + Groq conversation handler
config.py                  Pydantic Settings (all env vars)
bot/flows.py               FSM: 3 flows (Familiar, Hospital, Rescatista)
bot/sessions.py            In-memory session store per phone
consolidation_pipeline.py  Text embed + text/face cross-match + cedula match
face_pipeline.py           InsightFace photo processing
embeddings.py              Text + face embedding helpers
scraper_orchestrator.py    APScheduler jobs for all scrapers
scrapers/                  One file per source + BaseScraper base class
bulk_importer.py           Batch import of pre-existing data (admin endpoint)
directorio_sismo_ve_importer.py  Directory-level importer for sismo data
migrations/                SQL migrations (001-004)
esfuerzos/                 Jorge's docs + prox module
```

## Monitoring

```bash
# API health
curl http://13.140.166.72:8080/health

# Container logs
ssh root@13.140.166.72 "docker logs reune-ve-api --tail 100 -f"

# WAHA dashboard
open http://13.140.166.72:3000

# DB counts (Supabase SQL editor)
select source, count(*) from reports group by source order by count desc;
select count(*) from photos where quality_ok = true;
select count(*), status from matches group by status;
```

## Pending

- [ ] WhatsApp number for WAHA QR scan (spare SIM or virtual)
- [ ] Add LLM_API_KEY (Groq key) to /root/reune/.env on VPS + restart API
- [ ] Rename Supabase project display name to "Reune VE" (Settings > General)
- [ ] Rotate Postgres DB password (was exposed in git history — Settings > Database > Reset)
