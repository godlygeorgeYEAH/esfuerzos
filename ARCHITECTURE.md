# Architecture - Reune VE

## Data flow

```
WhatsApp user
    |
    v
Base44 agent (ReúneVE)         <- system prompt defines intake flow
    |  [REPORT:{...}] marker
    |  [PHONE:+XX...] marker
    v
base44_poller.py               <- polls Base44 every 30s (no webhook plan needed)
    |
    +-> conversation_ids table (Supabase) - phone <-> conv_id mapping
    |
    +-> reports table (Supabase) - upsert on (source, source_url)
    |
    +-> photos table (Supabase) - photo URLs from user messages
    |
    v
face_pipeline.py
    |
    +-> InsightFace buffalo_sc  - 512-dim face embedding (CPU)
    |
    +-> match_reports_by_face() - pgvector cosine similarity RPC
    |     WHERE r.kind != report.kind  (missing <-> found cross-search)
    |     AND similarity >= COMBINED_MATCH_THRESHOLD (default 0.65)
    |
    +-> if match: insert into matches table
    |
    v
notification
    POST /conversations/{conv_id}/messages  (Base44 API)
    -> both reporter and matched party get WhatsApp message
    -> text: "posible coincidencia, en verificacion"
```

## Scraper pipeline

```
APScheduler (every 5 min poll, 1 hr full sweep)
    |
    v
scraper_orchestrator.py -> each BaseScraper/BaseVEScraper
    |
    v
Supabase reports table  (upsert on source, source_url)
    |
    v
face_pipeline.py per scraped photo (async background)
```

## Supabase schema

### reports
| Column | Type | Notes |
|--------|------|-------|
| id | uuid | PK |
| kind | text | "missing" or "found" |
| full_name | text | |
| age | int | |
| last_seen_location | text | last seen / hospital name |
| distinguishing_marks | text | physical description |
| clothing | text | |
| person_state | text | "buscando", "encontrado", etc. |
| source | text | scraper name or "base44_whatsapp" |
| source_url | text | unique per source |
| reporter_wa_hash | text | hashed WhatsApp ID of reporter |
| reporter_contact_enc | text | encrypted contact (not plain phone) |
| consent | bool | GDPR consent flag |
| text_embedding | vector(768) | SentenceTransformer |
| dedup_group_id | uuid | links duplicate reports |
| created_at | timestamptz | |
| updated_at | timestamptz | |
| expires_at | timestamptz | |
| unique_src | text | |

UNIQUE constraint: (source, source_url)

### photos
| Column | Type | Notes |
|--------|------|-------|
| id | uuid | PK |
| report_id | uuid | FK reports |
| storage_path | text | URL or local path |
| face_embedding | vector(512) | InsightFace buffalo_sc |
| quality_ok | bool | face detected |
| created_at | timestamptz | |

### matches
| Column | Type | Notes |
|--------|------|-------|
| id | uuid | PK |
| missing_id | uuid | FK reports (kind=missing) |
| found_id | uuid | FK reports (kind=found) |
| face_score | float | cosine similarity |
| text_score | float | text embedding similarity |
| combined_score | float | weighted face+text |
| status | text | "pending", "confirmed", "rejected" |
| reviewed_by | text | human reviewer ID |
| reviewed_at | timestamptz | |
| created_at | timestamptz | |

### conversation_ids
| Column | Type | Notes |
|--------|------|-------|
| conv_id | text | PK (Base44 conversation ID) |
| phone | text | E.164 format |
| report_id | uuid | FK reports |
| last_seen_at | timestamptz | last polled timestamp |
| updated_at | timestamptz | |

## Supabase RPC

```sql
match_reports_by_face(
    query_embedding vector(512),
    query_kind text,           -- kind of the source report (RPC returns opposite kind)
    match_threshold float,
    match_count int
) RETURNS TABLE(id uuid, full_name text, face_similarity float)
```

## Key config values

| Setting | Default | Effect |
|---------|---------|--------|
| FACE_MATCH_THRESHOLD | 0.50 | min face similarity to consider |
| COMBINED_MATCH_THRESHOLD | 0.65 | min combined score to insert match |
| FACE_WEIGHT | 0.35 | weight of face score in combined |
| TEXT_WEIGHT | 0.65 | weight of text score in combined |
| PHOTO_RETENTION_DAYS | 30 | days before photos purged |

## Container layout

```
/app/                   <- api/ bind-mounted from host /root/reune/api/
/root/.insightface/     <- named volume (model cache, ~500MB)
/root/.cache/huggingface/ <- named volume (SentenceTransformer cache, ~500MB)
/root/sos_images/       <- host bind mount (read-only, 50k WebP photos)
/root/crisis_images/    <- host bind mount (read-only)
/crisis_data/           <- host bind mount (read-only, SQLite DBs + bulk data)
/app/data/              <- named volume (app-local persistent state)
```

## Base44 polling vs webhooks

Base44 webhooks require the Builder plan. Current setup uses polling (every 30s via `base44_poller.py`). If Builder plan is activated, register webhook at `POST /hooks/base44` and polling can be disabled.

## Human-in-the-loop

All matches go into `matches` table with `status = "pending"`. The bot sends "posible coincidencia, en verificacion" to both parties. A human (via Supabase dashboard or future admin UI) must set `status = "confirmed"` before any definitive information is shared. The bot NEVER says "encontrado" or communicates a death.
