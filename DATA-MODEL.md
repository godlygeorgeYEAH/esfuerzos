# Modelo de datos — Reúne VE (esquema VIVO)

Fuente de verdad: la base Supabase en producción (`bgebvwchqtrhvdhkpzgk`), introspeccionada
vía PostgREST el 2026-06-29. **Las migraciones en `migrations/` están drifted** respecto a la
DB viva (ver `migrations/000_current_schema_reference.sql`); este documento refleja la DB real.

## Diagrama entidad-relación

```mermaid
erDiagram
    reports ||--o{ photos : "report_id"
    reports ||--o{ bot_subscribers : "report_id"
    reports ||--o{ matches : "missing_id"
    reports ||--o{ matches : "found_id"
    reports ||--|| canonical_reports : "vista (no-duplicados)"

    reports {
        uuid id PK
        report_kind kind "missing | found"
        text full_name
        int age
        text last_seen_location
        float last_seen_lat
        float last_seen_lng
        text distinguishing_marks "lleva 'CI: <cedula>' para exact-match"
        text clothing
        person_state person_state "unknown|alive|injured|deceased"
        text reporter_wa_hash "hash del telefono (PII)"
        text reporter_contact_enc "contacto cifrado (PII)"
        text source "scraper o waha_whatsapp"
        text source_url "unico por (source, source_url)"
        jsonb raw_data
        bool consent
        vector text_embedding "768-dim (SentenceTransformer)"
        uuid dedup_group_id "reservado; dedup real va en raw_data.possible_duplicate_of"
        timestamptz created_at
        timestamptz updated_at
        timestamptz expires_at
        text unique_src
    }
    photos {
        uuid id PK
        uuid report_id FK
        text storage_path "URL/path de la imagen"
        text face_subject_id
        vector face_embedding "512-dim (InsightFace buffalo_sc)"
        bool quality_ok "true si se detecto cara"
        real det_score
        jsonb face_bbox
        timestamptz created_at
    }
    matches {
        uuid id PK
        uuid missing_id FK "report kind=missing"
        uuid found_id FK "report kind=found"
        real text_score
        real face_score
        real combined_score
        match_status status "pending|confirmed|rejected"
        text reviewer "quien aprobo (dashboard)"
        timestamptz reviewed_at
        bool notify_sent "el notifier ya aviso a la familia"
        timestamptz created_at
    }
    bot_subscribers {
        uuid report_id PK "FK a reports"
        text phone "PII: numero del familiar"
        text full_name
        text kind
        timestamptz created_at
        timestamptz last_notified_at
    }
    llm_leads {
        uuid id PK
        text source
        text source_url
        text full_name
        int age
        text location
        text kind
        text contact
        float confidence
        text context "cita textual de respaldo"
        jsonb raw_data
        text review_status "pending|approved|rejected"
        timestamptz reviewed_at
        timestamptz created_at
    }
    waha_sessions {
        text phone PK
        jsonb state "conv history + collected + rkey"
        timestamptz updated_at
    }
    scraper_runs {
        uuid id PK
        text source
        text run_type "poll | full"
        timestamptz started_at
        timestamptz finished_at
        int rows_inserted
        int rows_updated
        text error
        timestamptz created_at
    }
```

## Enums (tipos Postgres)
- `report_kind`: `missing` | `found`
- `person_state`: `unknown` | `alive` | `injured` | `deceased` (enum vivo verificado 2026-06-29; `found`/`discharged` NO son válidos, los rechaza con 400)
- `match_status`: `pending` | `confirmed` | `rejected`

## Flujo de datos
1. **Ingesta** → todo aterriza en `reports` (scrapers + `waha_whatsapp` + `llm_approved`). `source` marca el canal; único por `(source, source_url)`.
2. **Fotos** → `photos` (1:N con `reports`); embedding facial 512-dim; `quality_ok=true` si hay cara.
3. **Dedup** → `dedup_pipeline` marca duplicados en `raw_data.possible_duplicate_of`. La vista `canonical_reports` expone solo los no-duplicados (~70.5k de ~82.7k).
4. **Matching** → `consolidation_pipeline` (texto, pgvector) + `face_pipeline` (cara) + `run_cedula_exact_match` (CI exacta, la señal más fuerte) escriben pares en `matches` con `status='pending'`.
5. **Revisión humana** → `/admin/dashboard` lista `matches` pending; aprobar setea `status='confirmed'` + `reviewer` + `reviewed_at`.
6. **Notificación** → `notify_pipeline` (cada 10 min) toma `status='confirmed'` + `notify_sent=false` y avisa a `bot_subscribers` de ambos lados, marcando `notify_sent=true`.

## Índices y búsqueda vectorial
- `reports.text_embedding` — ivfflat cosine (RPC `match_reports_by_text`).
- `photos.face_embedding` — ivfflat cosine, `WHERE quality_ok=true` (RPC `match_reports_by_face`).
- Único `(source, source_url)` en `reports` para upsert idempotente.

## Vistas
- `canonical_reports` (migración 014) = `reports WHERE raw_data->>'possible_duplicate_of' IS NULL`. Base limpia, una fila por persona.
