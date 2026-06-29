-- migrations/000_current_schema_reference.sql
--
-- ⚠ REFERENCE ONLY — DO NOT RUN. This is NOT an ordered migration.
--
-- The numbered migrations (001..014) drifted from the production database (the
-- live DB was hand-edited over the incident). This file is the authoritative
-- snapshot of the LIVE schema, reconstructed via PostgREST introspection on
-- 2026-06-29, so a from-scratch rebuild has a correct target. When the live DB
-- and the numbered migrations disagree, THIS file (and the live DB) win.
--
-- Known drift vs migrations/002_match_functions.sql:
--   matches: live uses missing_id/found_id + reviewer + notify_sent
--            (migration said report_missing_id/report_found_id + reviewed_by).
--   reports: live has extra cols person_state, reporter_wa_hash,
--            reporter_contact_enc, consent, expires_at, unique_src, updated_at.
--   photos:  live has face_subject_id, det_score, face_bbox.

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TYPE report_kind  AS ENUM ('missing', 'found');
-- person_state verified against the LIVE DB by insert-probe (2026-06-29): only these
-- four are accepted; 'found' and 'discharged' are REJECTED (400). Scrapers write
-- 'alive' for located patients. Do not assume 'found'/'discharged' exist.
CREATE TYPE person_state AS ENUM ('unknown', 'alive', 'injured', 'deceased');
CREATE TYPE match_status AS ENUM ('pending', 'confirmed', 'rejected');

CREATE TABLE reports (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind                 report_kind NOT NULL,
    full_name            text,
    age                  integer,
    last_seen_location   text,
    last_seen_lat        double precision,
    last_seen_lng        double precision,
    distinguishing_marks text,                       -- carries "CI: <cedula>" for exact match
    clothing             text,
    person_state         person_state DEFAULT 'unknown',
    reporter_wa_hash     text,                        -- PII: hashed reporter phone
    reporter_contact_enc text,                        -- PII: encrypted contact
    source               text NOT NULL,
    source_url           text,
    raw_data             jsonb,                       -- dedup flag lives in raw_data->>'possible_duplicate_of'
    consent              boolean,
    text_embedding       vector(768),
    dedup_group_id       uuid,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz,
    expires_at           timestamptz,
    unique_src           text,
    UNIQUE (source, source_url)
);

CREATE TABLE photos (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id       uuid NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    storage_path    text,
    face_subject_id text,
    face_embedding  vector(512),
    quality_ok      boolean,
    det_score       real,
    face_bbox       jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE matches (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    missing_id     uuid NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    found_id       uuid NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
    text_score     real DEFAULT 0,
    face_score     real DEFAULT 0,
    combined_score real NOT NULL,
    status         match_status NOT NULL DEFAULT 'pending',
    reviewer       text,
    reviewed_at    timestamptz,
    notify_sent    boolean DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (missing_id, found_id)
);

CREATE TABLE bot_subscribers (
    report_id        uuid PRIMARY KEY REFERENCES reports(id) ON DELETE CASCADE,
    phone            text NOT NULL,                   -- PII
    full_name        text,
    kind             text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    last_notified_at timestamptz
);

CREATE TABLE llm_leads (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source        text,
    source_url    text,
    full_name     text,
    age           integer,
    location      text,
    kind          text,
    contact       text,
    confidence    double precision,
    context       text,
    raw_data      jsonb,
    review_status text DEFAULT 'pending',
    reviewed_at   timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE waha_sessions (
    phone      text PRIMARY KEY,
    state      jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE scraper_runs (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source        text,
    run_type      text,
    started_at    timestamptz,
    finished_at   timestamptz,
    rows_inserted integer,
    rows_updated  integer,
    error         text,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Deduplicated base (migration 014).
CREATE OR REPLACE VIEW canonical_reports AS
    SELECT * FROM reports WHERE raw_data->>'possible_duplicate_of' IS NULL;

-- RLS: reports/photos have RLS ENABLED with NO anon policy (anon denied);
-- service_role bypasses RLS. See migration 012. GRANTs to service_role on all tables.
