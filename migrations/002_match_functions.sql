-- migrations/002_match_functions.sql
-- Reune v1: missing persons match engine
-- Tables, indexes, RLS, and RPC functions for vector search.
-- Run after 001_initial.sql.

-- -- Extension ------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS vector;

-- -- Tables ---------------------------------------------------------------------

-- reports: one row per missing or found person filing
CREATE TABLE IF NOT EXISTS reports (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at           TIMESTAMPTZ NOT NULL    DEFAULT now(),
    kind                 TEXT        NOT NULL    CHECK (kind IN ('missing', 'found')),
    source               TEXT        NOT NULL,
    full_name            TEXT,
    age                  INT,
    last_seen_location   TEXT,
    distinguishing_marks TEXT,
    clothing             TEXT,
    -- 768-dim: paraphrase-multilingual-mpnet-base-v2
    text_embedding       vector(768),
    -- set to a shared UUID when near-duplicates are detected (same kind)
    dedup_group_id       UUID
);

CREATE INDEX IF NOT EXISTS idx_reports_kind
    ON reports (kind);
CREATE INDEX IF NOT EXISTS idx_reports_created_at
    ON reports (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_dedup
    ON reports (dedup_group_id)
    WHERE dedup_group_id IS NOT NULL;

-- IVFFlat for approximate cosine search on text embeddings.
-- Rebuild or increase lists after the dataset grows beyond ~10k rows.
-- For disaster-window datasets (thousands, not millions), consider exact search
-- or raising ivfflat.probes at query time to improve recall.
CREATE INDEX IF NOT EXISTS idx_reports_text_ivfflat
    ON reports USING ivfflat (text_embedding vector_cosine_ops)
    WITH (lists = 100);

-- photos: one or more face images per report
CREATE TABLE IF NOT EXISTS photos (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at     TIMESTAMPTZ NOT NULL    DEFAULT now(),
    report_id      UUID        NOT NULL    REFERENCES reports(id) ON DELETE CASCADE,
    url            TEXT        NOT NULL,
    -- 512-dim: CompreFace calculator plugin
    face_embedding vector(512),
    -- true when CompreFace det_prob >= 0.7 at enrollment time
    quality_ok     BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_photos_report_id
    ON photos (report_id);

-- Partial index: only index quality photos used in face search
CREATE INDEX IF NOT EXISTS idx_photos_face_ivfflat
    ON photos USING ivfflat (face_embedding vector_cosine_ops)
    WITH (lists = 100)
    WHERE quality_ok = true;

-- matches: candidate pairs pending human review
-- Auto-confirmation is prohibited; status transitions require explicit review.
CREATE TABLE IF NOT EXISTS matches (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at        TIMESTAMPTZ NOT NULL    DEFAULT now(),
    report_found_id   UUID        NOT NULL    REFERENCES reports(id) ON DELETE CASCADE,
    report_missing_id UUID        NOT NULL    REFERENCES reports(id) ON DELETE CASCADE,
    combined_score    FLOAT8      NOT NULL,
    text_score        FLOAT8      NOT NULL    DEFAULT 0,
    face_score        FLOAT8      NOT NULL    DEFAULT 0,
    status            TEXT        NOT NULL    DEFAULT 'pending'
                                  CHECK (status IN ('pending', 'confirmed', 'rejected')),
    reviewed_by       TEXT,
    reviewed_at       TIMESTAMPTZ,
    -- on_conflict target used by the upsert in match_engine.py
    CONSTRAINT uq_match_pair UNIQUE (report_found_id, report_missing_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_status
    ON matches (status);
CREATE INDEX IF NOT EXISTS idx_matches_found_id
    ON matches (report_found_id);
CREATE INDEX IF NOT EXISTS idx_matches_missing_id
    ON matches (report_missing_id);

-- -- Row Level Security ---------------------------------------------------------

ALTER TABLE reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE photos  ENABLE ROW LEVEL SECURITY;
ALTER TABLE matches ENABLE ROW LEVEL SECURITY;

-- Public can read reports and photos (search / display)
CREATE POLICY "Public read reports"
    ON reports FOR SELECT TO anon USING (true);

CREATE POLICY "Public read photos"
    ON photos  FOR SELECT TO anon USING (true);

-- Matches are restricted to service_role (admin review UI uses the service key).
-- Anon cannot read match candidates; no SELECT policy for anon on matches.

-- -- RPC Functions --------------------------------------------------------------

-- match_reports_by_text
-- Cosine similarity search over reports.text_embedding.
-- kind filtering is intentionally left to the caller (match_engine.py) so the
-- same function works for both matching (opposite kind) and dedup (same kind).
CREATE OR REPLACE FUNCTION match_reports_by_text(
    query_embedding vector,
    match_threshold float,
    match_count     int
)
RETURNS TABLE (
    id          uuid,
    full_name   text,
    source      text,
    kind        text,
    similarity  float
)
LANGUAGE sql STABLE
AS $$
    SELECT
        r.id,
        r.full_name,
        r.source,
        r.kind,
        (1 - (r.text_embedding <=> query_embedding))::float AS similarity
    FROM reports r
    WHERE r.text_embedding IS NOT NULL
      AND (1 - (r.text_embedding <=> query_embedding)) >= match_threshold
    ORDER BY r.text_embedding <=> query_embedding   -- ascending distance = descending similarity
    LIMIT match_count;
$$;


-- match_photos_by_face
-- Cosine similarity search over photos.face_embedding.
-- Only considers photos where quality_ok = true.
CREATE OR REPLACE FUNCTION match_photos_by_face(
    query_embedding vector,
    match_threshold float,
    match_count     int
)
RETURNS TABLE (
    id          uuid,
    report_id   uuid,
    similarity  float
)
LANGUAGE sql STABLE
AS $$
    SELECT
        p.id,
        p.report_id,
        (1 - (p.face_embedding <=> query_embedding))::float AS similarity
    FROM photos p
    WHERE p.face_embedding IS NOT NULL
      AND p.quality_ok = true
      AND (1 - (p.face_embedding <=> query_embedding)) >= match_threshold
    ORDER BY p.face_embedding <=> query_embedding
    LIMIT match_count;
$$;
