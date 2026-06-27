-- Migration: create external_leads table
-- Stores per-query exploratory search results from external sources.
-- report_id references reunion_reports (WhatsApp bot intake table),
-- NOT the 'reports' table (scraper aggregate). These are separate tables.
--
-- Run on Supabase: Settings > SQL Editor > New Query > paste + run.

CREATE TABLE IF NOT EXISTS external_leads (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- FK to reunion_reports (bot intake), NOT reports (scraper aggregate)
    report_id       UUID REFERENCES reunion_reports(id) ON DELETE CASCADE,
    source          TEXT NOT NULL,       -- source_name (hospitales_ve, red_ayuda_ve, etc.)
    source_url      TEXT,
    full_name       TEXT NOT NULL,
    age             INTEGER,
    location        TEXT,
    detail          TEXT,
    contact         TEXT,                -- phone number if available from source
    photo_url       TEXT,
    score           FLOAT NOT NULL,      -- composite 0.0-1.0 (primary ranking)
    name_similarity FLOAT NOT NULL,      -- rapidfuzz WRatio component
    kind            TEXT,                -- missing | found | hospital_patient | safe
    raw_data        JSONB,               -- original API response record
    notified_at     TIMESTAMPTZ,         -- timestamp when WhatsApp follow-up was sent
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_external_leads_report_id
    ON external_leads(report_id);

CREATE INDEX IF NOT EXISTS idx_external_leads_score
    ON external_leads(score DESC);

CREATE INDEX IF NOT EXISTS idx_external_leads_source
    ON external_leads(source);

CREATE INDEX IF NOT EXISTS idx_external_leads_created_at
    ON external_leads(created_at DESC);

-- RLS: service role can do everything; anon cannot read (contains contact data)
ALTER TABLE external_leads ENABLE ROW LEVEL SECURITY;

CREATE POLICY "service_role_all" ON external_leads
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Optional: expose a summary view to authenticated coordinators
-- CREATE VIEW external_leads_summary AS
--     SELECT report_id, source, full_name, score, location, contact, created_at
--     FROM external_leads
--     WHERE score >= 0.6
--     ORDER BY created_at DESC;
