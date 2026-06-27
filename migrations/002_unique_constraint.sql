-- migrations/002_unique_constraint.sql
-- Add source_url column and UNIQUE constraint on (source, source_url) to reports.
-- Required by BaseScraper.upsert_report for idempotent ingestion.
-- Run after 002_match_functions.sql.

-- Add source_url column if not present (idempotent)
ALTER TABLE public.reports
    ADD COLUMN IF NOT EXISTS source_url TEXT;

-- UNIQUE constraint (DO block avoids error on re-run)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname      = 'reports_source_source_url_key'
          AND conrelid     = 'public.reports'::regclass
    ) THEN
        ALTER TABLE public.reports
            ADD CONSTRAINT reports_source_source_url_key
            UNIQUE (source, source_url);
    END IF;
END;
$$;

-- Secondary explicit index (IF NOT EXISTS is idempotent; the constraint above
-- already creates a backing unique index, but this named index is referenced
-- by the scraper layer directly).
CREATE UNIQUE INDEX IF NOT EXISTS reports_source_url_idx
    ON public.reports (source, source_url);
