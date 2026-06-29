-- migrations/014_canonical_view.sql
--
-- Canonical (deduplicated) reports view.
--
-- dedup_pipeline.py clusters near-duplicate reports (same person reported by
-- several sources) and annotates the NON-canonical rows with
-- raw_data->>'possible_duplicate_of' (pointing at the canonical record). It does
-- NOT delete or merge rows. As of 2026-06 it had marked ~12,193 of ~82,768 rows.
--
-- The canonical set is therefore every report that is NOT itself a marked
-- duplicate. This view exposes that clean, one-row-per-person base so the bot
-- search, the admin dashboard, and any export read deduplicated data instead of
-- showing the same person 2-3 times.
--
-- Read-only view over `reports`; no data is moved. Safe to re-run (idempotent).

CREATE OR REPLACE VIEW canonical_reports AS
SELECT *
FROM reports
WHERE raw_data->>'possible_duplicate_of' IS NULL;

GRANT SELECT ON canonical_reports TO service_role;

-- Optional convenience: a count helper is not needed; query directly, e.g.
--   SELECT count(*) FROM canonical_reports;          -- unique persons
--   SELECT count(*) FROM reports;                    -- raw rows incl. duplicates
