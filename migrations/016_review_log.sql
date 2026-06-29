-- 016: audit log for human review actions on matches.
--
-- Records every approve/found/reject decision made from the admin dashboard.
-- Names and sources are denormalised so the log is self-contained even if
-- the underlying reports or matches are later deleted.
-- Purely additive; no existing tables are modified.
CREATE TABLE IF NOT EXISTS match_review_log (
    id             bigint generated always as identity primary key,
    match_id       uuid        not null,
    decision       text        not null,   -- 'confirmed' | 'found' | 'rejected'
    missing_name   text,
    found_name     text,
    missing_source text,
    found_source   text,
    reviewed_at    timestamptz not null default now()
);

CREATE INDEX IF NOT EXISTS match_review_log_reviewed_at_idx
    ON match_review_log (reviewed_at DESC);
