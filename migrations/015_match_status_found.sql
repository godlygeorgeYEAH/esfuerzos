-- 015: add 'found' to match_status.
--
-- Lets a human reviewer mark the searched person as located WITHOUT notifying
-- the family. The notifier only acts on status='confirmed', so a 'found' match
-- is recorded and removed from the review queue but never triggers an alert.
--
-- Additive and non-destructive: existing rows are untouched. Idempotent via
-- IF NOT EXISTS. ADD VALUE is not used elsewhere in this file, so it is safe
-- even if the migration runner wraps statements in a transaction.
ALTER TYPE match_status ADD VALUE IF NOT EXISTS 'found';
