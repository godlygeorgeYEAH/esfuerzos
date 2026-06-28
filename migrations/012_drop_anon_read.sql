-- migrations/012_drop_anon_read.sql
--
-- F4/V4 — Close mass-enumeration of PII. Migration 002 created:
--   CREATE POLICY "Public read reports" ON reports FOR SELECT TO anon USING (true);
--   CREATE POLICY "Public read photos"  ON photos  FOR SELECT TO anon USING (true);
-- With those, anyone holding the (public, publishable) Supabase anon key could
-- `SELECT *` the entire reports/photos corpus — full_name, age, last_seen_location,
-- distinguishing_marks, source_url, storage_path — for every crisis-affected person.
--
-- The bot/API uses the service_role key, which BYPASSES RLS, so dropping these
-- anon policies does NOT affect any runtime path. There is no public frontend
-- querying these tables directly. RLS stays ENABLED; with no anon policy, anon
-- SELECT is denied by default.

DROP POLICY IF EXISTS "Public read reports" ON public.reports;
DROP POLICY IF EXISTS "Public read photos"  ON public.photos;

-- Defensive: ensure RLS remains enabled (no-op if already enabled).
ALTER TABLE public.reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.photos  ENABLE ROW LEVEL SECURITY;
