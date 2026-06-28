-- migrations/013_waha_sessions.sql
--
-- B3/B4 — Durable per-phone conversation session so a container restart/OOM/
-- redeploy never loses an in-flight report, and in-memory state can be evicted
-- after each message (bounded memory, no OOM at scale).
--
-- state JSONB holds: { conv: [...messages], collected: {...fields}, rkey: "<report key>", searched: bool }.
-- service_role only (PII: conversation content + phone). RLS enabled, no anon policy.

CREATE TABLE IF NOT EXISTS public.waha_sessions (
    phone       text PRIMARY KEY,
    state       jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_waha_sessions_updated ON public.waha_sessions(updated_at);

ALTER TABLE public.waha_sessions ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.waha_sessions TO service_role;

-- Optional housekeeping (run periodically): drop sessions idle > 7 days.
-- DELETE FROM public.waha_sessions WHERE updated_at < now() - interval '7 days';
