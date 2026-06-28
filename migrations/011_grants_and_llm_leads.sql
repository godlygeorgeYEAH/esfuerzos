-- migrations/011_grants_and_llm_leads.sql
--
-- (1) Fix: bot_subscribers (migration 010) enabled RLS but service_role was
--     never GRANTed table privileges, so every read/write returned 42501
--     "permission denied". service_role bypasses RLS but still needs the GRANT.
-- (2) New: llm_leads — review queue for records extracted by the LLM panel from
--     volunteer-submitted URLs / pasted text. LLM output NEVER enters the
--     canonical reports table directly; a human approves rows here first.

-- (1) ----------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON public.bot_subscribers TO service_role;

-- (2) ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.llm_leads (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source        text NOT NULL,        -- 'llm:url' | 'llm:whatsapp' | ...
    source_url    text,                 -- origin URL if any
    full_name     text NOT NULL,
    age           int,
    location      text,
    kind          text,                 -- 'missing' | 'found'
    contact       text,
    confidence    float,                -- LLM self-reported 0..1
    context       text,                 -- verbatim source quote (audit trail)
    raw_data      jsonb,
    review_status text NOT NULL DEFAULT 'pending',  -- pending|approved|rejected
    reviewed_at   timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_llm_leads_status     ON public.llm_leads(review_status);
CREATE INDEX IF NOT EXISTS idx_llm_leads_created_at ON public.llm_leads(created_at DESC);
-- Dedup guard: same person from same URL only once
CREATE UNIQUE INDEX IF NOT EXISTS uq_llm_leads_source_url_name
    ON public.llm_leads(source, COALESCE(source_url, ''), full_name);

ALTER TABLE public.llm_leads ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.llm_leads TO service_role;
