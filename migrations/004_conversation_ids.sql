-- Migration 004: conversation_ids table
-- Tracks Base44 conversation IDs for the polling loop.
-- PK is conv_id (Base44's internal conversation ID), not phone.

CREATE TABLE IF NOT EXISTS public.conversation_ids (
  conv_id      TEXT PRIMARY KEY,
  phone        TEXT,
  updated_at   TIMESTAMPTZ DEFAULT now(),
  report_id    UUID REFERENCES public.reports(id),
  last_seen_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_conv_ids_phone ON public.conversation_ids(phone);

GRANT ALL ON public.conversation_ids TO service_role;
