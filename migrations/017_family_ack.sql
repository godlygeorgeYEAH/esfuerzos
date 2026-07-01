-- 017: let a family self-acknowledge a proposed match from the bot chat, not
-- only from the admin dashboard.
--
-- family_ack is a SIGNAL, not a decision: it never changes `status`. A human
-- still must approve/dismiss via /admin/match-review before the notifier fires
-- or anyone is told the match is real. This exists to jump self-acked matches
-- to the top of the human review queue (see /admin/matches ORDER BY).
-- Purely additive; no existing columns/constraints touched.
ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS family_ack text,        -- 'yes' | 'no', reporter's own claim
  ADD COLUMN IF NOT EXISTS family_ack_at timestamptz,
  ADD COLUMN IF NOT EXISTS family_ack_side text;    -- 'missing' | 'found' — which side replied
