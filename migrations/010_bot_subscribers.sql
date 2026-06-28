-- migrations/010_bot_subscribers.sql
--
-- Maps a WhatsApp reporter's phone to their report so that a background
-- cross-match found LATER (text/face cross-match jobs) can notify the family.
--
-- Why a separate table: reports.source_url stores the phone only as an
-- irreversible hash (waha:{md5(phone)}), so the phone cannot be recovered from
-- the report. This table holds the plaintext phone (PII) in one RLS-locked
-- place, keyed by report_id.
--
-- One row per report_id. Because each phone reuses the same source_url
-- (waha:{md5(phone)}), a phone maps to exactly one report_id, so this is
-- effectively one row per phone.

CREATE TABLE IF NOT EXISTS bot_subscribers (
    report_id        uuid PRIMARY KEY REFERENCES reports(id) ON DELETE CASCADE,
    phone            text NOT NULL,
    full_name        text,          -- what the family reported, for the message
    kind             text,          -- 'missing' | 'found'
    created_at       timestamptz NOT NULL DEFAULT now(),
    last_notified_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_bot_subscribers_phone ON bot_subscribers(phone);

-- PII: phone numbers of crisis-affected people. Lock to service_role only.
ALTER TABLE bot_subscribers ENABLE ROW LEVEL SECURITY;

-- No anon/public policies created — only the service_role key (which bypasses
-- RLS) can read/write. This is intentional.

-- notify_sent already exists on the matches table (migration 002). The notifier
-- flips it to true after a successful WhatsApp send so a match is never sent twice.
