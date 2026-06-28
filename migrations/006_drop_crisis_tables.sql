-- Drop legacy Crisis VE tables (executed 2026-06-27).
-- These tables (damage_reports, safe_checkins) were from the original Crisis VE
-- earthquake response app and are not used by Reune VE.
-- Already applied to Supabase project bgebvwchqtrhvdhkpzgk — kept for record only.

drop table if exists damage_reports cascade;
drop table if exists safe_checkins cascade;
