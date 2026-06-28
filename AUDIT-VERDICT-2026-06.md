# Reúne VE — Release-Readiness Audit Verdict (2026-06-28)

## VERDICT: 🔴 NO-GO for public release · Score 58/100 (gate 95)

A 4-phase audit (deep-audit → security-review → E2E simulation → scored verdict)
with an independent qa-scorer gate. The bot must NOT be released to the public
until the P0 blockers below are closed. With V4 open and multi-victim reports
overwriting each other, releasing now actively endangers the people it should reunite.

## Open P0 blockers (each fails the gate)
1. **V4 RLS anon-read OPEN** — live mass-PII dump. `migrations/012_drop_anon_read.sql`
   written but NOT applied. Anyone with the public Supabase anon key can `SELECT *`
   all reports+photos. NOT covered by the VPS firewall (Supabase is a separate host).
2. **Unidentified `found` person blocked** — `waha_intake.py` intake requires a name;
   unconscious/unidentified hospital patients (the highest-value case) cannot be registered.
3. **Data-loss: one report per PHONE, not per person** — `conv_key = md5(phone)`; a phone
   reporting multiple missing relatives overwrites all but the last.
4. **Proactive notifier inert in prod** — needs `status='confirmed'` via `/admin/match-review`,
   which is 503 until `ADMIN_KEY` is set + a review path exists.
5. **Ops not reboot-safe / fixes not on main** — firewall not persistent; running off an
   unmerged dirty branch (`audit-fixes-2026-06`).

## Done & verified this audit (on branch audit-fixes-2026-06)
- F2 admin fail-closed (503 when ADMIN_KEY unset) — verified.
- F3 firewall (DOCKER-USER drop :8080/:3000) — verified blocked externally (NOT persistent).
- F5 public photo StaticFiles mounts removed (404) — verified.
- F6 phone hashed in logs, body not logged — verified.
- F7 (partial) waha_whatsapp excluded from TEXT search — NOTE: NOT yet on the FACE path.
- F4 migration 012 written (apply pending).
- E2E suite created (`tests/test_e2e_entities.py`), 20/22 PASS.

## Audit self-corrections (qa-scorer caught these)
- F7 incomplete: face path (`_lookup_match_details`, `_search_face_matches`) still leaks
  private waha_whatsapp reports to strangers.
- Synchronous face disclosure fires at face_score 0.50 and ignores COMBINED_THRESHOLD=0.65
  (code contradicts docstring); no human confirmation on the sync path.
- Rate limit is per-IP (all WAHA traffic = one IP) → legit messages 429'd in a surge.

## Path to 95
P0: apply mig 012 · allow nameless found · per-report conv_key · set ADMIN_KEY + review path ·
    merge to main + clean deploy · persist firewall.
P1: F7 on face path · gate/raise sync face disclosure threshold · WAHA HMAC · per-phone rate limit.
P2: persist conv_state · CI runs the E2E suite · rotate WAHA key.

Re-score after P0+P1 land and E2E is green at 22/22 with the breach closed and the review loop live.
