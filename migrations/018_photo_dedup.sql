-- 018: same-photo detection for the face-matching pipeline + hospital CI dedup.
--
-- Found 2026-07-01: several aggregator sources (venezreporta, venezuela_te_busca,
-- reconexion) re-host each other's photos. The face pipeline was counting a
-- re-hosted copy of the SAME photo as independent "cross-source corroboration"
-- (face_score up to 1.0), which is not a real signal — it is the same picture
-- scraped twice, not two sightings of the same person. Purely additive.
ALTER TABLE photos
  ADD COLUMN IF NOT EXISTS phash text;              -- 64-bit dHash, hex string

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS same_photo_suspected boolean DEFAULT false;
  -- true = the two photos are likely the same underlying image re-hosted
  -- across sources (phash near-duplicate, or a known re-hosting URL/filename
  -- pattern) -> NOT independent corroboration, exclude from "confirmed" review.
