-- migrations/009_fix_rpc_volatile.sql
--
-- Fix: migration 008 put `SET LOCAL ivfflat.probes = 5` INSIDE the function
-- body while the function is declared STABLE. Postgres rejects this at call
-- time with: "SET is not allowed in a non-volatile function". Result: BOTH
-- match_reports_by_face and match_reports_by_text returned HTTP 400 on every
-- call, so face matching (and text matching, once 008 was applied) was fully
-- broken.
--
-- Correct approach: declare the probes GUC with the function-level
-- `SET ivfflat.probes = 5` clause. That sets the GUC for the duration of the
-- call WITHOUT an in-body SET statement, and IS allowed for STABLE functions.
-- Reverts both functions to plain `LANGUAGE sql STABLE`.

-- ---------------------------------------------------------------------------
-- Face matching RPC
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS match_reports_by_face(vector(512), report_kind, float, int);

CREATE OR REPLACE FUNCTION match_reports_by_face(
    query_embedding  vector(512),
    query_kind       report_kind,
    match_threshold  float,
    match_count      int
)
RETURNS TABLE (
    report_id   uuid,
    full_name   text,
    similarity  float
)
LANGUAGE sql STABLE
SET ivfflat.probes = 5
AS $$
    SELECT
        r.id                                                 AS report_id,
        r.full_name,
        (1 - (p.face_embedding <=> query_embedding))::float  AS similarity
    FROM  photos  p
    JOIN  reports r ON r.id = p.report_id
    WHERE r.kind           = query_kind
      AND p.quality_ok     = true
      AND p.face_embedding IS NOT NULL
      AND (1 - (p.face_embedding <=> query_embedding)) >= match_threshold
    ORDER BY p.face_embedding <=> query_embedding
    LIMIT match_count;
$$;

GRANT EXECUTE
    ON FUNCTION match_reports_by_face(vector(512), report_kind, float, int)
    TO service_role;

-- ---------------------------------------------------------------------------
-- Text matching RPC
-- ---------------------------------------------------------------------------
DROP FUNCTION IF EXISTS match_reports_by_text(vector, float, int);

CREATE OR REPLACE FUNCTION match_reports_by_text(
    query_embedding  vector,
    match_threshold  float,
    match_count      int
)
RETURNS TABLE (
    id          uuid,
    full_name   text,
    source      text,
    kind        text,
    similarity  float
)
LANGUAGE sql STABLE
SET ivfflat.probes = 5
AS $$
    SELECT
        r.id,
        r.full_name,
        r.source,
        r.kind,
        (1 - (r.text_embedding <=> query_embedding))::float AS similarity
    FROM reports r
    WHERE r.text_embedding IS NOT NULL
      AND (1 - (r.text_embedding <=> query_embedding)) >= match_threshold
    ORDER BY r.text_embedding <=> query_embedding
    LIMIT match_count;
$$;

GRANT EXECUTE
    ON FUNCTION match_reports_by_text(vector, float, int)
    TO service_role;
