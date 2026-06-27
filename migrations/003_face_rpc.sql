-- migrations/003_face_rpc.sql
-- Face search RPC: match_reports_by_face
-- Joins photos -> reports and returns report-level results ranked by face similarity.
-- Returns one row per report (best-scoring photo per report is implicit via ORDER BY).
-- Run after 002_match_functions.sql.

CREATE OR REPLACE FUNCTION match_reports_by_face(
    query_embedding  vector(512),
    query_kind       text,
    match_threshold  float,
    match_count      int
)
RETURNS TABLE (
    id               uuid,
    full_name        text,
    face_similarity  float
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        r.id,
        r.full_name,
        (1 - (p.face_embedding <=> query_embedding))::float AS face_similarity
    FROM photos p
    JOIN reports r ON r.id = p.report_id
    WHERE r.kind        = query_kind
      AND p.quality_ok  = true
      AND p.face_embedding IS NOT NULL
      AND (1 - (p.face_embedding <=> query_embedding)) >= match_threshold
    ORDER BY p.face_embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

GRANT EXECUTE
    ON FUNCTION match_reports_by_face(vector(512), text, float, int)
    TO service_role;
