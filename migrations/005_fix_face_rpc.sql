-- migrations/005_fix_face_rpc.sql
-- Fix match_reports_by_face:
--   1. Change query_kind from text to report_kind (fixes type mismatch in WHERE)
--   2. Return report_id (not id) and similarity (not face_similarity) to match face_pipeline.py
-- Also adds IVFFlat index on photos.face_embedding if not present.

-- Drop old overload (text signature) so CREATE OR REPLACE works cleanly
DROP FUNCTION IF EXISTS match_reports_by_face(vector(512), text, float, int);

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
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        r.id                                                          AS report_id,
        r.full_name,
        (1 - (p.face_embedding <=> query_embedding))::float          AS similarity
    FROM  photos  p
    JOIN  reports r ON r.id = p.report_id
    WHERE r.kind             = query_kind
      AND p.quality_ok       = true
      AND p.face_embedding   IS NOT NULL
      AND (1 - (p.face_embedding <=> query_embedding)) >= match_threshold
    ORDER BY p.face_embedding <=> query_embedding
    LIMIT match_count;
END;
$$;

GRANT EXECUTE
    ON FUNCTION match_reports_by_face(vector(512), report_kind, float, int)
    TO service_role;

-- IVFFlat index on photos.face_embedding (approximate cosine search)
CREATE INDEX IF NOT EXISTS idx_photos_face_ivfflat
    ON photos USING ivfflat (face_embedding vector_cosine_ops)
    WITH (lists = 100)
    WHERE quality_ok = true;
