-- Set ivfflat.probes = 5 inside each RPC for better recall.
-- Default is 1 (fast, low recall). 5 checks 5 of 50 lists = 10% coverage
-- before falling to exact scan — good balance for a crisis dataset.
--
-- match_reports_by_text converted to plpgsql so SET LOCAL is valid.

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
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    SET LOCAL ivfflat.probes = 5;
    RETURN QUERY
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
END;
$$;

GRANT EXECUTE
    ON FUNCTION match_reports_by_text(vector, float, int)
    TO service_role;


-- Update face RPC to also use probes = 5
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
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    SET LOCAL ivfflat.probes = 5;
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
