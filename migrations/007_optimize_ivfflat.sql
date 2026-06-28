-- Rebuild IVFFlat indexes with lists=50 (appropriate for < 10k rows).
-- At 100 lists the index is oversized for small datasets and wastes RAM.
-- Revisit when reports > 10k: increase lists to 200 and run REINDEX CONCURRENTLY.

DROP INDEX IF EXISTS idx_reports_text_ivfflat;
CREATE INDEX idx_reports_text_ivfflat
    ON reports USING ivfflat (text_embedding vector_cosine_ops)
    WITH (lists = 50);

DROP INDEX IF EXISTS idx_photos_face_ivfflat;
CREATE INDEX idx_photos_face_ivfflat
    ON photos USING ivfflat (face_embedding vector_cosine_ops)
    WITH (lists = 50)
    WHERE quality_ok = true;
