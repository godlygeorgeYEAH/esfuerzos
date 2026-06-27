-- damage_reports table
CREATE TABLE damage_reports (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at       TIMESTAMPTZ NOT NULL    DEFAULT now(),
  photo_url        TEXT,
  location_text    TEXT        NOT NULL,
  state            TEXT        NOT NULL,
  lat              FLOAT8,
  lng              FLOAT8,
  risk_level       TEXT        NOT NULL    CHECK (risk_level IN ('BAJO', 'MEDIO', 'ALTO')),
  fema_category    INT                     CHECK (fema_category BETWEEN 1 AND 5),
  damage_type      TEXT,
  ai_analysis      TEXT,
  recommendation   TEXT,
  verified         BOOLEAN     NOT NULL    DEFAULT false,
  false_report     BOOLEAN     NOT NULL    DEFAULT false
);

CREATE INDEX idx_damage_reports_state      ON damage_reports (state);
CREATE INDEX idx_damage_reports_risk_level ON damage_reports (risk_level);
CREATE INDEX idx_damage_reports_created_at ON damage_reports (created_at DESC);

-- safe_checkins table
CREATE TABLE safe_checkins (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  created_at   TIMESTAMPTZ NOT NULL    DEFAULT now(),
  full_name    TEXT        NOT NULL,
  state        TEXT        NOT NULL,
  city         TEXT        NOT NULL,
  message      TEXT        CHECK (char_length(message) <= 200),
  share_token  TEXT        UNIQUE      DEFAULT encode(gen_random_bytes(3), 'hex'),
  verified     BOOLEAN     NOT NULL    DEFAULT false
);

CREATE INDEX idx_safe_checkins_state      ON safe_checkins (state);
CREATE INDEX idx_safe_checkins_full_name  ON safe_checkins (lower(full_name));
CREATE INDEX idx_safe_checkins_created_at ON safe_checkins (created_at DESC);

-- RLS
ALTER TABLE damage_reports  ENABLE ROW LEVEL SECURITY;
ALTER TABLE safe_checkins   ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read damage_reports"
  ON damage_reports FOR SELECT TO anon USING (true);

CREATE POLICY "Public read safe_checkins"
  ON safe_checkins FOR SELECT TO anon USING (true);
