CREATE TABLE IF NOT EXISTS horoshop_api_preview (
  id BIGSERIAL PRIMARY KEY,
  job_id BIGINT,
  article TEXT NOT NULL,
  supplier TEXT,
  presence_ua TEXT,
  display_in_showcase BOOLEAN,
  parent_article TEXT,
  price NUMERIC(12, 2),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_horoshop_api_preview_job ON horoshop_api_preview (job_id);
