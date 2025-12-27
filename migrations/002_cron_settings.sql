CREATE TABLE IF NOT EXISTS cron_settings (
  name TEXT PRIMARY KEY,
  cron TEXT NOT NULL,
  is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO cron_settings (name, cron, is_enabled)
VALUES
  ('import_all', '0 3 * * *', TRUE),
  ('finalize', '30 3 * * *', TRUE),
  ('export', '0 4 * * *', TRUE)
ON CONFLICT (name) DO NOTHING;
