CREATE TABLE IF NOT EXISTS horoshop_mirror (
  article TEXT PRIMARY KEY,
  supplier TEXT,
  presence_ua TEXT,
  display_in_showcase BOOLEAN,
  parent_article TEXT,
  price NUMERIC(12, 2),
  raw JSONB,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_horoshop_mirror_supplier ON horoshop_mirror (supplier);
