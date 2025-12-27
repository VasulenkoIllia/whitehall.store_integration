ALTER TABLE column_mappings
  ADD COLUMN IF NOT EXISTS mapping_meta JSONB;
