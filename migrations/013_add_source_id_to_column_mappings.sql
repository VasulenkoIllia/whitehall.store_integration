ALTER TABLE column_mappings
  ADD COLUMN IF NOT EXISTS source_id BIGINT REFERENCES sources(id) ON DELETE CASCADE;

UPDATE column_mappings
SET source_id = CASE
  WHEN (mapping_meta->>'source_id') ~ '^[0-9]+$'
    THEN (mapping_meta->>'source_id')::bigint
  ELSE NULL
END
WHERE source_id IS NULL
  AND mapping_meta ? 'source_id';

CREATE INDEX IF NOT EXISTS idx_column_mappings_supplier_source
  ON column_mappings (supplier_id, source_id, created_at DESC);
