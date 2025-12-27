ALTER TABLE sources
  ADD COLUMN IF NOT EXISTS name TEXT;

UPDATE sources
SET name = COALESCE(NULLIF(name, ''), NULLIF(sheet_name, ''), 'Source ' || id)
WHERE name IS NULL OR name = '';

ALTER TABLE sources
  ALTER COLUMN name SET NOT NULL,
  ALTER COLUMN name SET DEFAULT 'Source';
