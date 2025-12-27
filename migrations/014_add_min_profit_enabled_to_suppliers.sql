ALTER TABLE suppliers
  ADD COLUMN IF NOT EXISTS min_profit_enabled BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE suppliers
SET min_profit_enabled = TRUE
WHERE min_profit_enabled IS NULL;
