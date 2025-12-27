ALTER TABLE suppliers
  ADD COLUMN IF NOT EXISTS min_profit_amount NUMERIC(12, 2) NOT NULL DEFAULT 500;

UPDATE suppliers
SET min_profit_amount = 500
WHERE min_profit_amount IS NULL;
