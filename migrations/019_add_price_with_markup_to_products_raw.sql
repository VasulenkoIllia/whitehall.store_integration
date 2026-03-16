ALTER TABLE products_raw
  ADD COLUMN IF NOT EXISTS price_with_markup NUMERIC(12, 2);
