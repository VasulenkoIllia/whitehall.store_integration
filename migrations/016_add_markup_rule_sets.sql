CREATE TABLE IF NOT EXISTS markup_rule_sets (
  id BIGSERIAL PRIMARY KEY,
  code TEXT UNIQUE,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS markup_rule_conditions (
  id BIGSERIAL PRIMARY KEY,
  rule_set_id BIGINT NOT NULL REFERENCES markup_rule_sets(id) ON DELETE CASCADE,
  priority INT NOT NULL DEFAULT 100,
  price_from NUMERIC(12, 2) NOT NULL,
  price_to NUMERIC(12, 2),
  action_type TEXT NOT NULL,
  action_value NUMERIC(12, 2) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT markup_rule_conditions_price_from_non_negative CHECK (price_from >= 0),
  CONSTRAINT markup_rule_conditions_price_to_gt_from CHECK (price_to IS NULL OR price_to > price_from),
  CONSTRAINT markup_rule_conditions_action_value_non_negative CHECK (action_value >= 0)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'markup_rule_conditions_action_type_check'
  ) THEN
    ALTER TABLE markup_rule_conditions
      ADD CONSTRAINT markup_rule_conditions_action_type_check
      CHECK (action_type IN ('percent', 'fixed_add'));
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_markup_rule_conditions_rule_set
  ON markup_rule_conditions (rule_set_id);

CREATE INDEX IF NOT EXISTS idx_markup_rule_conditions_match
  ON markup_rule_conditions (rule_set_id, is_active, priority, price_from, price_to);

CREATE TABLE IF NOT EXISTS markup_settings (
  id INT PRIMARY KEY,
  global_rule_set_id BIGINT REFERENCES markup_rule_sets(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT markup_settings_single_row CHECK (id = 1)
);

ALTER TABLE suppliers
  ADD COLUMN IF NOT EXISTS pricing_mode TEXT NOT NULL DEFAULT 'legacy';

ALTER TABLE suppliers
  ADD COLUMN IF NOT EXISTS markup_rule_set_id BIGINT REFERENCES markup_rule_sets(id) ON DELETE SET NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'suppliers_pricing_mode_check'
  ) THEN
    ALTER TABLE suppliers
      ADD CONSTRAINT suppliers_pricing_mode_check
      CHECK (pricing_mode IN ('legacy', 'global', 'custom'));
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_suppliers_markup_rule_set_id
  ON suppliers (markup_rule_set_id);

INSERT INTO markup_rule_sets (code, name, description, is_active)
VALUES (
  'default_v1',
  'Базове правило націнки',
  'Автоматично створений стартовий набір націнок.',
  TRUE
)
ON CONFLICT (code) DO NOTHING;

WITH default_set AS (
  SELECT id
  FROM markup_rule_sets
  WHERE code = 'default_v1'
),
needs_seed AS (
  SELECT ds.id
  FROM default_set ds
  WHERE NOT EXISTS (
    SELECT 1
    FROM markup_rule_conditions c
    WHERE c.rule_set_id = ds.id
  )
)
INSERT INTO markup_rule_conditions (
  rule_set_id,
  priority,
  price_from,
  price_to,
  action_type,
  action_value,
  is_active
)
SELECT
  ns.id,
  seed.priority,
  seed.price_from,
  seed.price_to,
  seed.action_type,
  seed.action_value,
  TRUE
FROM needs_seed ns
CROSS JOIN (
  VALUES
    (10, 0::numeric, 1100::numeric, 'fixed_add'::text, 300::numeric),
    (20, 1100::numeric, 7000::numeric, 'percent'::text, 27::numeric),
    (30, 7000::numeric, 10000::numeric, 'percent'::text, 20::numeric),
    (40, 10000::numeric, NULL::numeric, 'percent'::text, 14::numeric)
) AS seed(priority, price_from, price_to, action_type, action_value);

INSERT INTO markup_settings (id, global_rule_set_id, updated_at)
SELECT 1, rs.id, NOW()
FROM markup_rule_sets rs
WHERE rs.code = 'default_v1'
ON CONFLICT (id) DO UPDATE
SET global_rule_set_id = COALESCE(markup_settings.global_rule_set_id, EXCLUDED.global_rule_set_id),
    updated_at = NOW();

CREATE OR REPLACE FUNCTION compute_markup_price(base_price NUMERIC, supplier_ref BIGINT)
RETURNS NUMERIC
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  supplier_row RECORD;
  selected_rule_set_id BIGINT;
  global_rule_set_id BIGINT;
  condition_row RECORD;
  legacy_price NUMERIC;
BEGIN
  IF base_price IS NULL OR base_price <= 0 THEN
    RETURN NULL;
  END IF;

  SELECT
    s.markup_percent,
    s.min_profit_enabled,
    s.min_profit_amount,
    COALESCE(s.pricing_mode, 'legacy') AS pricing_mode,
    s.markup_rule_set_id
  INTO supplier_row
  FROM suppliers s
  WHERE s.id = supplier_ref;

  IF NOT FOUND THEN
    RETURN base_price;
  END IF;

  legacy_price := CASE
    WHEN supplier_row.min_profit_enabled = TRUE
      AND (base_price * (1 + supplier_row.markup_percent / 100)) - base_price < supplier_row.min_profit_amount
      THEN base_price + supplier_row.min_profit_amount
    ELSE base_price * (1 + supplier_row.markup_percent / 100)
  END;

  IF supplier_row.pricing_mode = 'legacy' THEN
    RETURN legacy_price;
  END IF;

  IF supplier_row.pricing_mode = 'custom' THEN
    selected_rule_set_id := supplier_row.markup_rule_set_id;
  ELSIF supplier_row.pricing_mode = 'global' THEN
    SELECT ms.global_rule_set_id
    INTO global_rule_set_id
    FROM markup_settings ms
    WHERE ms.id = 1;

    selected_rule_set_id := global_rule_set_id;
  ELSE
    RETURN legacy_price;
  END IF;

  IF selected_rule_set_id IS NULL THEN
    RETURN legacy_price;
  END IF;

  SELECT
    c.action_type,
    c.action_value
  INTO condition_row
  FROM markup_rule_conditions c
  WHERE c.rule_set_id = selected_rule_set_id
    AND c.is_active = TRUE
    AND base_price >= c.price_from
    AND (c.price_to IS NULL OR base_price < c.price_to)
  ORDER BY c.priority ASC, c.id ASC
  LIMIT 1;

  IF NOT FOUND THEN
    RETURN legacy_price;
  END IF;

  IF condition_row.action_type = 'fixed_add' THEN
    RETURN base_price + condition_row.action_value;
  END IF;

  IF condition_row.action_type = 'percent' THEN
    RETURN base_price * (1 + condition_row.action_value / 100);
  END IF;

  RETURN legacy_price;
END;
$$;
