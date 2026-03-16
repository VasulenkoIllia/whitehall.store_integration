CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mrc_active_lookup
  ON markup_rule_conditions (rule_set_id, priority, id, price_from, price_to)
  WHERE is_active = TRUE;
