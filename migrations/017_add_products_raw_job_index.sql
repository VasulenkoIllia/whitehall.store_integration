CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_raw_job
  ON products_raw (job_id);
