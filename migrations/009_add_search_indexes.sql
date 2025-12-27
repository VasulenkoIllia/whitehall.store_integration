CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_products_raw_article_trgm
  ON products_raw USING GIN (article gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_products_raw_extra_trgm
  ON products_raw USING GIN (extra gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_products_final_article_trgm
  ON products_final USING GIN (article gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_products_final_extra_trgm
  ON products_final USING GIN (extra gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_horoshop_mirror_article_trgm
  ON horoshop_mirror USING GIN (article gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_horoshop_api_preview_article_trgm
  ON horoshop_api_preview USING GIN (article gin_trgm_ops);
