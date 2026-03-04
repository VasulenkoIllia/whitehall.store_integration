const db = require('../db');
const logService = require('./logService');

async function buildFinalDataset(jobId) {
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');

    const importJobResult = await client.query(
      `SELECT id FROM jobs
       WHERE type = 'import_all'
       ORDER BY id DESC
       LIMIT 1`
    );
    const importJobId = importJobResult.rows[0]?.id;
    if (!importJobId) {
      throw new Error('No import_all job found');
    }

    const rawCountResult = await client.query(
      'SELECT COUNT(*) FROM products_raw WHERE job_id = $1',
      [importJobId]
    );
    const rawCount = Number(rawCountResult.rows[0].count || 0);

    await client.query('TRUNCATE products_final');

    const insertSql = `
      WITH base AS (
        SELECT
          pr.id AS raw_id,
          pr.article,
          pr.size,
          pr.quantity,
          pr.price AS price_base,
          pr.extra,
          pr.supplier_id,
          s.priority AS supplier_priority,
          COALESCE(s.pricing_mode, 'legacy') AS pricing_mode,
          CASE
            WHEN s.min_profit_enabled = TRUE
              AND (pr.price * (1 + s.markup_percent / 100)) - pr.price < s.min_profit_amount
              THEN pr.price + s.min_profit_amount
            ELSE pr.price * (1 + s.markup_percent / 100)
          END AS legacy_price,
          CASE
            WHEN COALESCE(s.pricing_mode, 'legacy') = 'custom' THEN s.markup_rule_set_id
            WHEN COALESCE(s.pricing_mode, 'legacy') = 'global' THEN ms.global_rule_set_id
            ELSE NULL
          END AS effective_rule_set_id
        FROM products_raw pr
        JOIN suppliers s ON s.id = pr.supplier_id
        LEFT JOIN markup_settings ms ON ms.id = 1
        WHERE s.is_active = TRUE
          AND pr.job_id = $2
      ),
      rule_match AS (
        SELECT
          b.raw_id,
          c.action_type,
          c.action_value,
          ROW_NUMBER() OVER (PARTITION BY b.raw_id ORDER BY c.priority ASC, c.id ASC) AS rn
        FROM base b
        JOIN markup_rule_conditions c
          ON c.rule_set_id = b.effective_rule_set_id
         AND c.is_active = TRUE
         AND b.price_base >= c.price_from
         AND (c.price_to IS NULL OR b.price_base < c.price_to)
      ),
      computed AS (
        SELECT
          b.article,
          b.size,
          b.quantity,
          b.price_base,
          CASE
            WHEN b.pricing_mode = 'legacy' THEN b.legacy_price
            WHEN b.effective_rule_set_id IS NULL THEN b.legacy_price
            WHEN rm.raw_id IS NULL THEN b.legacy_price
            WHEN rm.action_type = 'fixed_add' THEN b.price_base + rm.action_value
            WHEN rm.action_type = 'percent' THEN b.price_base * (1 + rm.action_value / 100)
            ELSE b.legacy_price
          END AS price_with_markup,
          b.extra,
          b.supplier_id,
          b.supplier_priority
        FROM base b
        LEFT JOIN rule_match rm
          ON rm.raw_id = b.raw_id
         AND rm.rn = 1
      ),
      rounded AS (
        SELECT
          article,
          size,
          quantity,
          price_base,
          CEIL(price_with_markup / 10) * 10 AS price_final,
          extra,
          supplier_id,
          supplier_priority
        FROM computed
      ),
      ranked AS (
        SELECT
          *,
          MIN(supplier_priority) OVER (PARTITION BY article, size) AS min_priority,
          MIN(price_final) OVER (PARTITION BY article, size, supplier_priority) AS min_price_for_priority
        FROM rounded
      ),
      filtered AS (
        SELECT *
        FROM ranked
        WHERE supplier_priority = min_priority
          AND price_final = min_price_for_priority
      )
      INSERT INTO products_final (
        job_id,
        article,
        size,
        quantity,
        price_base,
        price_final,
        extra,
        supplier_id
      )
      SELECT
        $1,
        article,
        size,
        quantity,
        price_base,
        price_final,
        extra,
        supplier_id
      FROM filtered;
    `;

    await client.query(insertSql, [jobId, importJobId]);

    await client.query(
      `UPDATE products_final pf
       SET price_final = po.price_final
       FROM price_overrides po
       WHERE po.is_active = TRUE
         AND pf.article = po.article
         AND NULLIF(pf.size, '') IS NOT DISTINCT FROM NULLIF(po.size, '')`
    );

    const finalCountResult = await client.query('SELECT COUNT(*) FROM products_final');
    const finalCount = Number(finalCountResult.rows[0].count || 0);

    await client.query('COMMIT');

    await logService.log(jobId, 'info', 'Final dataset built', {
      rawCount,
      finalCount,
      importJobId
    });

    return { rawCount, finalCount };
  } catch (err) {
    await client.query('ROLLBACK');
    await logService.log(jobId, 'error', 'Final dataset build failed', { error: err.message });
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  buildFinalDataset
};
