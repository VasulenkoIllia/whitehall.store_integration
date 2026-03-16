const db = require('../db');
const logService = require('./logService');
const { finalizeStatementTimeoutMs, finalizeWorkMemMb } = require('../config');

async function buildFinalDataset(jobId) {
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    if (Number.isFinite(finalizeStatementTimeoutMs) && finalizeStatementTimeoutMs > 0) {
      const timeoutMs = Math.max(1000, Math.trunc(finalizeStatementTimeoutMs));
      await client.query(`SELECT set_config('statement_timeout', $1, true)`, [String(timeoutMs)]);
    }
    if (Number.isFinite(finalizeWorkMemMb) && finalizeWorkMemMb > 0) {
      const workMemMb = Math.max(4, Math.trunc(finalizeWorkMemMb));
      await client.query(`SELECT set_config('work_mem', $1, true)`, [`${workMemMb}MB`]);
    }

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

    // Avoid ACCESS EXCLUSIVE lock from TRUNCATE in production.
    // DELETE keeps readers available while finalize transaction is running.
    await client.query('DELETE FROM products_final');

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
          CASE
            WHEN s.min_profit_enabled = TRUE
              AND (pr.price * (1 + s.markup_percent / 100)) - pr.price < s.min_profit_amount
              THEN pr.price + s.min_profit_amount
            ELSE pr.price * (1 + s.markup_percent / 100)
          END AS legacy_price,
          s.markup_rule_set_id AS effective_rule_set_id
        FROM products_raw pr
        JOIN suppliers s ON s.id = pr.supplier_id
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
        JOIN markup_rule_sets rs
          ON rs.id = b.effective_rule_set_id
         AND rs.is_active = TRUE
        JOIN markup_rule_conditions c
          ON c.rule_set_id = rs.id
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
      best_priority AS (
        SELECT
          article,
          size,
          MIN(supplier_priority) AS min_priority
        FROM rounded
        GROUP BY article, size
      ),
      best_price AS (
        SELECT
          r.article,
          r.size,
          MIN(r.price_final) AS min_price_for_priority
        FROM rounded r
        JOIN best_priority bp
          ON bp.article IS NOT DISTINCT FROM r.article
         AND bp.size IS NOT DISTINCT FROM r.size
         AND bp.min_priority = r.supplier_priority
        GROUP BY r.article, r.size
      ),
      filtered AS (
        SELECT r.*
        FROM rounded r
        JOIN best_priority bp
          ON bp.article IS NOT DISTINCT FROM r.article
         AND bp.size IS NOT DISTINCT FROM r.size
        JOIN best_price bpr
          ON bpr.article IS NOT DISTINCT FROM r.article
         AND bpr.size IS NOT DISTINCT FROM r.size
        WHERE r.supplier_priority = bp.min_priority
          AND r.price_final = bpr.min_price_for_priority
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

    try {
      await client.query(insertSql, [jobId, importJobId]);
    } catch (err) {
      if (err?.code === '57014') {
        const timeoutMs = Number.isFinite(finalizeStatementTimeoutMs)
          ? Math.max(1000, Math.trunc(finalizeStatementTimeoutMs))
          : null;
        const timeoutHint = timeoutMs ? `${timeoutMs}ms` : 'configured timeout';
        throw new Error(
          `Finalize query timed out after ${timeoutHint}. Check import volume and indexes.`
        );
      }
      throw err;
    }

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
