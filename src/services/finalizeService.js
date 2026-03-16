const db = require('../db');
const logService = require('./logService');
const { finalizeStatementTimeoutMs, finalizeWorkMemMb } = require('../config');

async function buildFinalDataset(jobId) {
  const client = await db.pool.connect();
  const startedAt = Date.now();
  const stageMs = {};
  try {
    await client.query('BEGIN');
    await client.query(`SELECT set_config('application_name', $1, true)`, [
      `whitehall:finalize:${jobId}`
    ]);
    if (Number.isFinite(finalizeStatementTimeoutMs) && finalizeStatementTimeoutMs > 0) {
      const timeoutMs = Math.max(1000, Math.trunc(finalizeStatementTimeoutMs));
      await client.query(`SELECT set_config('statement_timeout', $1, true)`, [String(timeoutMs)]);
    }
    if (Number.isFinite(finalizeWorkMemMb) && finalizeWorkMemMb > 0) {
      const workMemMb = Math.max(4, Math.trunc(finalizeWorkMemMb));
      await client.query(`SELECT set_config('work_mem', $1, true)`, [`${workMemMb}MB`]);
    }

    let stageStartedAt = Date.now();
    const importJobResult = await client.query(
      `SELECT id FROM jobs
       WHERE type = 'import_all'
         AND status = 'success'
       ORDER BY id DESC
       LIMIT 1`
    );
    stageMs.selectImportJob = Date.now() - stageStartedAt;
    const importJobId = importJobResult.rows[0]?.id;
    if (!importJobId) {
      throw new Error('No import_all job found');
    }

    stageStartedAt = Date.now();
    const rawCountResult = await client.query(
      'SELECT COUNT(*) FROM products_raw WHERE job_id = $1',
      [importJobId]
    );
    stageMs.countRaw = Date.now() - stageStartedAt;
    const rawCount = Number(rawCountResult.rows[0].count || 0);

    // Avoid ACCESS EXCLUSIVE lock from TRUNCATE in production.
    // DELETE keeps readers available while finalize transaction is running.
    stageStartedAt = Date.now();
    await client.query('DELETE FROM products_final');
    stageMs.clearFinal = Date.now() - stageStartedAt;

    const insertSql = `
      WITH base AS (
        SELECT
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
          active_rs.id AS effective_rule_set_id
        FROM products_raw pr
        JOIN suppliers s ON s.id = pr.supplier_id
        LEFT JOIN markup_rule_sets active_rs
          ON active_rs.id = s.markup_rule_set_id
         AND active_rs.is_active = TRUE
        WHERE s.is_active = TRUE
          AND pr.job_id = $2
      ),
      computed AS (
        SELECT
          b.article,
          b.size,
          b.quantity,
          b.price_base,
          CASE
            WHEN b.effective_rule_set_id IS NULL THEN b.legacy_price
            WHEN selected_rule.action_type = 'fixed_add' THEN b.price_base + selected_rule.action_value
            WHEN selected_rule.action_type = 'percent' THEN b.price_base * (1 + selected_rule.action_value / 100)
            ELSE b.legacy_price
          END AS price_with_markup,
          b.extra,
          b.supplier_id,
          b.supplier_priority
        FROM base b
        LEFT JOIN LATERAL (
          SELECT
            c.action_type,
            c.action_value
          FROM markup_rule_conditions c
          WHERE c.rule_set_id = b.effective_rule_set_id
            AND c.is_active = TRUE
            AND b.price_base >= c.price_from
            AND (c.price_to IS NULL OR b.price_base < c.price_to)
          ORDER BY c.priority ASC, c.id ASC
          LIMIT 1
        ) selected_rule ON TRUE
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
      stageStartedAt = Date.now();
      await client.query(insertSql, [jobId, importJobId]);
      stageMs.insertFinal = Date.now() - stageStartedAt;
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

    stageStartedAt = Date.now();
    await client.query(
      `UPDATE products_final pf
       SET price_final = po.price_final
       FROM price_overrides po
       WHERE po.is_active = TRUE
         AND pf.article = po.article
         AND NULLIF(pf.size, '') IS NOT DISTINCT FROM NULLIF(po.size, '')`
    );
    stageMs.applyOverrides = Date.now() - stageStartedAt;

    stageStartedAt = Date.now();
    const finalCountResult = await client.query('SELECT COUNT(*) FROM products_final');
    stageMs.countFinal = Date.now() - stageStartedAt;
    const finalCount = Number(finalCountResult.rows[0].count || 0);

    stageStartedAt = Date.now();
    await client.query('COMMIT');
    stageMs.commit = Date.now() - stageStartedAt;

    await logService.log(jobId, 'info', 'Final dataset built', {
      rawCount,
      finalCount,
      importJobId,
      durationMs: Date.now() - startedAt,
      stageMs
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
