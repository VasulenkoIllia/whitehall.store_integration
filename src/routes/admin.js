const express = require('express');
const db = require('../db');
const { scheduleTasks } = require('../jobs/scheduler');
const { getSheetPreview, listSheetNames } = require('../services/googleSheetsService');
const jobService = require('../services/jobService');
const logService = require('../services/logService');
const { jobTimeoutMinutes } = require('../config');

const router = express.Router();

async function applyJobTimeouts() {
  if (!Number.isFinite(jobTimeoutMinutes) || jobTimeoutMinutes <= 0) {
    return;
  }
  const timedOut = await jobService.timeoutStaleJobs(jobTimeoutMinutes);
  for (const job of timedOut) {
    await logService.log(job.id, 'error', 'Job timed out', {
      timeoutMinutes: jobTimeoutMinutes,
      jobType: job.type
    });
  }
}

async function ensureCronTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS cron_settings (
      name TEXT PRIMARY KEY,
      cron TEXT NOT NULL,
      is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );

  await db.query(
    'ALTER TABLE cron_settings ADD COLUMN IF NOT EXISTS meta JSONB NOT NULL DEFAULT \'{}\'::jsonb'
  );

  await db.query(
    `INSERT INTO cron_settings (name, cron, is_enabled, meta)
     VALUES
       ('update_pipeline', '0 3 * * *', TRUE, '{"supplier":"drop"}'),
       ('horoshop_sync', '30 1 * * *', TRUE, '{}'::jsonb),
       ('cleanup', '15 2 * * *', TRUE, '{}'::jsonb)
     ON CONFLICT (name) DO NOTHING`
  );
}

async function ensureHoroshopMirrorTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS horoshop_mirror (
      article TEXT PRIMARY KEY,
      supplier TEXT,
      presence_ua TEXT,
      display_in_showcase BOOLEAN,
      parent_article TEXT,
      price NUMERIC(12, 2),
      raw JSONB,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      seen_at TIMESTAMPTZ
    )`
  );
  await db.query(
    'CREATE INDEX IF NOT EXISTS idx_horoshop_mirror_supplier ON horoshop_mirror (supplier)'
  );
}

async function ensureHoroshopApiPreviewTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS horoshop_api_preview (
      id BIGSERIAL PRIMARY KEY,
      job_id BIGINT,
      article TEXT NOT NULL,
      supplier TEXT,
      presence_ua TEXT,
      display_in_showcase BOOLEAN,
      parent_article TEXT,
      price NUMERIC(12, 2),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
}

function buildUpdateClause(fields, values) {
  const updates = [];
  Object.keys(fields).forEach((key) => {
    if (typeof fields[key] !== 'undefined') {
      values.push(fields[key]);
      updates.push(`${key} = $${values.length}`);
    }
  });
  return updates;
}

router.get('/suppliers', async (req, res, next) => {
  try {
    const result = await db.query(
      `SELECT id, name, markup_percent, priority, min_profit_enabled, min_profit_amount,
              is_active, created_at
       FROM suppliers ORDER BY id ASC`
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

router.post('/suppliers', async (req, res, next) => {
  try {
    const { name, markup_percent, priority, min_profit_enabled, min_profit_amount } = req.body;
    if (!name) {
      return res.status(400).json({ error: 'name is required' });
    }
    const priorityValue = Number.isFinite(Number(priority)) ? Number(priority) : 100;
    const minProfitEnabled =
      typeof min_profit_enabled === 'boolean' ? min_profit_enabled : true;
    const minProfitAmount = minProfitEnabled
      ? Math.max(0, Number.isFinite(Number(min_profit_amount)) ? Number(min_profit_amount) : 0)
      : 0;
    const result = await db.query(
      `INSERT INTO suppliers (name, markup_percent, priority, min_profit_enabled, min_profit_amount)
       VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [name, markup_percent || 0, priorityValue, minProfitEnabled, minProfitAmount]
    );
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'supplier name already exists' });
    }
    next(err);
  }
});

router.put('/suppliers/bulk', async (req, res, next) => {
  try {
    const { supplier_ids, markup_percent } = req.body;
    if (!Array.isArray(supplier_ids) || !supplier_ids.length) {
      return res.status(400).json({ error: 'supplier_ids are required' });
    }
    if (typeof markup_percent === 'undefined' || markup_percent === null) {
      return res.status(400).json({ error: 'markup_percent is required' });
    }
    const ids = supplier_ids.map((id) => Number(id)).filter((id) => Number.isFinite(id));
    if (!ids.length) {
      return res.status(400).json({ error: 'supplier_ids are invalid' });
    }

    const result = await db.query(
      'UPDATE suppliers SET markup_percent = $1 WHERE id = ANY($2::bigint[]) RETURNING id',
      [markup_percent, ids]
    );
    res.json({ updated: result.rowCount || 0 });
  } catch (err) {
    next(err);
  }
});

router.put('/suppliers/:id', async (req, res, next) => {
  try {
    const values = [];
    const updates = buildUpdateClause(
      {
        name: req.body.name,
        markup_percent: req.body.markup_percent,
        priority: req.body.priority,
        is_active: req.body.is_active,
        min_profit_amount:
          typeof req.body.min_profit_enabled === 'boolean' && req.body.min_profit_enabled === false
            ? 0
            : Number.isFinite(Number(req.body.min_profit_amount))
              ? Math.max(0, Number(req.body.min_profit_amount))
              : undefined,
        min_profit_enabled: req.body.min_profit_enabled
      },
      values
    );
    if (!updates.length) {
      return res.status(400).json({ error: 'no fields to update' });
    }
    values.push(req.params.id);
    const result = await db.query(
      `UPDATE suppliers SET ${updates.join(', ')} WHERE id = $${values.length} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'supplier name already exists' });
    }
    next(err);
  }
});

router.delete('/suppliers/:id', async (req, res, next) => {
  try {
    const result = await db.query('DELETE FROM suppliers WHERE id = $1 RETURNING *', [
      req.params.id
    ]);
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

router.get('/sources', async (req, res, next) => {
  try {
    const supplierId = req.query.supplierId;
    const values = [];
    let whereClause = '';
    if (supplierId) {
      values.push(supplierId);
      whereClause = 'WHERE s.supplier_id = $1';
    }
    const result = await db.query(
      `SELECT s.id, s.supplier_id, s.name, sp.name AS supplier_name, s.source_type, s.source_url,
              s.sheet_name, s.is_active, s.created_at
       FROM sources s
       JOIN suppliers sp ON sp.id = s.supplier_id
       ${whereClause}
       ORDER BY s.id ASC`,
      values
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

router.post('/sources', async (req, res, next) => {
  try {
    const { supplier_id, source_type, source_url, sheet_name, name } = req.body;
    if (!supplier_id || !source_type || !source_url) {
      return res.status(400).json({ error: 'supplier_id, source_type, source_url are required' });
    }
    const result = await db.query(
      'INSERT INTO sources (supplier_id, source_type, source_url, sheet_name, name) VALUES ($1, $2, $3, $4, $5) RETURNING *',
      [supplier_id, source_type, source_url, sheet_name || null, name || sheet_name || 'Source']
    );
    res.json(result.rows[0]);
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'source already exists' });
    }
    next(err);
  }
});

router.put('/sources/:id', async (req, res, next) => {
  try {
    const values = [];
    const updates = buildUpdateClause(
      {
        supplier_id: req.body.supplier_id,
        source_type: req.body.source_type,
        source_url: req.body.source_url,
        sheet_name: req.body.sheet_name,
        name: req.body.name,
        is_active: req.body.is_active
      },
      values
    );
    if (!updates.length) {
      return res.status(400).json({ error: 'no fields to update' });
    }
    values.push(req.params.id);
    const result = await db.query(
      `UPDATE sources SET ${updates.join(', ')} WHERE id = $${values.length} RETURNING *`,
      values
    );
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

router.delete('/sources/:id', async (req, res, next) => {
  try {
    const result = await db.query(
      'UPDATE sources SET is_active = FALSE WHERE id = $1 RETURNING *',
      [req.params.id]
    );
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

router.get('/mappings/:supplierId', async (req, res, next) => {
  try {
    const sourceId = req.query.sourceId ? Number(req.query.sourceId) : null;
    let result;
    if (Number.isFinite(sourceId) && sourceId > 0) {
      result = await db.query(
        `SELECT id, supplier_id, source_id, mapping, header_row, mapping_meta, created_at
         FROM column_mappings
         WHERE supplier_id = $1 AND (source_id = $2 OR source_id IS NULL)
         ORDER BY (source_id = $2) DESC, id DESC
         LIMIT 1`,
        [req.params.supplierId, sourceId]
      );
    } else {
      result = await db.query(
        `SELECT id, supplier_id, source_id, mapping, header_row, mapping_meta, created_at
         FROM column_mappings
         WHERE supplier_id = $1
         ORDER BY id DESC
         LIMIT 1`,
        [req.params.supplierId]
      );
    }
    res.json(result.rows[0] || null);
  } catch (err) {
    next(err);
  }
});

router.post('/mappings/:supplierId', async (req, res, next) => {
  try {
    const { mapping, header_row, mapping_meta, source_id } = req.body;
    if (!mapping) {
      return res.status(400).json({ error: 'mapping is required' });
    }
    const headerValue = Number.isFinite(Number(header_row)) ? Number(header_row) : null;
    const sourceValue = Number.isFinite(Number(source_id))
      ? Number(source_id)
      : Number.isFinite(Number(mapping_meta?.source_id))
        ? Number(mapping_meta.source_id)
        : null;
    const result = await db.query(
      `INSERT INTO column_mappings (supplier_id, source_id, mapping, header_row, mapping_meta)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [req.params.supplierId, sourceValue, mapping, headerValue, mapping_meta || null]
    );
    res.json(result.rows[0]);
  } catch (err) {
    next(err);
  }
});

router.get('/jobs', async (req, res, next) => {
  try {
    await applyJobTimeouts();
    const limit = Number(req.query.limit || 50);
    const result = await db.query(
      'SELECT id, type, status, meta, started_at, finished_at, created_at FROM jobs ORDER BY id DESC LIMIT $1',
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

router.get('/logs', async (req, res, next) => {
  try {
    const jobId = req.query.jobId;
    const level = req.query.level;
    const limit = Number(req.query.limit || 200);
    if (!jobId) {
      if (level) {
        const result = await db.query(
          'SELECT id, job_id, level, message, data, created_at FROM logs WHERE level = $1 ORDER BY id DESC LIMIT $2',
          [level, limit]
        );
        return res.json(result.rows);
      }
      const result = await db.query(
        'SELECT id, job_id, level, message, data, created_at FROM logs ORDER BY id DESC LIMIT $1',
        [limit]
      );
      return res.json(result.rows);
    }
    const result = await db.query(
      'SELECT id, job_id, level, message, data, created_at FROM logs WHERE job_id = $1 ORDER BY id DESC LIMIT $2',
      [jobId, limit]
    );
    return res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

router.get('/stats', async (req, res, next) => {
  try {
    await applyJobTimeouts();
    const [
      suppliers,
      sources,
      raw,
      final,
      lastJob,
      lastHoroshopJob,
      horoshopSuccessJobs,
      lastPipelineJob
    ] = await Promise.all([
      db.query('SELECT COUNT(*) FROM suppliers'),
      db.query('SELECT COUNT(*) FROM sources'),
      db.query('SELECT COUNT(*) FROM products_raw'),
      db.query('SELECT COUNT(*) FROM products_final'),
      db.query('SELECT id, type, status, created_at FROM jobs ORDER BY id DESC LIMIT 1'),
      db.query(
        `SELECT id, type, status, created_at, started_at, finished_at, meta
         FROM jobs
         WHERE type = 'horoshop_sync'
         ORDER BY id DESC
         LIMIT 1`
      ),
      db.query(
        `SELECT id, started_at, finished_at
         FROM jobs
         WHERE type = 'horoshop_sync'
           AND status = 'success'
           AND started_at IS NOT NULL
           AND finished_at IS NOT NULL
         ORDER BY id DESC
         LIMIT 3`
      ),
      db.query(
        `SELECT id, type, status, created_at, started_at, finished_at, meta
         FROM jobs
         WHERE type = 'update_pipeline'
         ORDER BY id DESC
         LIMIT 1`
      )
    ]);

    let avgHoroshopDurationMs = null;
    if (horoshopSuccessJobs.rows.length) {
      const durations = horoshopSuccessJobs.rows
        .map((row) => new Date(row.finished_at).getTime() - new Date(row.started_at).getTime())
        .filter((value) => Number.isFinite(value) && value >= 0);
      if (durations.length) {
        avgHoroshopDurationMs = Math.round(
          durations.reduce((sum, value) => sum + value, 0) / durations.length
        );
      }
    }

    let lastHoroshopSync = null;
    const horoshopJob = lastHoroshopJob.rows[0] || null;
    if (horoshopJob) {
      let expectedTotal = null;
      const lastSuccessJob = horoshopSuccessJobs.rows[0] || null;
      if (lastSuccessJob) {
        const successLog = await db.query(
          `SELECT data
           FROM logs
           WHERE job_id = $1
             AND message = 'Horoshop sync completed'
           ORDER BY id DESC
           LIMIT 1`,
          [lastSuccessJob.id]
        );
        if (typeof successLog.rows[0]?.data?.total !== 'undefined') {
          expectedTotal = Number(successLog.rows[0].data.total);
        }
      }

      let progressTotal = null;
      if (horoshopJob.status === 'running') {
        const progressLog = await db.query(
          `SELECT data, created_at
           FROM logs
           WHERE job_id = $1
             AND message = 'Horoshop sync progress'
           ORDER BY id DESC
           LIMIT 1`,
          [horoshopJob.id]
        );
        if (typeof progressLog.rows[0]?.data?.total !== 'undefined') {
          progressTotal = Number(progressLog.rows[0].data.total);
        }
      }

      const startedAt = horoshopJob.started_at ? new Date(horoshopJob.started_at) : null;
      const finishedAt = horoshopJob.finished_at ? new Date(horoshopJob.finished_at) : null;
      const durationMs =
        startedAt && finishedAt ? Math.max(0, finishedAt.getTime() - startedAt.getTime()) : null;
      const elapsedMs =
        horoshopJob.status === 'running' && startedAt
          ? Math.max(0, Date.now() - startedAt.getTime())
          : null;
      const ratePerSecond =
        progressTotal !== null && elapsedMs && elapsedMs > 0
          ? progressTotal / (elapsedMs / 1000)
          : null;
      const etaMs =
        expectedTotal !== null &&
        progressTotal !== null &&
        ratePerSecond &&
        ratePerSecond > 0 &&
        expectedTotal > progressTotal
          ? Math.round(((expectedTotal - progressTotal) / ratePerSecond) * 1000)
          : null;
      const logResult = await db.query(
        `SELECT level, message, data, created_at
         FROM logs
         WHERE job_id = $1
           AND message IN ('Horoshop sync finished', 'Horoshop sync completed', 'Horoshop sync failed')
         ORDER BY id DESC
         LIMIT 1`,
        [horoshopJob.id]
      );
      const logRow = logResult.rows[0] || null;
      const data = logRow?.data || {};
      lastHoroshopSync = {
        id: horoshopJob.id,
        status: horoshopJob.status,
        created_at: horoshopJob.created_at,
        started_at: horoshopJob.started_at,
        finished_at: horoshopJob.finished_at,
        message: logRow?.message || null,
        level: logRow?.level || null,
        total: typeof data.total !== 'undefined' ? Number(data.total) : null,
        deleted: typeof data.deleted !== 'undefined' ? Number(data.deleted) : null,
        duration_ms: durationMs,
        estimate: {
          avg_duration_ms: avgHoroshopDurationMs,
          expected_total: expectedTotal,
          processed: progressTotal,
          rate_per_sec: ratePerSecond,
          eta_ms: etaMs
        },
        error: data.error || horoshopJob.meta?.error || null
      };
    }

    let lastUpdatePipeline = null;
    const pipelineJob = lastPipelineJob.rows[0] || null;
    if (pipelineJob) {
      const pipelineLog = await db.query(
        `SELECT level, message, data, created_at
         FROM logs
         WHERE job_id = $1
         ORDER BY id DESC
         LIMIT 1`,
        [pipelineJob.id]
      );
      const summaryLog = await db.query(
        `SELECT data
         FROM logs
         WHERE job_id = $1
           AND message = 'Update pipeline finished'
         ORDER BY id DESC
         LIMIT 1`,
        [pipelineJob.id]
      );
      const logRow = pipelineLog.rows[0] || null;
      const summaryData = summaryLog.rows[0]?.data || null;
      const startedAt = pipelineJob.started_at ? new Date(pipelineJob.started_at) : null;
      const finishedAt = pipelineJob.finished_at ? new Date(pipelineJob.finished_at) : null;
      const durationMs =
        startedAt && finishedAt ? Math.max(0, finishedAt.getTime() - startedAt.getTime()) : null;
      lastUpdatePipeline = {
        id: pipelineJob.id,
        status: pipelineJob.status,
        created_at: pipelineJob.created_at,
        started_at: pipelineJob.started_at,
        finished_at: pipelineJob.finished_at,
        message: logRow?.message || null,
        level: logRow?.level || null,
        duration_ms: durationMs,
        summary: summaryData,
        error: logRow?.data?.error || pipelineJob.meta?.error || null
      };
    }

    res.json({
      suppliers: Number(suppliers.rows[0].count || 0),
      sources: Number(sources.rows[0].count || 0),
      products_raw: Number(raw.rows[0].count || 0),
      products_final: Number(final.rows[0].count || 0),
      lastJob: lastJob.rows[0] || null,
      lastHoroshopSync,
      lastUpdatePipeline,
      horoshopSyncAvgDurationMs: avgHoroshopDurationMs
    });
  } catch (err) {
    next(err);
  }
});

router.get('/merged-preview', async (req, res, next) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    let jobId = req.query.jobId ? Number(req.query.jobId) : null;
    const search = (req.query.search || '').trim();
    const sort = req.query.sort || 'article_asc';

    if (!jobId) {
      const jobResult = await db.query(
        `SELECT id FROM jobs
         WHERE type = 'import_all'
         ORDER BY id DESC
         LIMIT 1`
      );
      jobId = jobResult.rows[0]?.id || null;
    }

    const whereParts = [];
    const values = [];
    if (jobId) {
      values.push(jobId);
      const jobIndex = values.length;
      whereParts.push(`pr.job_id = $${jobIndex}`);
    }

    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(pr.article ILIKE $${searchIndex} OR pr.extra ILIKE $${searchIndex})`
      );
    }

    let orderBy = 'pr.article ASC, pr.id DESC';
    if (sort === 'article_desc') {
      orderBy = 'pr.article DESC, pr.id DESC';
    } else if (sort === 'created_desc') {
      orderBy = 'pr.id DESC';
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const result = await db.query(
      `SELECT pr.article, pr.size, pr.quantity, pr.price, pr.extra,
              sp.name AS supplier_name, pr.created_at, pr.job_id,
              COUNT(*) OVER() AS total
       FROM products_raw pr
       JOIN suppliers sp ON sp.id = pr.supplier_id
       ${whereClause}
       ORDER BY ${orderBy}
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);

    return res.json({ jobId, total, rows });
  } catch (err) {
    next(err);
  }
});

router.get('/merged-export', async (req, res, next) => {
  try {
    const search = (req.query.search || '').trim();
    const sort = req.query.sort || 'article_asc';
    let jobId = req.query.jobId ? Number(req.query.jobId) : null;

    if (!jobId) {
      const jobResult = await db.query(
        `SELECT id FROM jobs
         WHERE type = 'import_all'
         ORDER BY id DESC
         LIMIT 1`
      );
      jobId = jobResult.rows[0]?.id || null;
    }

    const whereParts = [];
    const values = [];
    if (jobId) {
      values.push(jobId);
      const jobIndex = values.length;
      whereParts.push(`pr.job_id = $${jobIndex}`);
    }

    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(pr.article ILIKE $${searchIndex} OR pr.extra ILIKE $${searchIndex})`
      );
    }

    let orderBy = 'pr.article ASC, pr.id DESC';
    if (sort === 'article_desc') {
      orderBy = 'pr.article DESC, pr.id DESC';
    } else if (sort === 'created_desc') {
      orderBy = 'pr.id DESC';
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="merged_export_${Date.now()}.xlsx"`
    );

    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      stream: res,
      useStyles: false,
      useSharedStrings: true
    });
    const sheet = workbook.addWorksheet('Merged');
    sheet
      .addRow(['article', 'size', 'quantity', 'price', 'extra', 'supplier', 'created_at'])
      .commit();

    const pageSize = 5000;
    let offset = 0;
    const baseValues = [...values];
    while (true) {
      const result = await db.query(
        `SELECT pr.article, pr.size, pr.quantity, pr.price, pr.extra,
                sp.name AS supplier_name, pr.created_at
         FROM products_raw pr
         JOIN suppliers sp ON sp.id = pr.supplier_id
         ${whereClause}
         ORDER BY ${orderBy}
         LIMIT $${baseValues.length + 1} OFFSET $${baseValues.length + 2}`,
        [...baseValues, pageSize, offset]
      );

      if (!result.rows.length) {
        break;
      }

      for (const row of result.rows) {
        sheet
          .addRow([
            row.article,
            row.size || '',
            row.quantity ?? '',
            row.price === null ? '' : Number(row.price),
            row.extra || '',
            row.supplier_name || '',
            row.created_at ? row.created_at.toISOString() : ''
          ])
          .commit();
      }

      if (result.rows.length < pageSize) {
        break;
      }
      offset += pageSize;
    }

    await sheet.commit();
    await workbook.commit();
  } catch (err) {
    next(err);
  }
});

router.get('/final-preview', async (req, res, next) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    let jobId = req.query.jobId ? Number(req.query.jobId) : null;
    const search = (req.query.search || '').trim();
    const sort = req.query.sort || 'article_asc';
    const supplierId = req.query.supplierId ? Number(req.query.supplierId) : null;

    if (!jobId) {
      const jobResult = await db.query(
        `SELECT id FROM jobs
         WHERE type = 'finalize'
         ORDER BY id DESC
         LIMIT 1`
      );
      jobId = jobResult.rows[0]?.id || null;
    }

    const whereParts = [];
    const values = [];
    if (jobId) {
      values.push(jobId);
      const jobIndex = values.length;
      whereParts.push(`pf.job_id = $${jobIndex}`);
    }

    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(pf.article ILIKE $${searchIndex} OR pf.extra ILIKE $${searchIndex} OR sp.name ILIKE $${searchIndex})`
      );
    }

    if (supplierId) {
      values.push(supplierId);
      const supplierIndex = values.length;
      whereParts.push(`pf.supplier_id = $${supplierIndex}`);
    }

    let orderBy = 'pf.article ASC, pf.id DESC';
    if (sort === 'article_desc') {
      orderBy = 'pf.article DESC, pf.id DESC';
    } else if (sort === 'created_desc') {
      orderBy = 'pf.id DESC';
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const result = await db.query(
      `SELECT pf.article, pf.size, pf.quantity, pf.price_base,
              COALESCE(po.price_final, pf.price_final) AS price_final,
              pf.extra, sp.name AS supplier_name, pf.created_at, pf.job_id,
              po.id AS override_id, po.price_final AS override_price, po.notes AS override_notes,
              COUNT(*) OVER() AS total
       FROM products_final pf
       LEFT JOIN suppliers sp ON sp.id = pf.supplier_id
       LEFT JOIN price_overrides po
         ON po.article = pf.article
        AND NULLIF(po.size, '') IS NOT DISTINCT FROM NULLIF(pf.size, '')
        AND po.is_active = TRUE
       ${whereClause}
       ORDER BY ${orderBy}
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);

    return res.json({ jobId, total, rows });
  } catch (err) {
    next(err);
  }
});

router.get('/price-overrides', async (req, res, next) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const search = (req.query.search || '').trim();

    const whereParts = [];
    const values = [];
    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(`(article ILIKE $${searchIndex} OR notes ILIKE $${searchIndex})`);
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    const result = await db.query(
      `SELECT id, article, size, price_final, is_active, notes, created_at,
              COUNT(*) OVER() AS total
       FROM price_overrides
       ${whereClause}
       ORDER BY created_at DESC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);
    res.json({ total, rows });
  } catch (err) {
    next(err);
  }
});

router.post('/price-overrides', async (req, res, next) => {
  try {
    const { article, size, price_final, notes } = req.body;
    if (!article || !price_final) {
      return res.status(400).json({ error: 'article and price_final are required' });
    }
    const sizeValue = typeof size === 'undefined' || size === '' ? null : size;

    const existing = await db.query(
      `SELECT id FROM price_overrides
       WHERE article = $1
         AND NULLIF(size, '') IS NOT DISTINCT FROM NULLIF($2, '')
       ORDER BY id DESC LIMIT 1`,
      [article, sizeValue]
    );

    let record;
    if (existing.rows[0]) {
      const updateResult = await db.query(
        `UPDATE price_overrides
         SET price_final = $1, notes = $2, is_active = TRUE
         WHERE id = $3
         RETURNING *`,
        [price_final, notes || null, existing.rows[0].id]
      );
      record = updateResult.rows[0];
    } else {
      const insertResult = await db.query(
        `INSERT INTO price_overrides (article, size, price_final, notes)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [article, sizeValue, price_final, notes || null]
      );
      record = insertResult.rows[0];
    }

    await db.query(
      `UPDATE products_final
       SET price_final = $1
       WHERE article = $2
         AND NULLIF(size, '') IS NOT DISTINCT FROM NULLIF($3, '')`,
      [price_final, article, sizeValue]
    );

    res.json(record);
  } catch (err) {
    next(err);
  }
});

router.put('/price-overrides/:id', async (req, res, next) => {
  try {
    const { price_final, notes, is_active } = req.body;
    const overrideResult = await db.query(
      'SELECT * FROM price_overrides WHERE id = $1',
      [req.params.id]
    );
    const override = overrideResult.rows[0];
    if (!override) {
      return res.status(404).json({ error: 'override not found' });
    }

    const values = [];
    const updates = buildUpdateClause(
      {
        price_final,
        notes,
        is_active
      },
      values
    );
    if (!updates.length) {
      return res.status(400).json({ error: 'no fields to update' });
    }
    values.push(req.params.id);
    const updateResult = await db.query(
      `UPDATE price_overrides SET ${updates.join(', ')} WHERE id = $${values.length} RETURNING *`,
      values
    );
    const updated = updateResult.rows[0];

    if (typeof is_active !== 'undefined') {
      if (is_active) {
        await db.query(
          `UPDATE products_final
           SET price_final = $1
           WHERE article = $2
             AND NULLIF(size, '') IS NOT DISTINCT FROM NULLIF($3, '')`,
          [updated.price_final, updated.article, updated.size]
        );
      } else {
        await db.query(
          `UPDATE products_final pf
           SET price_final = CEIL((
             CASE
               WHEN s.min_profit_enabled = TRUE
                 AND (pf.price_base * (1 + s.markup_percent / 100)) - pf.price_base < s.min_profit_amount
                 THEN pf.price_base + s.min_profit_amount
               ELSE pf.price_base * (1 + s.markup_percent / 100)
             END
           ) / 10) * 10
           FROM suppliers s
           WHERE pf.supplier_id = s.id
             AND pf.article = $1
             AND NULLIF(pf.size, '') IS NOT DISTINCT FROM NULLIF($2, '')`,
          [updated.article, updated.size]
        );
      }
    } else if (typeof price_final !== 'undefined') {
      await db.query(
        `UPDATE products_final
         SET price_final = $1
         WHERE article = $2
           AND NULLIF(size, '') IS NOT DISTINCT FROM NULLIF($3, '')`,
        [updated.price_final, updated.article, updated.size]
      );
    }

    res.json(updated);
  } catch (err) {
    next(err);
  }
});

router.get('/horoshop-preview', async (req, res, next) => {
  try {
    await ensureHoroshopMirrorTable();
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const search = (req.query.search || '').trim();

    const whereParts = [];
    const values = [];

    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(article ILIKE $${searchIndex} OR supplier ILIKE $${searchIndex} OR parent_article ILIKE $${searchIndex})`
      );
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const result = await db.query(
      `SELECT article, supplier, presence_ua, display_in_showcase, parent_article, price, synced_at,
              COUNT(*) OVER() AS total
       FROM horoshop_mirror
       ${whereClause}
       ORDER BY article ASC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);

    res.json({ total, rows });
  } catch (err) {
    next(err);
  }
});

router.get('/horoshop-suppliers', async (req, res, next) => {
  try {
    await ensureHoroshopMirrorTable();
    const result = await db.query(
      `SELECT DISTINCT supplier
       FROM horoshop_mirror
       WHERE supplier IS NOT NULL AND supplier <> ''
       ORDER BY supplier ASC`
    );
    const suppliers = result.rows.map((row) => row.supplier);
    res.json({ suppliers });
  } catch (err) {
    next(err);
  }
});

router.get('/horoshop-export', async (req, res, next) => {
  try {
    await ensureHoroshopMirrorTable();
    const search = (req.query.search || '').trim();

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="horoshop_preview_${Date.now()}.xlsx"`
    );

    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      stream: res,
      useStyles: false,
      useSharedStrings: true
    });
    const sheet = workbook.addWorksheet('Horoshop');
    sheet
      .addRow([
        'article',
        'supplier',
        'presence_ua',
        'display_in_showcase',
        'parent_article',
        'price',
        'synced_at'
      ])
      .commit();

    const pageSize = 5000;
    let lastArticle = '';
    while (true) {
      const whereParts = ['article > $1'];
      const values = [lastArticle];

      if (search) {
        values.push(`%${search}%`);
        const searchIndex = values.length;
        whereParts.push(
          `(article ILIKE $${searchIndex} OR supplier ILIKE $${searchIndex} OR parent_article ILIKE $${searchIndex})`
        );
      }

      const result = await db.query(
        `SELECT article, supplier, presence_ua, display_in_showcase, parent_article, price, synced_at
         FROM horoshop_mirror
         WHERE ${whereParts.join(' AND ')}
         ORDER BY article ASC
         LIMIT $${values.length + 1}`,
        [...values, pageSize]
      );

      if (!result.rows.length) {
        break;
      }

      for (const row of result.rows) {
        sheet
          .addRow([
            row.article,
            row.supplier || '',
            row.presence_ua || '',
            row.display_in_showcase ? 'true' : 'false',
            row.parent_article || '',
            row.price === null ? '' : Number(row.price),
            row.synced_at ? row.synced_at.toISOString() : ''
          ])
          .commit();
        lastArticle = row.article;
      }
    }

    await sheet.commit();
    await workbook.commit();
  } catch (err) {
    next(err);
  }
});

router.get('/horoshop-api-preview', async (req, res, next) => {
  try {
    await ensureHoroshopApiPreviewTable();
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const search = (req.query.search || '').trim();
    const supplier = (req.query.supplier || '').trim();

    const whereParts = [];
    const values = [];

    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(article ILIKE $${searchIndex} OR supplier ILIKE $${searchIndex} OR parent_article ILIKE $${searchIndex})`
      );
    }

    if (supplier) {
      values.push(supplier);
      const supplierIndex = values.length;
      whereParts.push(`LOWER(supplier) = LOWER($${supplierIndex})`);
    }

    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const result = await db.query(
      `SELECT
         article,
         supplier,
         presence_ua,
         display_in_showcase,
         parent_article,
         price,
         COUNT(*) OVER() AS total
      FROM horoshop_api_preview
       ${whereClause}
       ORDER BY article ASC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);

    res.json({ total, rows });
  } catch (err) {
    next(err);
  }
});

router.get('/final-export', async (req, res, next) => {
  try {
    const search = (req.query.search || '').trim();
    const supplierId = req.query.supplierId ? Number(req.query.supplierId) : null;

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="finalize_export_${Date.now()}.xlsx"`
    );

    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      stream: res,
      useStyles: false,
      useSharedStrings: true
    });
    const sheet = workbook.addWorksheet('Finalize');
    sheet
      .addRow([
        'article',
        'size',
        'quantity',
        'price_base',
        'price_final',
        'extra',
        'supplier'
      ])
      .commit();

    const pageSize = 5000;
    let lastId = 0;
    while (true) {
      const whereParts = ['pf.id > $1'];
      const values = [lastId];

      if (search) {
        values.push(`%${search}%`);
        const searchIndex = values.length;
        whereParts.push(
          `(pf.article ILIKE $${searchIndex} OR pf.extra ILIKE $${searchIndex} OR sp.name ILIKE $${searchIndex})`
        );
      }

      if (supplierId) {
        values.push(supplierId);
        const supplierIndex = values.length;
        whereParts.push(`pf.supplier_id = $${supplierIndex}`);
      }

      const result = await db.query(
        `SELECT pf.id, pf.article, pf.size, pf.quantity, pf.price_base, pf.price_final,
                pf.extra, sp.name AS supplier_name
         FROM products_final pf
         LEFT JOIN suppliers sp ON sp.id = pf.supplier_id
         WHERE ${whereParts.join(' AND ')}
         ORDER BY pf.id ASC
         LIMIT $${values.length + 1}`,
        [...values, pageSize]
      );

      if (!result.rows.length) {
        break;
      }

      for (const row of result.rows) {
        sheet
          .addRow([
            row.article,
            row.size || '',
            row.quantity ?? '',
            row.price_base === null ? '' : Number(row.price_base),
            row.price_final === null ? '' : Number(row.price_final),
            row.extra || '',
            row.supplier_name || ''
          ])
          .commit();
        lastId = row.id;
      }
    }

    await sheet.commit();
    await workbook.commit();
  } catch (err) {
    next(err);
  }
});

router.get('/compare-preview', async (req, res, next) => {
  try {
    const limit = Number(req.query.limit || 100);
    const offset = Number(req.query.offset || 0);
    const search = (req.query.search || '').trim();
    const supplierId = req.query.supplierId ? Number(req.query.supplierId) : null;
    const missingOnly = req.query.missingOnly === 'true' || req.query.missingOnly === '1';

    const baseWhereParts = [];
    const baseValues = [];
    if (supplierId) {
      baseValues.push(supplierId);
      baseWhereParts.push(`pf.supplier_id = $${baseValues.length}`);
    }
    const baseWhereClause = baseWhereParts.length ? `WHERE ${baseWhereParts.join(' AND ')}` : '';

    const whereParts = [];
    const values = [...baseValues];
    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(base.article ILIKE $${searchIndex}
          OR base.extra ILIKE $${searchIndex}
          OR base.supplier_name ILIKE $${searchIndex}
          OR base.sku_article ILIKE $${searchIndex})`
      );
    }
    if (missingOnly) {
      whereParts.push('hm_sku.article IS NULL');
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    const result = await db.query(
      `WITH base AS (
         SELECT
           pf.id,
           pf.article,
           pf.size,
           pf.quantity,
           pf.price_base,
           pf.price_final,
           pf.extra,
           sp.name AS supplier_name,
           CASE
             WHEN pf.size IS NULL OR btrim(pf.size) = '' THEN pf.article
             WHEN right(lower(pf.article), length(replace(btrim(pf.size), ',', '.'))) =
                  lower(replace(btrim(pf.size), ',', '.'))
               THEN pf.article
             ELSE pf.article || '-' || replace(btrim(pf.size), ',', '.')
           END AS sku_article
         FROM products_final pf
         LEFT JOIN suppliers sp ON sp.id = pf.supplier_id
         ${baseWhereClause}
       )
       SELECT
         base.article,
         base.size,
         base.quantity,
         base.price_base,
         base.price_final,
         base.extra,
         base.supplier_name,
         base.sku_article,
         hm_base.article AS horoshop_article,
         hm_sku.article AS horoshop_sku,
         hm_sku.presence_ua AS horoshop_presence,
         hm_sku.display_in_showcase AS horoshop_display,
         COUNT(*) OVER() AS total
       FROM base
       LEFT JOIN horoshop_mirror hm_base ON hm_base.article = base.article
       LEFT JOIN horoshop_mirror hm_sku ON hm_sku.article = base.sku_article
       ${whereClause}
       ORDER BY base.id ASC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      [...values, limit, offset]
    );

    const total = result.rows[0]?.total ? Number(result.rows[0].total) : 0;
    const rows = result.rows.map(({ total: _total, ...row }) => row);

    res.json({ total, rows });
  } catch (err) {
    next(err);
  }
});

router.get('/compare-export', async (req, res, next) => {
  try {
    const search = (req.query.search || '').trim();
    const supplierId = req.query.supplierId ? Number(req.query.supplierId) : null;
    const missingOnly = req.query.missingOnly === 'true' || req.query.missingOnly === '1';

    const baseWhereParts = [];
    const baseValues = [];
    if (supplierId) {
      baseValues.push(supplierId);
      baseWhereParts.push(`pf.supplier_id = $${baseValues.length}`);
    }
    const baseWhereClause = baseWhereParts.length ? `WHERE ${baseWhereParts.join(' AND ')}` : '';

    const whereParts = [];
    const values = [...baseValues];
    if (search) {
      values.push(`%${search}%`);
      const searchIndex = values.length;
      whereParts.push(
        `(base.article ILIKE $${searchIndex}
          OR base.extra ILIKE $${searchIndex}
          OR base.supplier_name ILIKE $${searchIndex}
          OR base.sku_article ILIKE $${searchIndex})`
      );
    }
    if (missingOnly) {
      whereParts.push('hm_sku.article IS NULL');
    }
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="compare_export_${Date.now()}.xlsx"`
    );

    const ExcelJS = require('exceljs');
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      stream: res,
      useStyles: false,
      useSharedStrings: true
    });
    const sheet = workbook.addWorksheet('Compare');
    sheet
      .addRow([
        'article',
        'size',
        'quantity',
        'price_base',
        'price_final',
        'extra',
        'supplier',
        'sku_article',
        'horoshop_article',
        'horoshop_sku',
        'horoshop_presence',
        'horoshop_display'
      ])
      .commit();

    const pageSize = 5000;
    let offset = 0;
    const baseValuesCopy = [...values];
    while (true) {
      const result = await db.query(
        `WITH base AS (
           SELECT
             pf.id,
             pf.article,
             pf.size,
             pf.quantity,
             pf.price_base,
             pf.price_final,
             pf.extra,
             sp.name AS supplier_name,
             CASE
               WHEN pf.size IS NULL OR btrim(pf.size) = '' THEN pf.article
               WHEN right(lower(pf.article), length(replace(btrim(pf.size), ',', '.'))) =
                    lower(replace(btrim(pf.size), ',', '.'))
                 THEN pf.article
               ELSE pf.article || '-' || replace(btrim(pf.size), ',', '.')
             END AS sku_article
           FROM products_final pf
           LEFT JOIN suppliers sp ON sp.id = pf.supplier_id
           ${baseWhereClause}
         )
         SELECT
           base.article,
           base.size,
           base.quantity,
           base.price_base,
           base.price_final,
           base.extra,
           base.supplier_name,
           base.sku_article,
           hm_base.article AS horoshop_article,
           hm_sku.article AS horoshop_sku,
           hm_sku.presence_ua AS horoshop_presence,
           hm_sku.display_in_showcase AS horoshop_display
         FROM base
         LEFT JOIN horoshop_mirror hm_base ON hm_base.article = base.article
         LEFT JOIN horoshop_mirror hm_sku ON hm_sku.article = base.sku_article
         ${whereClause}
         ORDER BY base.id ASC
         LIMIT $${baseValuesCopy.length + 1} OFFSET $${baseValuesCopy.length + 2}`,
        [...baseValuesCopy, pageSize, offset]
      );

      if (!result.rows.length) {
        break;
      }

      for (const row of result.rows) {
        sheet
          .addRow([
            row.article,
            row.size || '',
            row.quantity ?? '',
            row.price_base === null ? '' : Number(row.price_base),
            row.price_final === null ? '' : Number(row.price_final),
            row.extra || '',
            row.supplier_name || '',
            row.sku_article || '',
            row.horoshop_article || '',
            row.horoshop_sku || '',
            row.horoshop_presence || '',
            typeof row.horoshop_display === 'boolean'
              ? row.horoshop_display
                ? 'true'
                : 'false'
              : ''
          ])
          .commit();
      }

      if (result.rows.length < pageSize) {
        break;
      }
      offset += pageSize;
    }

    await sheet.commit();
    await workbook.commit();
  } catch (err) {
    next(err);
  }
});

router.get('/cron-settings', async (req, res, next) => {
  try {
    await ensureCronTable();
    const result = await db.query(
      'SELECT name, cron, is_enabled, meta, updated_at FROM cron_settings ORDER BY name ASC'
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

router.get('/source-preview', async (req, res, next) => {
  try {
    const sourceId = req.query.sourceId;
    const headerRowParam = req.query.headerRow;
    const headerRow =
      typeof headerRowParam === 'undefined' ? 1 : Number(headerRowParam);
    const sheetName = req.query.sheetName || null;
    if (!sourceId) {
      return res.status(400).json({ error: 'sourceId is required' });
    }

    const sourceResult = await db.query(
      `SELECT s.*, sp.id AS supplier_id
       FROM sources s
       JOIN suppliers sp ON sp.id = s.supplier_id
       WHERE s.id = $1 AND s.is_active = TRUE AND sp.is_active = TRUE`,
      [sourceId]
    );
    const source = sourceResult.rows[0];
    if (!source) {
      return res.status(404).json({ error: 'source not found' });
    }
    if (source.source_type !== 'google_sheet') {
      return res.status(400).json({ error: 'unsupported source type' });
    }

    const preview = await getSheetPreview(
      source.source_url,
      sheetName || source.sheet_name,
      headerRow,
      5
    );
    const hasHeader = preview.headerRow > 0;
    let headers = [];
    let sampleRows = [];
    if (hasHeader) {
      headers = preview.rows[0] || [];
      sampleRows = preview.rows.slice(1);
    } else {
      const maxColumns = preview.rows.reduce(
        (max, row) => Math.max(max, row.length),
        0
      );
      headers = Array.from({ length: maxColumns }, () => '');
      sampleRows = preview.rows;
    }
    return res.json({
      sourceId: source.id,
      sheetName: preview.sheetName,
      headerRow: preview.headerRow,
      headers,
      sampleRows
    });
  } catch (err) {
    next(err);
  }
});

router.get('/source-sheets', async (req, res, next) => {
  try {
    const sourceId = req.query.sourceId;
    if (!sourceId) {
      return res.status(400).json({ error: 'sourceId is required' });
    }

    const sourceResult = await db.query('SELECT * FROM sources WHERE id = $1', [sourceId]);
    const source = sourceResult.rows[0];
    if (!source) {
      return res.status(404).json({ error: 'source not found' });
    }
    if (source.source_type !== 'google_sheet') {
      return res.status(400).json({ error: 'unsupported source type' });
    }

    const sheets = await listSheetNames(source.source_url);
    let selectedSheetName = source.sheet_name || null;
    if (selectedSheetName && !sheets.includes(selectedSheetName)) {
      selectedSheetName = null;
    }
    if (!selectedSheetName) {
      selectedSheetName = sheets[0] || null;
    }

    res.json({
      sourceId: source.id,
      sheets,
      selectedSheetName
    });
  } catch (err) {
    next(err);
  }
});

router.put('/cron-settings', async (req, res, next) => {
  try {
    const settings = req.body?.settings;
    if (!Array.isArray(settings)) {
      return res.status(400).json({ error: 'settings array is required' });
    }

    await ensureCronTable();
    for (const setting of settings) {
      if (!setting.name || !setting.cron) {
        continue;
      }
      await db.query(
        `INSERT INTO cron_settings (name, cron, is_enabled, meta, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (name)
         DO UPDATE SET cron = EXCLUDED.cron, is_enabled = EXCLUDED.is_enabled,
           meta = EXCLUDED.meta, updated_at = NOW()`,
        [setting.name, setting.cron, Boolean(setting.is_enabled), setting.meta || {}]
      );
    }

    await scheduleTasks();
    const result = await db.query(
      'SELECT name, cron, is_enabled, meta, updated_at FROM cron_settings ORDER BY name ASC'
    );
    res.json(result.rows);
  } catch (err) {
    next(err);
  }
});

module.exports = router;
