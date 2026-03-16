const db = require('../db');
const jobService = require('../services/jobService');
const logService = require('../services/logService');
const { importGoogleSheetSource } = require('../services/importService');
const { buildFinalDataset } = require('../services/finalizeService');
const { exportForHoroshop } = require('../services/exportService');
const { syncHoroshopCatalog, importHoroshopPreview } = require('../services/horoshopService');
const { cleanupOldData } = require('../services/cleanupService');
const { postImportAnalyzeEnabled } = require('../config');

const BLOCKING_JOB_TYPES = [
  'update_pipeline',
  'import_all',
  'import_source',
  'import_supplier',
  'finalize',
  'export',
  'horoshop_sync',
  'horoshop_import'
];

async function ensureNoRunningJobs() {
  const running = await jobService.findRunningJobs(BLOCKING_JOB_TYPES);
  if (running.length) {
    const err = new Error(`Another job is running: #${running[0].id} (${running[0].type})`);
    err.status = 409;
    throw err;
  }
}

async function acquireLock(jobId) {
  const acquired = await jobService.acquireJobLock(jobId);
  if (!acquired) {
    const err = new Error('Another job is running');
    err.status = 409;
    throw err;
  }
}

async function assertNoVisibleRealParentRows(jobId) {
  const result = await db.query(
    `WITH real_parents AS (
       SELECT DISTINCT
         parent_article,
         LOWER(COALESCE(supplier, '')) AS supplier_key
       FROM horoshop_mirror
       WHERE parent_article IS NOT NULL
         AND parent_article <> ''
         AND article <> parent_article
     ),
     violations AS (
       SELECT
         p.id,
         p.article,
         p.supplier,
         p.parent_article,
         p.presence_ua,
         p.display_in_showcase,
         p.price
       FROM horoshop_api_preview p
       JOIN real_parents rp
         ON rp.parent_article = p.article
        AND rp.supplier_key = LOWER(COALESCE(p.supplier, ''))
       WHERE COALESCE(p.display_in_showcase, FALSE) = TRUE
          OR COALESCE(p.presence_ua, '') = 'В наявності'
     )
     SELECT
       id,
       article,
       supplier,
       parent_article,
       presence_ua,
       display_in_showcase,
       price,
       COUNT(*) OVER() AS total_count
     FROM violations
     ORDER BY id DESC
     LIMIT 20`
  );

  const total = Number(result.rows[0]?.total_count || 0);
  if (!total) {
    return;
  }

  const sample = result.rows.map((row) => ({
    id: row.id,
    article: row.article,
    supplier: row.supplier,
    parentArticle: row.parent_article,
    presenceUa: row.presence_ua,
    displayInShowcase: row.display_in_showcase,
    price: row.price
  }));

  if (jobId) {
    await logService.log(jobId, 'error', 'Horoshop import blocked: real parent SKU is visible', {
      total,
      sample
    });
  }

  const err = new Error(`Horoshop import blocked: ${total} real parent SKU rows are visible in API preview`);
  err.code = 'PARENT_SKU_VISIBILITY_VIOLATION';
  err.status = 422;
  err.details = { total, sample };
  throw err;
}

function buildMappingMeta(record) {
  if (!record) {
    return null;
  }
  const meta = record.mapping_meta ? { ...record.mapping_meta } : {};
  if (typeof record.source_id !== 'undefined') {
    if (record.source_id === null) {
      if (typeof meta.source_id !== 'undefined') {
        delete meta.source_id;
      }
    } else if (typeof meta.source_id === 'undefined' || meta.source_id === null) {
      meta.source_id = record.source_id;
    }
  }
  if (typeof record.header_row !== 'undefined' && record.header_row !== null) {
    if (typeof meta.header_row === 'undefined' || meta.header_row === null) {
      meta.header_row = record.header_row;
    }
  }
  return Object.keys(meta).length ? meta : null;
}

async function importAllSources(jobId) {
  await logService.log(jobId, 'info', 'Import all sources started');
  const sourcesResult = await db.query(
    `SELECT s.*, sp.id AS supplier_id, sp.name AS supplier_name, s.name AS source_name
     FROM sources s
     JOIN suppliers sp ON sp.id = s.supplier_id
     WHERE s.is_active = TRUE AND sp.is_active = TRUE
     ORDER BY s.id ASC`
  );

  const summary = [];
  for (const source of sourcesResult.rows) {
    const statusResult = await db.query('SELECT status FROM jobs WHERE id = $1', [jobId]);
    if (statusResult.rows[0]?.status === 'canceled') {
      const err = new Error('Job canceled');
      err.code = 'JOB_CANCELED';
      throw err;
    }
    const mappingResult = await db.query(
      `SELECT mapping, mapping_meta, header_row, source_id
       FROM column_mappings
       WHERE supplier_id = $1 AND source_id = $2
       ORDER BY id DESC LIMIT 1`,
      [source.supplier_id, source.id]
    );
    const mappingRecord = mappingResult.rows[0];
    const mappingMeta = buildMappingMeta(mappingRecord);

    if (source.source_type !== 'google_sheet') {
      await logService.log(jobId, 'error', 'Unsupported source type', {
        sourceId: source.id,
        sourceType: source.source_type,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null
      });
      summary.push({
        sourceId: source.id,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null,
        imported: 0,
        error: 'unsupported source type'
      });
      continue;
    }

    try {
      const result = await importGoogleSheetSource({
        source,
        supplierId: source.supplier_id,
        jobId,
        mappingOverride: mappingRecord?.mapping || null,
        mappingMeta
      });
      summary.push({
        sourceId: source.id,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null,
        imported: result.imported,
        error: result.error || null
      });
    } catch (err) {
      if (err.code === 'JOB_CANCELED' || err.message === 'Job canceled') {
        throw err;
      }
      await logService.log(jobId, 'error', 'Import source failed', {
        sourceId: source.id,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null,
        error: err.message
      });
      summary.push({
        sourceId: source.id,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null,
        imported: 0,
        error: err.message
      });
    }
  }

  return summary;
}

async function runStepJob({ type, meta, startMessage, finishMessage, handler, startData }) {
  const job = await jobService.createJob(type, meta || {});
  await jobService.startJob(job.id);
  if (startMessage) {
    await logService.log(job.id, 'info', startMessage, startData);
  }
  try {
    const result = await handler(job.id);
    await jobService.finishJob(job.id);
    if (finishMessage) {
      await logService.log(job.id, 'info', finishMessage, result);
    }
    return { job, result };
  } catch (err) {
    await jobService.failJob(job.id, err);
    await logService.log(job.id, 'error', `${type} failed`, { error: err.message });
    throw err;
  }
}

async function runPostImportAnalyze(jobId) {
  if (!postImportAnalyzeEnabled) {
    return { enabled: false, skipped: true };
  }

  const startedAt = Date.now();
  try {
    await db.query('ANALYZE products_raw');
    await db.query('ANALYZE suppliers');
    await db.query('ANALYZE markup_rule_conditions');
    const result = {
      enabled: true,
      skipped: false,
      durationMs: Date.now() - startedAt,
      tables: ['products_raw', 'suppliers', 'markup_rule_conditions']
    };
    if (jobId) {
      await logService.log(jobId, 'info', 'Post-import analyze finished', result);
    }
    return result;
  } catch (err) {
    const result = {
      enabled: true,
      skipped: true,
      durationMs: Date.now() - startedAt,
      error: err.message
    };
    if (jobId) {
      await logService.log(jobId, 'warning', 'Post-import analyze skipped', result);
    }
    return result;
  }
}

async function runImportSource(sourceId) {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const sourceResult = await db.query(
      `SELECT s.*, sp.id AS supplier_id, sp.name AS supplier_name, s.name AS source_name
       FROM sources s
       JOIN suppliers sp ON sp.id = s.supplier_id
       WHERE s.id = $1 AND s.is_active = TRUE AND sp.is_active = TRUE`,
      [sourceId]
    );
    const source = sourceResult.rows[0];
    if (!source) {
      throw new Error('source not found');
    }

    const mappingResult = await db.query(
      `SELECT mapping, mapping_meta, header_row, source_id
       FROM column_mappings
       WHERE supplier_id = $1 AND source_id = $2
       ORDER BY id DESC LIMIT 1`,
      [source.supplier_id, source.id]
    );
    const mappingRecord = mappingResult.rows[0];
    const mappingMeta = buildMappingMeta(mappingRecord);

    const job = await jobService.createJob('import_source', { sourceId });
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Import source started', {
      sourceId,
      sourceName: source.source_name || source.name || null,
      supplierName: source.supplier_name || null
    });

    if (source.source_type !== 'google_sheet') {
      await logService.log(jobId, 'error', 'Unsupported source type', {
        sourceId: source.id,
        sourceType: source.source_type,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null
      });
      await jobService.finishJob(jobId);
      return { jobId, result: { imported: 0, mapping: null } };
    }

    const result = await importGoogleSheetSource({
      source,
      supplierId: source.supplier_id,
      jobId,
      mappingOverride: mappingRecord?.mapping || null,
      mappingMeta
    });

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Import source finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Import source failed', {
        sourceId,
        sourceName: source?.source_name || source?.name || null,
        supplierName: source?.supplier_name || null,
        error: err.message
      });
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runImportAll() {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('import_all', {});
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    const summary = await importAllSources(jobId);
    const analyze = await runPostImportAnalyze(jobId);
    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Import all sources finished', { summary, analyze });
    return { jobId, summary, analyze };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runCleanup() {
  let jobId = null;
  try {
    const job = await jobService.createJob('cleanup', {});
    jobId = job.id;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Cleanup started');

    const result = await cleanupOldData(jobId);

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Cleanup finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Cleanup failed', { error: err.message });
    }
    throw err;
  }
}

async function runImportSupplier(supplierId) {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('import_supplier', { supplierId });
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Import supplier started', { supplierId });

    const sourcesResult = await db.query(
      `SELECT s.*, sp.id AS supplier_id, sp.name AS supplier_name, s.name AS source_name
       FROM sources s
       JOIN suppliers sp ON sp.id = s.supplier_id
       WHERE s.is_active = TRUE AND sp.is_active = TRUE AND sp.id = $1
       ORDER BY s.id ASC`,
      [supplierId]
    );

    const summary = [];
    for (const source of sourcesResult.rows) {
      const mappingResult = await db.query(
        `SELECT mapping, mapping_meta, header_row, source_id
         FROM column_mappings
         WHERE supplier_id = $1 AND source_id = $2
         ORDER BY id DESC LIMIT 1`,
        [source.supplier_id, source.id]
      );
      const mappingRecord = mappingResult.rows[0];
      const mappingMeta = buildMappingMeta(mappingRecord);

      if (source.source_type !== 'google_sheet') {
        await logService.log(jobId, 'error', 'Unsupported source type', {
          sourceId: source.id,
          sourceType: source.source_type,
          sourceName: source.source_name || source.name || null,
          supplierName: source.supplier_name || null
        });
        summary.push({
          sourceId: source.id,
          sourceName: source.source_name || source.name || null,
          supplierName: source.supplier_name || null,
          imported: 0,
          error: 'unsupported source type'
        });
        continue;
      }

      try {
      const result = await importGoogleSheetSource({
        source,
        supplierId: source.supplier_id,
        jobId,
        mappingOverride: mappingRecord?.mapping || null,
        mappingMeta
      });
      summary.push({
        sourceId: source.id,
        sourceName: source.source_name || source.name || null,
        supplierName: source.supplier_name || null,
        imported: result.imported,
        error: result.error || null
      });
      } catch (err) {
        await logService.log(jobId, 'error', 'Import source failed', {
          sourceId: source.id,
          sourceName: source.source_name || source.name || null,
          supplierName: source.supplier_name || null,
          error: err.message
        });
        summary.push({
          sourceId: source.id,
          sourceName: source.source_name || source.name || null,
          supplierName: source.supplier_name || null,
          imported: 0,
          error: err.message
        });
      }
    }

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Import supplier finished', { summary });
    return { jobId, summary };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runFinalize() {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('finalize', {});
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Finalize started');

    const result = await buildFinalDataset(jobId);

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Finalize finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runExport(options = {}) {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const supplier = options?.supplier || null;
    const job = await jobService.createJob('export', supplier ? { supplier } : {});
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Export started', supplier ? { supplier } : undefined);

    const result = await exportForHoroshop(jobId, { supplier });

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Export finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runHoroshopSync() {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('horoshop_sync', {});
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Horoshop sync started');

    const result = await syncHoroshopCatalog(jobId);

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Horoshop sync finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Horoshop sync failed', { error: err.message });
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runHoroshopImport() {
  let jobId = null;
  let lockAcquired = false;
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('horoshop_import', {});
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Horoshop import started');

    await assertNoVisibleRealParentRows(jobId);
    const result = await importHoroshopPreview(jobId);

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Horoshop import finished', result);
    return { jobId, result };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Horoshop import failed', { error: err.message });
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

async function runUpdatePipeline(options = {}) {
  let jobId = null;
  let lockAcquired = false;
  const supplier = options?.supplier || 'drop';
  try {
    await ensureNoRunningJobs();
    const job = await jobService.createJob('update_pipeline', { supplier });
    jobId = job.id;
    await acquireLock(jobId);
    lockAcquired = true;
    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Update pipeline started', { supplier });

    const importStep = await runStepJob({
      type: 'import_all',
      meta: { pipeline_job_id: jobId },
      startMessage: null,
      finishMessage: 'Import all sources finished',
      handler: (stepJobId) => importAllSources(stepJobId)
    });
    const importAnalyze = await runPostImportAnalyze(importStep.job.id);
    await logService.log(jobId, 'info', 'Update pipeline import finished', {
      jobId: importStep.job.id,
      summary: importStep.result,
      analyze: importAnalyze
    });

    const finalizeStep = await runStepJob({
      type: 'finalize',
      meta: { pipeline_job_id: jobId },
      startMessage: 'Finalize started',
      finishMessage: 'Finalize finished',
      handler: (stepJobId) => buildFinalDataset(stepJobId)
    });
    await logService.log(jobId, 'info', 'Update pipeline finalize finished', {
      jobId: finalizeStep.job.id,
      ...finalizeStep.result
    });

    const exportStep = await runStepJob({
      type: 'export',
      meta: { pipeline_job_id: jobId, supplier },
      startMessage: 'Export started',
      finishMessage: 'Export finished',
      startData: supplier ? { supplier } : undefined,
      handler: (stepJobId) => exportForHoroshop(stepJobId, { supplier })
    });
    await logService.log(jobId, 'info', 'Update pipeline export finished', {
      jobId: exportStep.job.id,
      ...exportStep.result
    });

    const horoshopImportStep = await runStepJob({
      type: 'horoshop_import',
      meta: { pipeline_job_id: jobId },
      startMessage: 'Horoshop import started',
      finishMessage: 'Horoshop import finished',
      handler: async (stepJobId) => {
        await assertNoVisibleRealParentRows(stepJobId);
        return importHoroshopPreview(stepJobId);
      }
    });
    await logService.log(jobId, 'info', 'Update pipeline finished', {
      importJobId: importStep.job.id,
      finalizeJobId: finalizeStep.job.id,
      exportJobId: exportStep.job.id,
      horoshopImportJobId: horoshopImportStep.job.id,
      importSummary: importStep.result,
      importAnalyze,
      finalize: finalizeStep.result,
      export: exportStep.result,
      horoshopImport: horoshopImportStep.result
    });

    await jobService.finishJob(jobId);
    return {
      jobId,
      result: {
        importSummary: importStep.result,
        importAnalyze,
        finalize: finalizeStep.result,
        export: exportStep.result,
        horoshopImport: horoshopImportStep.result
      }
    };
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Update pipeline failed', { error: err.message });
    }
    throw err;
  } finally {
    if (lockAcquired) {
      await jobService.releaseJobLock(jobId);
    }
  }
}

module.exports = {
  runImportSource,
  runImportAll,
  runImportSupplier,
  runFinalize,
  runExport,
  runHoroshopSync,
  runHoroshopImport,
  runUpdatePipeline,
  runCleanup
};
