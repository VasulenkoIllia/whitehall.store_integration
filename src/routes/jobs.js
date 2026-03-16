const express = require('express');
const db = require('../db');
const jobService = require('../services/jobService');
const logService = require('../services/logService');
const { importExcelFile } = require('../services/importService');
const {
  runImportSource,
  runImportAll,
  runImportSupplier,
  runFinalize,
  runExport,
  runHoroshopSync,
  runHoroshopImport
} = require('../jobs/runners');

const router = express.Router();

router.post('/import', async (req, res, next) => {
  let jobId = null;
  try {
    const { filePath, supplierId, sourceId, mappingOverride } = req.body;
    if (!filePath || !supplierId) {
      return res.status(400).json({ error: 'filePath and supplierId are required' });
    }

    const job = await jobService.createJob('import', {
      filePath,
      supplierId,
      sourceId: sourceId || null
    });
    jobId = job.id;

    await jobService.startJob(jobId);
    await logService.log(jobId, 'info', 'Import started', { filePath, supplierId, sourceId });

    const result = await importExcelFile({
      filePath,
      supplierId,
      sourceId: sourceId || null,
      jobId,
      mappingOverride: mappingOverride || null
    });

    await jobService.finishJob(jobId);
    await logService.log(jobId, 'info', 'Import finished', result);

    return res.json({ jobId, result });
  } catch (err) {
    if (jobId) {
      await jobService.failJob(jobId, err);
      await logService.log(jobId, 'error', 'Import failed', { error: err.message });
    }
    return next(err);
  }
});

router.post('/finalize', async (req, res, next) => {
  try {
    const result = await runFinalize();
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/export', async (req, res, next) => {
  try {
    const { supplier } = req.body || {};
    const result = await runExport({ supplier });
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/import-source', async (req, res, next) => {
  try {
    const { sourceId } = req.body;
    if (!sourceId) {
      return res.status(400).json({ error: 'sourceId is required' });
    }
    const result = await runImportSource(sourceId);
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/import-all', async (req, res, next) => {
  try {
    const result = await runImportAll();
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/import-supplier', async (req, res, next) => {
  try {
    const { supplierId } = req.body;
    if (!supplierId) {
      return res.status(400).json({ error: 'supplierId is required' });
    }
    const result = await runImportSupplier(supplierId);
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/horoshop-sync', async (req, res, next) => {
  try {
    const result = await runHoroshopSync();
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/horoshop-import', async (req, res, next) => {
  try {
    const result = await runHoroshopImport();
    return res.json(result);
  } catch (err) {
    return next(err);
  }
});

router.post('/:jobId/cancel', async (req, res, next) => {
  try {
    const jobId = Number(req.params.jobId);
    if (!Number.isFinite(jobId)) {
      return res.status(400).json({ error: 'jobId is required' });
    }
    const jobResult = await db.query('SELECT id, status, type FROM jobs WHERE id = $1', [jobId]);
    const job = jobResult.rows[0];
    if (!job) {
      return res.status(404).json({ error: 'job not found' });
    }
    if (!['running', 'queued'].includes(job.status)) {
      return res.status(409).json({ error: 'job is not running' });
    }
    const reason = (req.body && req.body.reason) || 'Canceled by user';

    const childCanceled = [];
    if (job.type === 'update_pipeline') {
      const childJobs = await db.query(
        `SELECT id, type, status
         FROM jobs
         WHERE COALESCE(meta->>'pipeline_job_id', '') = $1
           AND status IN ('running', 'queued')
         ORDER BY id DESC`,
        [String(jobId)]
      );
      for (const child of childJobs.rows) {
        const childReason = `Canceled with parent pipeline #${jobId}`;
        await jobService.cancelJob(child.id, childReason);
        let childTerminated = [];
        if (child.status === 'running') {
          childTerminated = await jobService.terminateJobBackend(child.id, child.type);
        }
        const childPids = childTerminated
          .map((row) => Number(row.pid))
          .filter(Number.isFinite);
        await logService.log(child.id, 'error', 'Job canceled', {
          reason: childReason,
          jobType: child.type,
          parentPipelineJobId: jobId,
          terminatedPids: childPids
        });
        childCanceled.push({
          jobId: Number(child.id),
          jobType: child.type,
          terminatedPids: childPids
        });
      }
    }

    const canceled = await jobService.cancelJob(jobId, reason);
    let terminated = [];
    if (job.status === 'running') {
      terminated = await jobService.terminateJobBackend(jobId, job.type);
    }
    await logService.log(jobId, 'error', 'Job canceled', {
      reason,
      jobType: job.type,
      terminatedPids: terminated.map((row) => Number(row.pid)).filter(Number.isFinite),
      childCanceled
    });
    return res.json({ jobId: canceled?.id || jobId, status: 'canceled' });
  } catch (err) {
    return next(err);
  }
});

module.exports = router;
