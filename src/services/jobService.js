const db = require('../db');

async function createJob(type, meta) {
  const result = await db.query(
    'INSERT INTO jobs (type, status, meta) VALUES ($1, $2, $3) RETURNING *',
    [type, 'queued', meta || null]
  );
  return result.rows[0];
}

async function startJob(jobId) {
  await db.query(
    'UPDATE jobs SET status = $1, started_at = NOW() WHERE id = $2',
    ['running', jobId]
  );
}

async function finishJob(jobId) {
  await db.query(
    `UPDATE jobs
     SET status = $1, finished_at = NOW()
     WHERE id = $2 AND status = 'running'`,
    ['success', jobId]
  );
}

async function failJob(jobId, error) {
  await db.query(
    `UPDATE jobs
     SET status = $1,
         finished_at = NOW(),
         meta = COALESCE(meta, '{}') || $2::jsonb
     WHERE id = $3 AND status <> 'canceled'`,
    ['failed', JSON.stringify({ error: error?.message || String(error) }), jobId]
  );
}

async function findRunningJobs(types = []) {
  if (!types.length) {
    return [];
  }
  const result = await db.query(
    'SELECT id, type FROM jobs WHERE status = $1 AND type = ANY($2::text[]) ORDER BY id DESC',
    ['running', types]
  );
  return result.rows;
}

async function acquireJobLock(jobId, name = 'global') {
  const result = await db.query(
    `INSERT INTO job_locks (name, job_id)
     VALUES ($1, $2)
     ON CONFLICT (name) DO NOTHING
     RETURNING name`,
    [name, jobId]
  );
  return result.rowCount > 0;
}

async function releaseJobLock(jobId, name = 'global') {
  await db.query('DELETE FROM job_locks WHERE name = $1 AND job_id = $2', [name, jobId]);
}

async function cancelJob(jobId, reason) {
  const message = reason || 'Canceled by user';
  const result = await db.query(
    `UPDATE jobs
     SET status = $1,
         finished_at = NOW(),
         meta = COALESCE(meta, '{}') || $2::jsonb
     WHERE id = $3
     RETURNING id, type, status`,
    ['canceled', JSON.stringify({ error: message }), jobId]
  );
  await db.query('DELETE FROM job_locks WHERE job_id = $1', [jobId]);
  return result.rows[0] || null;
}

async function timeoutStaleJobs(timeoutMinutes) {
  if (!Number.isFinite(timeoutMinutes) || timeoutMinutes <= 0) {
    return [];
  }
  const interval = `${timeoutMinutes} minutes`;
  const result = await db.query(
    `UPDATE jobs
     SET status = $1,
         finished_at = NOW(),
         meta = COALESCE(meta, '{}') || $2::jsonb
     WHERE status = 'running'
       AND started_at IS NOT NULL
       AND started_at < NOW() - $3::interval
     RETURNING id, type, started_at`,
    ['failed', JSON.stringify({ error: `Timeout after ${timeoutMinutes} minutes` }), interval]
  );
  if (result.rows.length) {
    const ids = result.rows.map((row) => row.id);
    await db.query('DELETE FROM job_locks WHERE job_id = ANY($1::bigint[])', [ids]);
  }
  return result.rows;
}

module.exports = {
  createJob,
  startJob,
  finishJob,
  failJob,
  findRunningJobs,
  acquireJobLock,
  releaseJobLock,
  cancelJob,
  timeoutStaleJobs
};
