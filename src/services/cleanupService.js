const db = require('../db');
const logService = require('./logService');
const { jobRetentionDays } = require('../config');

async function cleanupOldData(jobId) {
  const days = Number.isFinite(jobRetentionDays) ? jobRetentionDays : 10;
  const interval = `${days} days`;

  const logsResult = await db.query(
    'DELETE FROM logs WHERE created_at < NOW() - $1::interval',
    [interval]
  );
  const rawResult = await db.query(
    'DELETE FROM products_raw WHERE created_at < NOW() - $1::interval',
    [interval]
  );
  const jobsResult = await db.query(
    'DELETE FROM jobs WHERE created_at < NOW() - $1::interval',
    [interval]
  );

  const summary = {
    retentionDays: days,
    deletedLogs: logsResult.rowCount || 0,
    deletedRaw: rawResult.rowCount || 0,
    deletedJobs: jobsResult.rowCount || 0
  };

  if (jobId) {
    await logService.log(jobId, 'info', 'Cleanup summary', summary);
  }

  return summary;
}

module.exports = {
  cleanupOldData
};
