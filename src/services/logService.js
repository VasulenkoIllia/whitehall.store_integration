const db = require('../db');
const { sendTelegramAlert } = require('./telegramService');

async function log(jobId, level, message, data) {
  await db.query(
    'INSERT INTO logs (job_id, level, message, data) VALUES ($1, $2, $3, $4)',
    [jobId || null, level, message, data || null]
  );
  if (level === 'error') {
    await sendTelegramAlert({ jobId, level, message, data });
  }
}

module.exports = {
  log
};
