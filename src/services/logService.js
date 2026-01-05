const db = require('../db');
const { sendTelegramAlert } = require('./telegramService');

function normalizeJsonPayload(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (value instanceof Error) {
    return { message: value.message, stack: value.stack || null };
  }
  if (typeof value === 'string') {
    return { message: value };
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return { value };
  }
  try {
    JSON.stringify(value);
    return value;
  } catch (err) {
    return { message: String(value) };
  }
}

async function log(jobId, level, message, data) {
  const payload = normalizeJsonPayload(data);
  await db.query(
    'INSERT INTO logs (job_id, level, message, data) VALUES ($1, $2, $3, $4)',
    [jobId || null, level, message, payload]
  );
  if (level === 'error') {
    await sendTelegramAlert({ jobId, level, message, data });
  }
}

module.exports = {
  log
};
