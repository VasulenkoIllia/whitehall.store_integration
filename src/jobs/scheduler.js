const cron = require('node-cron');
const db = require('../db');
const logger = require('../logger');
const { runUpdatePipeline, runCleanup } = require('./runners');

const tasks = new Map();
const running = new Set();

const defaultSettings = [
  {
    name: 'update_pipeline',
    cron: process.env.UPDATE_PIPELINE_CRON || '0 3 * * *',
    is_enabled: true,
    meta: { supplier: process.env.UPDATE_PIPELINE_SUPPLIER || 'drop' }
  },
  { name: 'cleanup', cron: process.env.CLEANUP_CRON || '15 2 * * *', is_enabled: true }
];

const allowedTasks = new Set(['update_pipeline', 'cleanup']);

async function loadSettings() {
  try {
    const result = await db.query('SELECT name, cron, is_enabled, meta FROM cron_settings');
    if (result.rows.length) {
      return result.rows.filter((row) => allowedTasks.has(row.name));
    }
  } catch (err) {
    logger.warn({ err }, 'Failed to load cron settings from DB');
  }
  return defaultSettings;
}

async function runTask(setting) {
  const name = setting?.name;
  if (running.has(name)) {
    logger.warn({ name }, 'Cron task already running');
    return;
  }
  running.add(name);
  try {
    if (name === 'update_pipeline') {
      const supplier = setting?.meta?.supplier || 'drop';
      await runUpdatePipeline({ supplier });
    } else if (name === 'cleanup') {
      await runCleanup();
    }
  } catch (err) {
    logger.error({ err, name }, 'Cron task failed');
  } finally {
    running.delete(name);
  }
}

function clearTasks() {
  tasks.forEach((task) => task.stop());
  tasks.clear();
}

async function scheduleTasks() {
  if (process.env.ENABLE_CRON !== 'true') {
    clearTasks();
    logger.info('Cron scheduler disabled');
    return;
  }
  clearTasks();
  const settings = await loadSettings();
  settings.forEach((setting) => {
    if (!setting.is_enabled) {
      return;
    }
    if (!cron.validate(setting.cron)) {
      logger.warn({ setting }, 'Invalid cron expression');
      return;
    }
    const task = cron.schedule(setting.cron, () => runTask(setting));
    tasks.set(setting.name, task);
    logger.info({ name: setting.name, cron: setting.cron }, 'Cron scheduled');
  });
}

function startScheduler() {
  if (process.env.ENABLE_CRON !== 'true') {
    logger.info('Cron scheduler disabled');
    return;
  }
  scheduleTasks();
}

module.exports = {
  startScheduler,
  scheduleTasks
};
