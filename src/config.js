require('dotenv').config();

const port = Number(process.env.PORT || 3000);
const databaseUrl = process.env.DATABASE_URL || '';
const logLevel = process.env.LOG_LEVEL || 'info';
const exportDir = process.env.EXPORT_DIR || 'exports';
const visibilityYes = process.env.VISIBILITY_YES || 'Так';
const horoshopDomain = process.env.HOROSHOP_DOMAIN || '';
const horoshopLogin = process.env.HOROSHOP_LOGIN || '';
const horoshopPassword = process.env.HOROSHOP_PASSWORD || '';
const horoshopExportLimit = Number(process.env.HOROSHOP_EXPORT_LIMIT || 500);
const horoshopSyncDelayMs = Number(process.env.HOROSHOP_SYNC_DELAY_MS || 250);
const horoshopSyncMaxRetries = Number(process.env.HOROSHOP_SYNC_MAX_RETRIES || 5);
const horoshopStoreRaw = process.env.HOROSHOP_STORE_RAW === 'true';
const jobRetentionDays = Number(process.env.JOB_RETENTION_DAYS || 10);
const jobTimeoutMinutes = Number(process.env.JOB_TIMEOUT_MINUTES || 0);
const telegramBotToken = process.env.TELEGRAM_BOT_TOKEN || '';
const telegramChatId = process.env.TELEGRAM_CHAT_ID || '';
const telegramAppName = process.env.TELEGRAM_APP_NAME || '';
const adminLogin = process.env.ADMIN_LOGIN || 'admin';
const adminPassword = process.env.ADMIN_PASSWORD || 'admin';

module.exports = {
  port,
  databaseUrl,
  logLevel,
  exportDir,
  visibilityYes,
  horoshopDomain,
  horoshopLogin,
  horoshopPassword,
  horoshopExportLimit,
  horoshopSyncDelayMs,
  horoshopSyncMaxRetries,
  horoshopStoreRaw,
  jobRetentionDays,
  jobTimeoutMinutes,
  telegramBotToken,
  telegramChatId,
  telegramAppName,
  adminLogin,
  adminPassword
};
