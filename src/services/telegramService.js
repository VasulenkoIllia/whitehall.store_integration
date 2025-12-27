const { telegramBotToken, telegramChatId, telegramAppName } = require('../config');

const TELEGRAM_MAX_LENGTH = 3800;

function isTelegramEnabled() {
  return Boolean(telegramBotToken && telegramChatId);
}

function buildText(payload) {
  const lines = [];
  const prefix = telegramAppName ? `${telegramAppName}: ` : '';
  lines.push(`${prefix}${payload.message || 'Error'}`);
  if (payload.jobId) {
    lines.push(`Job: #${payload.jobId}`);
  }
  if (payload.level) {
    lines.push(`Level: ${payload.level}`);
  }
  if (payload.data) {
    const dataText =
      typeof payload.data === 'string' ? payload.data : JSON.stringify(payload.data);
    if (dataText && dataText !== '{}') {
      lines.push(`Data: ${dataText}`);
    }
  }
  const text = lines.join('\n');
  if (text.length <= TELEGRAM_MAX_LENGTH) {
    return text;
  }
  return `${text.slice(0, TELEGRAM_MAX_LENGTH)}…`;
}

async function sendTelegramAlert(payload) {
  if (!isTelegramEnabled()) {
    return;
  }
  const text = buildText(payload);
  try {
    const response = await fetch(`https://api.telegram.org/bot${telegramBotToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: telegramChatId,
        text,
        disable_web_page_preview: true
      })
    });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`Telegram API error: ${response.status} ${body}`);
    }
  } catch (err) {
    // Avoid throwing to not break the main flow.
    console.warn('Failed to send Telegram alert', err.message);
  }
}

module.exports = {
  sendTelegramAlert
};
