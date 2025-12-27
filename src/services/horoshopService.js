const db = require('../db');
const logService = require('./logService');
const {
  horoshopDomain,
  horoshopLogin,
  horoshopPassword,
  horoshopExportLimit,
  horoshopSyncDelayMs,
  horoshopSyncMaxRetries,
  horoshopStoreRaw
} = require('../config');

function buildApiBase() {
  if (!horoshopDomain) {
    throw new Error('Horoshop domain is not set');
  }
  if (horoshopDomain.startsWith('http://') || horoshopDomain.startsWith('https://')) {
    return horoshopDomain.replace(/\/+$/, '');
  }
  return `https://${horoshopDomain.replace(/\/+$/, '')}`;
}

async function postJson(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  let data = null;
  try {
    data = await res.json();
  } catch {
    data = null;
  }
  if (!res.ok) {
    const message = data?.error?.message || data?.message || res.statusText;
    throw new Error(message);
  }
  return data;
}

async function getToken() {
  if (!horoshopLogin || !horoshopPassword) {
    throw new Error('Horoshop credentials are not set');
  }
  const base = buildApiBase();
  const response = await postJson(`${base}/api/auth/`, {
    login: horoshopLogin,
    password: horoshopPassword
  });
  const token = response?.response?.token || response?.token;
  if (!token) {
    throw new Error('Horoshop token not received');
  }
  return token;
}

async function exportCatalog(token, offset, limit) {
  const base = buildApiBase();
  const response = await postJson(`${base}/api/catalog/export/`, {
    token,
    offset,
    limit
  });
  const payload = response?.response ?? null;
  if (Array.isArray(payload)) {
    return payload;
  }

  const products = payload?.products || response?.products;
  const errorMessage =
    response?.error?.message ||
    payload?.error?.message ||
    payload?.message ||
    response?.message ||
    response?.error ||
    (typeof payload === 'string' ? payload : null);
  if (!Array.isArray(products) && errorMessage) {
    throw new Error(`Horoshop export error: ${errorMessage}`);
  }
  if (!Array.isArray(products)) {
    const keys = response && typeof response === 'object' ? Object.keys(response) : [];
    const payloadKeys = payload && typeof payload === 'object' ? Object.keys(payload) : [];
    throw new Error(
      `Horoshop export returned invalid response (keys: ${keys.join(
        ', '
      ) || 'none'}, response keys: ${payloadKeys.join(', ') || 'none'})`
    );
  }
  return products;
}

async function importCatalog(token, products) {
  const base = buildApiBase();
  const response = await postJson(`${base}/api/catalog/import/`, {
    token,
    products
  });
  const errorMessage =
    response?.error?.message ||
    response?.response?.error?.message ||
    response?.message ||
    response?.error;
  if (errorMessage) {
    throw new Error(`Horoshop import error: ${errorMessage}`);
  }
  return response;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterSeconds(message) {
  const match = String(message || '').match(/Retry after\s+(\d+)\s*seconds/i);
  if (match) {
    return Number(match[1]);
  }
  return null;
}

function normalizeProduct(product) {
  return {
    article: product?.article || '',
    supplier: product?.supplier?.value || product?.supplier || '',
    presenceUa: product?.presence?.value?.ua || product?.presence?.ua || '',
    displayInShowcase:
      typeof product?.display_in_showcase === 'boolean'
        ? product.display_in_showcase
        : Boolean(product?.display_in_showcase),
    parentArticle: product?.parent_article || product?.parentArticle || '',
    price: Number(product?.price || 0),
    raw: horoshopStoreRaw ? product : null
  };
}

async function insertMirrorBatch(client, items, runAt) {
  const rows = items
    .map(normalizeProduct)
    .filter((item) => item.article);
  if (!rows.length) {
    return 0;
  }

  const values = [];
  const placeholders = rows.map((row, idx) => {
    const base = idx * 8;
    values.push(
      row.article,
      row.supplier,
      row.presenceUa,
      row.displayInShowcase,
      row.parentArticle,
      row.price || null,
      row.raw ? JSON.stringify(row.raw) : null,
      runAt
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8})`;
  });

  await client.query(
    `INSERT INTO horoshop_mirror
     (article, supplier, presence_ua, display_in_showcase, parent_article, price, raw, seen_at)
     VALUES ${placeholders.join(', ')}
     ON CONFLICT (article) DO UPDATE SET
       supplier = EXCLUDED.supplier,
       presence_ua = EXCLUDED.presence_ua,
       display_in_showcase = EXCLUDED.display_in_showcase,
       parent_article = EXCLUDED.parent_article,
       price = EXCLUDED.price,
       raw = EXCLUDED.raw,
       synced_at = NOW(),
       seen_at = EXCLUDED.seen_at`,
    values
  );

  return rows.length;
}

function buildImportProduct(row) {
  const product = { article: row.article };
  if (row.presence_ua) {
    product.presence = { ua: row.presence_ua };
  }
  if (typeof row.display_in_showcase === 'boolean') {
    product.display_in_showcase = row.display_in_showcase;
  }
  if (row.parent_article) {
    product.parent_article = row.parent_article;
  }
  if (row.price !== null && typeof row.price !== 'undefined' && row.price !== '') {
    product.price = Number(row.price);
  }
  return product;
}

async function importHoroshopPreview(jobId) {
  let token = await getToken();
  const chunkSize = 500;
  let lastId = 0;
  let total = 0;
  let batches = 0;
  let retryAttempts = 0;
  const maxRetries = Number.isFinite(horoshopSyncMaxRetries) ? horoshopSyncMaxRetries : 5;
  const maxAttempts = maxRetries <= 0 ? Infinity : maxRetries;

  while (true) {
    const result = await db.query(
      `SELECT id, article, presence_ua, display_in_showcase, parent_article, price
       FROM horoshop_api_preview
       WHERE id > $1
       ORDER BY id ASC
       LIMIT $2`,
      [lastId, chunkSize]
    );

    if (!result.rows.length) {
      break;
    }

    const products = [];
    for (const row of result.rows) {
      lastId = row.id;
      if (!row.article) {
        continue;
      }
      products.push(buildImportProduct(row));
    }

    if (!products.length) {
      continue;
    }

    while (true) {
      try {
        await importCatalog(token, products);
        retryAttempts = 0;
        break;
      } catch (err) {
        const message = err?.message || '';
        const retryAfterSeconds = parseRetryAfterSeconds(message);
        let waitMs = null;
        if (Number.isFinite(retryAfterSeconds)) {
          waitMs = retryAfterSeconds * 1000;
        } else if (/requests limit has been exceeded/i.test(message)) {
          waitMs = Math.min(15 * 60 * 1000, (retryAttempts + 1) * 60 * 1000);
        }

        if (waitMs !== null && retryAttempts < maxAttempts) {
          retryAttempts += 1;
          if (jobId) {
            await logService.log(jobId, 'warning', 'Horoshop import rate limited', {
              attempt: retryAttempts,
              waitMs,
              retryAfterSeconds,
              batch: batches + 1
            });
          }
          await sleep(waitMs);
          token = await getToken();
          if (jobId) {
            await logService.log(jobId, 'info', 'Horoshop token refreshed after wait', {
              batch: batches + 1
            });
          }
          continue;
        }

        if (/incorrect auth data/i.test(message) && retryAttempts < maxAttempts) {
          retryAttempts += 1;
          token = await getToken();
          if (jobId) {
            await logService.log(jobId, 'warning', 'Horoshop import auth refreshed', {
              attempt: retryAttempts,
              batch: batches + 1
            });
          }
          continue;
        }

        throw err;
      }
    }

    batches += 1;
    total += products.length;
    if (jobId) {
      await logService.log(jobId, 'info', 'Horoshop import batch sent', {
        batch: batches,
        sent: products.length,
        total
      });
    }
  }

  if (jobId) {
    await logService.log(jobId, 'info', 'Horoshop import completed', {
      total,
      batches
    });
  }
  return { total, batches };
}

async function syncHoroshopCatalog(jobId) {
  let token = await getToken();
  const client = await db.pool.connect();
  try {
    const runAt = new Date().toISOString();
    const maxRetries = Number.isFinite(horoshopSyncMaxRetries) ? horoshopSyncMaxRetries : 5;
    const maxAttempts = maxRetries <= 0 ? Infinity : maxRetries;
    const delayMs = Number.isFinite(horoshopSyncDelayMs) ? horoshopSyncDelayMs : 0;

    let offset = 0;
    let total = 0;
    const limit = horoshopExportLimit || 500;
    if (!Number.isFinite(limit) || limit <= 0) {
      throw new Error('Horoshop export limit must be greater than 0');
    }
    let batchIndex = 0;
    let retryAttempts = 0;

    while (true) {
      let products;
      try {
        products = await exportCatalog(token, offset * limit, limit);
        retryAttempts = 0;
      } catch (err) {
        const message = err?.message || '';
        const retryAfterSeconds = parseRetryAfterSeconds(message);
        let waitMs = null;
        if (Number.isFinite(retryAfterSeconds)) {
          waitMs = retryAfterSeconds * 1000;
        } else if (/requests limit has been exceeded/i.test(message)) {
          waitMs = Math.min(15 * 60 * 1000, (retryAttempts + 1) * 60 * 1000);
        }

        if (waitMs !== null && retryAttempts < maxAttempts) {
          retryAttempts += 1;
          if (jobId) {
            await logService.log(jobId, 'warning', 'Horoshop sync rate limited', {
              attempt: retryAttempts,
              waitMs,
              retryAfterSeconds,
              offset: offset * limit,
              limit
            });
          }
          await sleep(waitMs);
          token = await getToken();
          if (jobId) {
            await logService.log(jobId, 'info', 'Horoshop token refreshed after wait', {
              offset: offset * limit
            });
          }
          continue;
        }
        throw err;
      }
      if (!products.length) {
        if (batchIndex === 0 && jobId) {
          await logService.log(jobId, 'warning', 'Horoshop export returned empty batch', {
            offset: offset * limit,
            limit
          });
        }
        break;
      }
      const inserted = await insertMirrorBatch(client, products, runAt);
      total += inserted;
      batchIndex += 1;
      if (jobId) {
        await logService.log(jobId, 'info', 'Horoshop sync progress', {
          batch: batchIndex,
          fetched: products.length,
          inserted,
          total
        });
      }
      if (products.length < limit) {
        break;
      }
      if (delayMs > 0) {
        await sleep(delayMs);
      }
      offset += 1;
    }

    const cleanupResult = await client.query(
      'DELETE FROM horoshop_mirror WHERE seen_at IS DISTINCT FROM $1',
      [runAt]
    );
    if (jobId) {
      await logService.log(jobId, 'info', 'Horoshop sync completed', {
        total,
        deleted: cleanupResult.rowCount || 0
      });
    }
    return { total, deleted: cleanupResult.rowCount || 0 };
  } catch (err) {
    if (jobId) {
      await logService.log(jobId, 'error', 'Horoshop sync failed', { error: err.message });
    }
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  syncHoroshopCatalog,
  importHoroshopPreview
};
