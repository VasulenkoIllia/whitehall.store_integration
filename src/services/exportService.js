const path = require('path');
const ExcelJS = require('exceljs');
const db = require('../db');
const logService = require('./logService');
const { exportDir, visibilityYes } = require('../config');

async function ensureHoroshopMirrorTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS horoshop_mirror (
      article TEXT PRIMARY KEY,
      supplier TEXT,
      presence_ua TEXT,
      display_in_showcase BOOLEAN,
      parent_article TEXT,
      price NUMERIC(12, 2),
      raw JSONB,
      synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      seen_at TIMESTAMPTZ
    )`
  );
}

async function ensureHoroshopApiPreviewTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS horoshop_api_preview (
      id BIGSERIAL PRIMARY KEY,
      job_id BIGINT,
      article TEXT NOT NULL,
      supplier TEXT,
      presence_ua TEXT,
      display_in_showcase BOOLEAN,
      parent_article TEXT,
      price NUMERIC(12, 2),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
}

function deriveParentArticle(article, size) {
  const baseArticle = String(article || '').trim();
  const sizeValue = String(size || '').trim();
  if (!baseArticle || !sizeValue) {
    return baseArticle;
  }

  const escapedSize = sizeValue.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp(`([\\s\\-_/]+)?${escapedSize}$`, 'i');
  if (!pattern.test(baseArticle)) {
    return baseArticle;
  }
  const stripped = baseArticle.replace(pattern, '').trim();
  return stripped || baseArticle;
}

function normalizeKey(value) {
  return String(value || '').trim();
}

function normalizeSupplier(value) {
  return String(value || '').trim();
}

function normalizeSize(value) {
  return normalizeKey(value).replace(',', '.');
}

function buildSku(article, size) {
  const baseArticle = normalizeKey(article);
  if (!baseArticle) {
    return '';
  }
  const sizeValue = normalizeSize(size);
  if (!sizeValue) {
    return baseArticle;
  }
  const escapedSize = sizeValue.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp(`([\\s\\-_/]+)?${escapedSize}$`, 'i');
  if (pattern.test(baseArticle)) {
    return baseArticle;
  }
  return `${baseArticle}-${sizeValue}`;
}

async function fetchHoroshopMirrorMap({ supplier } = {}) {
  const values = [];
  const whereParts = [];

  if (supplier) {
    values.push(supplier);
    const supplierIndex = values.length;
    whereParts.push(`LOWER(supplier) = LOWER($${supplierIndex})`);
  }

  const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
  const result = await db.query(
    `SELECT article, supplier, parent_article, display_in_showcase, presence_ua, price
     FROM horoshop_mirror
     ${whereClause}`,
    values
  );
  const map = new Map();
  result.rows.forEach((row) => {
    const key = normalizeKey(row.article);
    if (key) {
      map.set(key, row);
    }
  });
  return map;
}

function normalizePrice(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function isSameShowState(mirrorRow, desired) {
  if (!mirrorRow) {
    return false;
  }
  const mirrorPresence = mirrorRow.presence_ua || '';
  if (mirrorPresence !== desired.presenceUa) {
    return false;
  }
  if (Boolean(mirrorRow.display_in_showcase) !== Boolean(desired.displayInShowcase)) {
    return false;
  }
  const mirrorPrice = normalizePrice(mirrorRow.price);
  const desiredPrice = normalizePrice(desired.price);
  if (mirrorPrice === null || desiredPrice === null) {
    if (mirrorPrice !== desiredPrice) {
      return false;
    }
  } else if (Math.abs(mirrorPrice - desiredPrice) > 0.01) {
    return false;
  }
  const mirrorParent = mirrorRow.parent_article || '';
  const desiredParent = desired.parentArticle || '';
  if (mirrorParent !== desiredParent) {
    return false;
  }
  return true;
}

async function insertApiPreviewBatch(rows) {
  if (!rows.length) {
    return;
  }

  const values = [];
  const placeholders = rows.map((row, idx) => {
    const base = idx * 7;
    values.push(
      row.jobId,
      row.article,
      row.supplier || null,
      row.presenceUa || null,
      row.displayInShowcase,
      row.parentArticle || null,
      row.price === '' || row.price === null ? null : row.price
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`;
  });

  await db.query(
    `INSERT INTO horoshop_api_preview
     (job_id, article, supplier, presence_ua, display_in_showcase, parent_article, price)
     VALUES ${placeholders.join(', ')}`,
    values
  );
}

async function fetchHiddenParentArticles(supplier) {
  const values = [];
  let supplierFilter = '';
  let supplierJoinFilter = '';

  if (supplier) {
    values.push(supplier);
    const supplierIndex = values.length;
    supplierFilter = `AND LOWER(supplier) = LOWER($${supplierIndex})`;
    supplierJoinFilter = `WHERE LOWER(hm.supplier) = LOWER($${supplierIndex})`;
  }

  const result = await db.query(
    `WITH grouped AS (
       SELECT parent_article
       FROM horoshop_mirror
       WHERE parent_article IS NOT NULL AND parent_article <> ''
       ${supplierFilter}
       GROUP BY parent_article
       HAVING BOOL_AND(display_in_showcase = FALSE)
     )
     SELECT DISTINCT ON (hm.parent_article)
       hm.article,
       hm.parent_article,
       hm.supplier
     FROM horoshop_mirror hm
     JOIN grouped g ON g.parent_article = hm.parent_article
     ${supplierJoinFilter}
     ORDER BY hm.parent_article, hm.article`,
    values
  );
  return result.rows;
}

async function fetchSupplierHiddenCandidates(supplier) {
  const values = [];
  let supplierFilter = '';

  if (supplier) {
    values.push(supplier);
    const supplierIndex = values.length;
    supplierFilter = `AND LOWER(hm.supplier) = LOWER($${supplierIndex})`;
  }

  const result = await db.query(
    `SELECT hm.article, hm.supplier, hm.display_in_showcase, hm.presence_ua
     FROM horoshop_mirror hm
     WHERE 1=1
       ${supplierFilter}
       AND NOT (
         COALESCE(hm.display_in_showcase, TRUE) = FALSE
         AND COALESCE(hm.presence_ua, '') = 'Немає в наявності'
       )`,
    values
  );
  return result.rows;
}

async function exportForHoroshop(jobId, options = {}) {
  const generateFiles = options.generateFiles === true;
  const supplierFilter = normalizeSupplier(options.supplier) || 'drop';
  const supplierLower = supplierFilter.toLowerCase();
  const finalizeJobResult = await db.query(
    `SELECT id FROM jobs
     WHERE type = 'finalize'
     ORDER BY id DESC
     LIMIT 1`
  );
  const finalizeJobId = finalizeJobResult.rows[0]?.id || null;
  if (!finalizeJobId) {
    throw new Error('No finalize job found');
  }
  let filePath = null;
  let controlFilePath = null;
  let apiFilePath = null;
  let workbook = null;
  let controlWorkbook = null;
  let apiWorkbook = null;
  let sheet = null;
  let controlSheet = null;
  let apiSheet = null;

  if (generateFiles) {
    const outputDir = path.isAbsolute(exportDir)
      ? exportDir
      : path.join(__dirname, '..', '..', exportDir);
    const timestamp = Date.now();
    const fileName = `horoshop_export_${timestamp}.xlsx`;
    const controlFileName = `horoshop_control_${timestamp}.xlsx`;
    const apiFileName = `horoshop_api_${timestamp}.xlsx`;
    filePath = path.join(outputDir, fileName);
    controlFilePath = path.join(outputDir, controlFileName);
    apiFilePath = path.join(outputDir, apiFileName);

    workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: filePath,
      useStyles: false,
      useSharedStrings: true
    });
    controlWorkbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: controlFilePath,
      useStyles: false,
      useSharedStrings: true
    });
    apiWorkbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: apiFilePath,
      useStyles: false,
      useSharedStrings: true
    });
    sheet = workbook.addWorksheet('Horoshop');
    sheet.addRow(['article', 'price', 'visibility']).commit();
    controlSheet = controlWorkbook.addWorksheet('Контроль');
    controlSheet
      .addRow(['article', 'size', 'quantity', 'price_base', 'price_final', 'extra', 'supplier'])
      .commit();
    apiSheet = apiWorkbook.addWorksheet('API');
    apiSheet
      .addRow([
        'article',
        'supplier',
        'presence_ua',
        'display_in_showcase',
        'parent_article',
        'price'
      ])
      .commit();
  }

  await ensureHoroshopMirrorTable();
  await ensureHoroshopApiPreviewTable();
  await db.query('TRUNCATE horoshop_api_preview');

  const pageSize = 5000;
  let total = 0;
  let apiTotal = 0;
  let apiHideTotal = 0;
  let apiShowTotal = 0;
  let apiSkipped = 0;
  const presentSkus = new Set();
  const apiArticles = new Set();
  const apiInsertBatch = [];
  const apiBatchSize = 500;
  const mirrorMap = await fetchHoroshopMirrorMap({ supplier: supplierFilter });

  let lastSku = '';
  while (true) {
    const result = await db.query(
      `WITH base AS (
         SELECT
           pf.id,
           pf.article,
           pf.size,
           pf.quantity,
           pf.price_base,
           COALESCE(po.price_final, pf.price_final) AS price_final,
           pf.extra,
           sp.name AS supplier_name,
           COALESCE(sp.priority, 9999) AS supplier_priority,
           CASE
             WHEN pf.size IS NULL OR btrim(pf.size) = '' THEN pf.article
             WHEN right(lower(pf.article), length(replace(btrim(pf.size), ',', '.'))) =
                  lower(replace(btrim(pf.size), ',', '.'))
               THEN pf.article
             ELSE pf.article || '-' || replace(btrim(pf.size), ',', '.')
           END AS sku_article
         FROM products_final pf
         LEFT JOIN suppliers sp ON sp.id = pf.supplier_id
         LEFT JOIN price_overrides po
           ON po.article = pf.article
          AND NULLIF(po.size, '') IS NOT DISTINCT FROM NULLIF(pf.size, '')
          AND po.is_active = TRUE
         WHERE pf.job_id = $1
       ),
       ranked AS (
         SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY sku_article
                  ORDER BY supplier_priority ASC, price_final ASC, id ASC
                ) AS rn
         FROM base
       )
       SELECT
         id,
         article,
         size,
         quantity,
         price_base,
         price_final,
         extra,
         supplier_name,
         sku_article
       FROM ranked
       WHERE rn = 1
         AND sku_article > $2
       ORDER BY sku_article ASC
       LIMIT $3`,
      [finalizeJobId, lastSku, pageSize]
    );

    if (!result.rows.length) {
      break;
    }

    for (const row of result.rows) {
      lastSku = row.sku_article;
      if (!row.article || row.price_final === null) {
        continue;
      }
      const priceFinal = Number(row.price_final);
      const sku = normalizeKey(row.sku_article);
      const mirrorRow = mirrorMap.get(sku) || mirrorMap.get(normalizeKey(row.article));
      const horoshopArticle = normalizeKey(mirrorRow?.article || sku);
      presentSkus.add(sku);
      if (mirrorRow?.article) {
        presentSkus.add(normalizeKey(mirrorRow.article));
      }
      if (sheet) {
        sheet.addRow([horoshopArticle, priceFinal, visibilityYes]).commit();
      }
      if (controlSheet) {
        controlSheet
          .addRow([
            row.article,
            row.size || '',
            row.quantity ?? '',
            row.price_base === null ? '' : Number(row.price_base),
            priceFinal,
            row.extra || '',
            row.supplier_name || ''
          ])
          .commit();
      }
      if (String(mirrorRow?.supplier || '').toLowerCase() === supplierLower) {
        const parentArticle = mirrorRow?.parent_article || '';
        if (!apiArticles.has(horoshopArticle)) {
          const apiRow = {
            jobId,
            article: horoshopArticle,
            supplier: mirrorRow?.supplier || '',
            presenceUa: 'В наявності',
            displayInShowcase: true,
            parentArticle,
            price: priceFinal
          };
          if (isSameShowState(mirrorRow, apiRow)) {
            apiSkipped += 1;
            apiArticles.add(horoshopArticle);
            continue;
          }
          if (apiSheet) {
            apiSheet
              .addRow([
                apiRow.article,
                apiRow.supplier,
                apiRow.presenceUa,
                apiRow.displayInShowcase,
                apiRow.parentArticle,
                apiRow.price
              ])
              .commit();
          }
          apiInsertBatch.push(apiRow);
          if (apiInsertBatch.length >= apiBatchSize) {
            const batch = apiInsertBatch.splice(0, apiInsertBatch.length);
            await insertApiPreviewBatch(batch);
          }
          apiArticles.add(horoshopArticle);
          apiTotal += 1;
        }
      }
      total += 1;
    }

  if (result.rows.length < pageSize) {
      break;
    }
  }

  if (sheet) {
    await sheet.commit();
  }
  if (workbook) {
    await workbook.commit();
  }
  if (controlSheet) {
    await controlSheet.commit();
  }
  if (controlWorkbook) {
    await controlWorkbook.commit();
  }

  const hiddenGroups = await fetchHiddenParentArticles(supplierFilter);
  for (const item of hiddenGroups) {
    const itemArticle = normalizeKey(item.article);
    if (!itemArticle || presentSkus.has(itemArticle)) {
      continue;
    }
    if (apiArticles.has(itemArticle)) {
      continue;
    }
    if (apiSheet) {
      apiSheet
        .addRow([
          itemArticle,
          item.supplier || '',
          'Немає в наявності',
          false,
          item.parent_article || '',
          ''
        ])
        .commit();
    }
    apiInsertBatch.push({
      jobId,
      article: itemArticle,
      supplier: item.supplier || '',
      presenceUa: 'Немає в наявності',
      displayInShowcase: false,
      parentArticle: item.parent_article || '',
      price: ''
    });
    if (apiInsertBatch.length >= apiBatchSize) {
      const batch = apiInsertBatch.splice(0, apiInsertBatch.length);
      await insertApiPreviewBatch(batch);
    }
    apiArticles.add(itemArticle);
    apiShowTotal += 1;
  }

  const hideCandidates = await fetchSupplierHiddenCandidates(supplierFilter);
  for (const item of hideCandidates) {
    const itemArticle = normalizeKey(item.article);
    if (!itemArticle || presentSkus.has(itemArticle)) {
      continue;
    }
    if (apiArticles.has(itemArticle)) {
      continue;
    }
    if (apiSheet) {
      apiSheet
        .addRow([
          itemArticle,
          item.supplier || '',
          'Немає в наявності',
          false,
          '',
          ''
        ])
        .commit();
    }
    apiInsertBatch.push({
      jobId,
      article: itemArticle,
      supplier: item.supplier || '',
      presenceUa: 'Немає в наявності',
      displayInShowcase: false,
      parentArticle: '',
      price: ''
    });
    if (apiInsertBatch.length >= apiBatchSize) {
      const batch = apiInsertBatch.splice(0, apiInsertBatch.length);
      await insertApiPreviewBatch(batch);
    }
    apiArticles.add(itemArticle);
    apiHideTotal += 1;
  }

  if (apiInsertBatch.length) {
    const batch = apiInsertBatch.splice(0, apiInsertBatch.length);
    await insertApiPreviewBatch(batch);
  }

  if (apiSheet) {
    await apiSheet.commit();
  }
  if (apiWorkbook) {
    await apiWorkbook.commit();
  }

  await logService.log(jobId, 'info', 'Export completed', {
    filePath,
    controlFilePath,
    apiFilePath,
    filesGenerated: generateFiles,
    total,
    apiTotal,
    apiShowTotal,
    apiHideTotal,
    apiSkipped,
    supplier: supplierFilter,
    finalizeJobId
  });
  return {
    filePath,
    controlFilePath,
    apiFilePath,
    filesGenerated: generateFiles,
    total,
    apiTotal,
    apiShowTotal,
    apiHideTotal,
    apiSkipped,
    supplier: supplierFilter,
    finalizeJobId
  };
}

module.exports = {
  exportForHoroshop
};
