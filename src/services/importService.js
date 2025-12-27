const ExcelJS = require('exceljs');
const db = require('../db');
const { detectMappingFromRow, hasRequiredFields, normalizeHeader } = require('./mappingService');
const logService = require('./logService');
const { getSheetInfo, getSheetRowChunk } = require('./googleSheetsService');

function parseMappingEntry(entry) {
  if (entry && typeof entry === 'object') {
    if (entry.type === 'static') {
      return { mode: 'static', value: entry.value ?? '' };
    }
    if (entry.type === 'column') {
      const index = Number(entry.index ?? entry.value);
      return { mode: 'column', index: Number.isFinite(index) ? index : null };
    }
    if (Number.isFinite(Number(entry.index))) {
      return { mode: 'column', index: Number(entry.index) };
    }
    if (typeof entry.value !== 'undefined') {
      return { mode: 'static', value: entry.value ?? '' };
    }
  }
  if (typeof entry === 'number' && Number.isFinite(entry)) {
    return { mode: 'column', index: entry };
  }
  if (typeof entry === 'string') {
    const trimmed = entry.trim();
    if (/^\d+$/.test(trimmed)) {
      return { mode: 'column', index: Number(trimmed) };
    }
    return { mode: 'static', value: trimmed };
  }
  return { mode: null };
}

function resolveMappingValue(entry, rowValues) {
  const info = parseMappingEntry(entry);
  if (info.mode === 'static') {
    return info.value;
  }
  if (info.mode === 'column' && info.index) {
    return rowValues[info.index];
  }
  return undefined;
}

function getCellValue(values, index) {
  const value = values[index];
  if (value === null || typeof value === 'undefined') {
    return '';
  }
  return value;
}

function normalizeSize(value) {
  const str = String(value || '').trim();
  return str.includes(',') ? str.replace(/,/g, '.') : str;
}

function normalizeNumeric(value) {
  if (value === null || typeof value === 'undefined') {
    return '';
  }
  let str = String(value).trim();
  if (!str) {
    return '';
  }
  str = str.replace(/\u00A0/g, '').replace(/\s+/g, '');
  if (str.includes(',') && str.includes('.')) {
    str = str.replace(/,/g, '');
  } else if (str.includes(',') && !str.includes('.')) {
    str = str.replace(/,/g, '.');
  }
  str = str.replace(/[^0-9.-]/g, '');
  return str;
}

function parseQuantity(rawValue) {
  if (rawValue === 0 || rawValue === '0') {
    return { value: null, reason: 'zero' };
  }
  if (rawValue === '' || rawValue === null || typeof rawValue === 'undefined') {
    return { value: 1, reason: 'defaulted' };
  }
  const normalized = normalizeNumeric(rawValue);
  if (!normalized) {
    return { value: null, reason: 'invalid' };
  }
  const parsed = parseInt(normalized, 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    return { value: null, reason: 'invalid' };
  }
  return { value: parsed, reason: null };
}

function parsePrice(rawValue) {
  const normalized = normalizeNumeric(rawValue);
  if (!normalized) {
    return { value: null, reason: 'missing' };
  }
  const parsed = parseFloat(normalized);
  if (Number.isNaN(parsed) || parsed <= 0) {
    return { value: null, reason: 'invalid' };
  }
  return { value: parsed, reason: null };
}

function hasMeaningfulValue(value) {
  if (value === null || typeof value === 'undefined') {
    return false;
  }
  if (typeof value === 'string') {
    return value.trim() !== '';
  }
  return true;
}

function hasMappedColumnValues(mapping, rowValues) {
  if (!mapping) {
    return false;
  }
  const fields = ['article', 'size', 'quantity', 'price', 'extra'];
  return fields.some((field) => {
    const info = parseMappingEntry(mapping[field]);
    if (info.mode !== 'column' || !info.index) {
      return false;
    }
    return hasMeaningfulValue(rowValues[info.index]);
  });
}

function createSkipStats() {
  return {
    empty_row: 0,
    missing_article: 0,
    missing_price: 0,
    invalid_price: 0,
    zero_quantity: 0,
    invalid_quantity: 0
  };
}

function recordSkip(stats, samples, reason, rowNumber, context = {}) {
  if (!stats[reason]) {
    stats[reason] = 0;
  }
  stats[reason] += 1;
  if (samples.length < 5) {
    samples.push({ row: rowNumber, reason, ...context });
  }
}

async function insertRawBatch(rows) {
  if (!rows.length) {
    return;
  }

  const values = [];
  const placeholders = rows.map((row, idx) => {
    const base = idx * 9;
    values.push(
      row.jobId,
      row.supplierId,
      row.sourceId,
      row.article,
      row.size,
      row.quantity,
      row.price,
      row.extra,
      row.rowData
    );
    return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9})`;
  });

  await db.query(
    `INSERT INTO products_raw
     (job_id, supplier_id, source_id, article, size, quantity, price, extra, row_data)
     VALUES ${placeholders.join(', ')}`,
    values
  );
}

async function importExcelFile({
  filePath,
  supplierId,
  sourceId,
  jobId,
  mappingOverride
}) {
  const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
  let mapping = mappingOverride || null;
  let headerRowIndex = null;
  let imported = 0;
  const batch = [];
  const skipStats = createSkipStats();
  const skipSamples = [];

  for await (const worksheet of workbook) {
    for await (const row of worksheet) {
      const values = row.values || [];

      if (!mapping) {
        const candidateMapping = detectMappingFromRow(values.slice(1));
        if (hasRequiredFields(candidateMapping)) {
          mapping = candidateMapping;
          headerRowIndex = row.number;
          await logService.log(jobId, 'info', 'Header detected', {
            sheet: worksheet.name,
            headerRow: headerRowIndex,
            mapping
          });
        }
        continue;
      }

      if (row.number === headerRowIndex) {
        continue;
      }

      if (mapping && !hasMappedColumnValues(mapping, values)) {
        recordSkip(skipStats, skipSamples, 'empty_row', row.number);
        continue;
      }

      const articleValue = resolveMappingValue(mapping.article, values);
      const article = String(articleValue || '').trim();
      if (!article) {
        recordSkip(skipStats, skipSamples, 'missing_article', row.number);
        continue;
      }

      const rawQuantity = resolveMappingValue(mapping.quantity, values);
      const quantityInfo = parseQuantity(rawQuantity);
      if (quantityInfo.value === null) {
        recordSkip(
          skipStats,
          skipSamples,
          quantityInfo.reason === 'zero' ? 'zero_quantity' : 'invalid_quantity',
          row.number,
          { article }
        );
        continue;
      }
      const quantity = quantityInfo.value;

      const rawPrice = resolveMappingValue(mapping.price, values);
      const priceInfo = parsePrice(rawPrice);
      if (!priceInfo.value) {
        recordSkip(
          skipStats,
          skipSamples,
          priceInfo.reason === 'missing' ? 'missing_price' : 'invalid_price',
          row.number,
          { article }
        );
        continue;
      }
      const price = priceInfo.value;

      const sizeValue = resolveMappingValue(mapping.size, values);
      const extraValue = resolveMappingValue(mapping.extra, values);
      const size = sizeValue ? normalizeSize(sizeValue) : null;
      const extra = extraValue ? String(extraValue || '').trim() : '';

      batch.push({
        jobId,
        supplierId,
        sourceId,
        article,
        size,
        quantity,
        price,
        extra,
        rowData: JSON.stringify(values)
      });

      if (batch.length >= 500) {
        const chunk = batch.splice(0, batch.length);
        await insertRawBatch(chunk);
        imported += chunk.length;
      }
    }
    break;
  }

  if (!mapping) {
    await logService.log(jobId, 'error', 'Header not detected', { filePath });
    return { imported: 0, mapping: null };
  }

  if (batch.length) {
    const chunk = batch.splice(0, batch.length);
    await insertRawBatch(chunk);
    imported += chunk.length;
  }

  const skippedTotal = Object.values(skipStats).reduce((sum, count) => sum + count, 0);
  if (skippedTotal > 0) {
    await logService.log(jobId, 'warning', 'Import skipped rows', {
      skippedTotal,
      skipStats,
      samples: skipSamples
    });
  }

  return { imported, mapping };
}

async function importGoogleSheetSource({
  source,
  supplierId,
  jobId,
  mappingOverride,
  mappingMeta
}) {
  const logContext = {
    sourceId: source.id,
    sourceName: source.name || source.source_name || null,
    supplierName: source.supplier_name || null
  };
  const sheetName = mappingMeta?.sheet_name || source.sheet_name;
  let sheetInfo;
  try {
    sheetInfo = await getSheetInfo(source.source_url, sheetName);
  } catch (err) {
    await logService.log(jobId, 'error', 'Google sheet load failed', {
      ...logContext,
      sheetName,
      error: err.message
    });
    return { imported: 0, mapping: null, error: err.message };
  }
  const {
    sheets,
    spreadsheetId,
    sheetName: targetSheetName,
    rowCount,
    columnCount
  } = sheetInfo;

  let mapping = mappingOverride || null;
  let headerRowIndex = null;
  let imported = 0;
  const batch = [];
  const skipStats = createSkipStats();
  const skipSamples = [];
  const maxHeaderScan = 20;
  const chunkSizeRaw = Number(process.env.GOOGLE_SHEETS_CHUNK_SIZE || 10000);
  const chunkSize = Number.isFinite(chunkSizeRaw) ? Math.max(1000, chunkSizeRaw) : 10000;

  if (mapping) {
    if (mappingMeta?.source_id && Number(mappingMeta.source_id) !== Number(source.id)) {
      const error = 'Mapping source mismatch. Please remap columns.';
      await logService.log(jobId, 'error', 'Mapping source mismatch', {
        ...logContext,
        mappingSourceId: mappingMeta.source_id
      });
      return { imported: 0, mapping, error };
    }

    const headerRowRaw = mappingMeta?.header_row;
    const headerRowValue =
      headerRowRaw === null || typeof headerRowRaw === 'undefined'
        ? 1
        : Number(headerRowRaw);
    const hasHeader = Number.isFinite(headerRowValue) && headerRowValue > 0;
    const requiredFields = ['article', 'quantity', 'price'];
    const errors = [];

    if (hasHeader) {
      if (rowCount && headerRowValue > rowCount) {
        const error = 'Header row out of range. Please remap columns.';
        await logService.log(jobId, 'error', 'Header row out of range', {
          ...logContext,
          headerRow: headerRowValue
        });
        return { imported: 0, mapping, error };
      }
      const headerRows = await getSheetRowChunk(
        sheets,
        spreadsheetId,
        targetSheetName,
        headerRowValue,
        headerRowValue
      );
      const headerRow = headerRows[0] || null;
      if (!headerRow) {
        const error = 'Header row not found. Please remap columns.';
        await logService.log(jobId, 'error', 'Header row not found', {
          ...logContext,
          headerRow: headerRowValue
        });
        return { imported: 0, mapping, error };
      }

      const expectedHeaders = mappingMeta?.headers || {};
      const maxColumns =
        Number.isFinite(Number(columnCount)) && Number(columnCount) > 0
          ? Number(columnCount)
          : headerRow.length;
      requiredFields.forEach((field) => {
        const info = parseMappingEntry(mapping[field]);
        if (info.mode === 'static') {
          if (!info.value && info.value !== 0) {
            errors.push(`Missing static value for ${field}`);
          }
          return;
        }
        const index = info.index;
        if (!index) {
          errors.push(`Missing mapping for ${field}`);
          return;
        }
        if (maxColumns && index > maxColumns) {
          errors.push(`Column index out of range for ${field}`);
          return;
        }
        const expected = expectedHeaders[field];
        const actual = headerRow[index - 1];
        if (expected && normalizeHeader(expected) !== normalizeHeader(actual)) {
          errors.push(
            `Header mismatch for ${field}: expected "${expected}" got "${actual ?? ''}"`
          );
        }
      });

      Object.keys(expectedHeaders).forEach((field) => {
        if (requiredFields.includes(field)) {
          return;
        }
        const info = parseMappingEntry(mapping[field]);
        if (info.mode === 'static') {
          return;
        }
        const index = info.index;
        if (!index || (maxColumns && index > maxColumns)) {
          errors.push(`Column index out of range for ${field}`);
          return;
        }
        const expected = expectedHeaders[field];
        const actual = headerRow[index - 1];
        if (expected && normalizeHeader(expected) !== normalizeHeader(actual)) {
          errors.push(
            `Header mismatch for ${field}: expected "${expected}" got "${actual ?? ''}"`
          );
        }
      });

      headerRowIndex = headerRowValue;
    } else {
      const scanEnd = rowCount
        ? Math.min(Math.max(maxHeaderScan, 1), rowCount)
        : Math.max(maxHeaderScan, 1);
      const sampleRows = await getSheetRowChunk(
        sheets,
        spreadsheetId,
        targetSheetName,
        1,
        scanEnd
      );
      const maxColumns =
        Number.isFinite(Number(columnCount)) && Number(columnCount) > 0
          ? Number(columnCount)
          : sampleRows.reduce((max, row) => Math.max(max, row.length), 0);
      requiredFields.forEach((field) => {
        const info = parseMappingEntry(mapping[field]);
        if (info.mode === 'static') {
          if (!info.value && info.value !== 0) {
            errors.push(`Missing static value for ${field}`);
          }
          return;
        }
        const index = info.index;
        if (!index) {
          errors.push(`Missing mapping for ${field}`);
          return;
        }
        if (maxColumns && index > maxColumns) {
          errors.push(`Column index out of range for ${field}`);
        }
      });
      headerRowIndex = null;
    }

    if (errors.length) {
      const error = 'Mapping validation failed. Please remap columns.';
      await logService.log(jobId, 'error', 'Mapping validation failed', {
        ...logContext,
        errors
      });
      return { imported: 0, mapping, error };
    }
  }

  if (!mapping) {
    if (rowCount === 0) {
      await logService.log(jobId, 'error', 'Google sheet is empty', {
        ...logContext
      });
      return { imported: 0, mapping: null, error: 'sheet is empty' };
    }
    const scanEnd = rowCount
      ? Math.min(Math.max(maxHeaderScan, 1), rowCount)
      : Math.max(maxHeaderScan, 1);
    const scanRows = await getSheetRowChunk(
      sheets,
      spreadsheetId,
      targetSheetName,
      1,
      scanEnd
    );
    if (!scanRows.length) {
      await logService.log(jobId, 'error', 'Google sheet is empty', {
        ...logContext
      });
      return { imported: 0, mapping: null, error: 'sheet is empty' };
    }

    for (let i = 0; i < scanRows.length; i += 1) {
      const candidateMapping = detectMappingFromRow(scanRows[i]);
      if (hasRequiredFields(candidateMapping)) {
        mapping = candidateMapping;
        headerRowIndex = i + 1;
        await logService.log(jobId, 'info', 'Header detected (Google Sheets)', {
          ...logContext,
          headerRow: headerRowIndex,
          mapping
        });
        break;
      }
    }
  }

  if (!mapping) {
    await logService.log(jobId, 'error', 'Header not detected (Google Sheets)', {
      ...logContext
    });
    return { imported: 0, mapping: null, error: 'header not detected' };
  }

  if (rowCount === 0) {
    await logService.log(jobId, 'error', 'Google sheet is empty', {
      ...logContext
    });
    return { imported: 0, mapping: null, error: 'sheet is empty' };
  }

  let startRow = 1;
  let hasData = false;
  let logLoaded = false;

  while (true) {
    if (rowCount && startRow > rowCount) {
      break;
    }
    const endRow = rowCount
      ? Math.min(startRow + chunkSize - 1, rowCount)
      : startRow + chunkSize - 1;
    const rows = await getSheetRowChunk(
      sheets,
      spreadsheetId,
      targetSheetName,
      startRow,
      endRow
    );
    if (!rows.length) {
      break;
    }
    hasData = true;
    if (!logLoaded && jobId) {
      logLoaded = true;
      await logService.log(jobId, 'info', 'Google sheet loaded', {
        ...logContext,
        sheetName: targetSheetName
      });
    }

    for (let i = 0; i < rows.length; i += 1) {
      const rowNumber = startRow + i;
      if (headerRowIndex && rowNumber === headerRowIndex) {
        continue;
      }

      const rowValues = [null, ...rows[i]];

      if (!hasMappedColumnValues(mapping, rowValues)) {
        recordSkip(skipStats, skipSamples, 'empty_row', rowNumber);
        continue;
      }

      const articleValue = resolveMappingValue(mapping.article, rowValues);
      const article = String(articleValue || '').trim();
      if (!article) {
        recordSkip(skipStats, skipSamples, 'missing_article', rowNumber);
        continue;
      }

      const rawQuantity = resolveMappingValue(mapping.quantity, rowValues);
      const quantityInfo = parseQuantity(rawQuantity);
      if (quantityInfo.value === null) {
        recordSkip(
          skipStats,
          skipSamples,
          quantityInfo.reason === 'zero' ? 'zero_quantity' : 'invalid_quantity',
          rowNumber,
          { article }
        );
        continue;
      }
      const quantity = quantityInfo.value;

      const rawPrice = resolveMappingValue(mapping.price, rowValues);
      const priceInfo = parsePrice(rawPrice);
      if (!priceInfo.value) {
        recordSkip(
          skipStats,
          skipSamples,
          priceInfo.reason === 'missing' ? 'missing_price' : 'invalid_price',
          rowNumber,
          { article }
        );
        continue;
      }
      const price = priceInfo.value;

      const sizeValue = resolveMappingValue(mapping.size, rowValues);
      const extraValue = resolveMappingValue(mapping.extra, rowValues);
      const size = sizeValue ? normalizeSize(sizeValue) : null;
      const extra = extraValue ? String(extraValue || '').trim() : '';

      batch.push({
        jobId,
        supplierId,
        sourceId: source.id,
        article,
        size,
        quantity,
        price,
        extra,
        rowData: JSON.stringify(rows[i])
      });

      if (batch.length >= 500) {
        const chunk = batch.splice(0, batch.length);
        await insertRawBatch(chunk);
        imported += chunk.length;
      }
    }

    startRow += chunkSize;
  }

  if (!hasData) {
    await logService.log(jobId, 'error', 'Google sheet is empty', {
      sourceId: source.id
    });
    return { imported: 0, mapping: null, error: 'sheet is empty' };
  }

  if (batch.length) {
    const chunk = batch.splice(0, batch.length);
    await insertRawBatch(chunk);
    imported += chunk.length;
  }

  const skippedTotal = Object.values(skipStats).reduce((sum, count) => sum + count, 0);
  if (skippedTotal > 0) {
    await logService.log(jobId, 'warning', 'Import skipped rows', {
      sourceId: source.id,
      skippedTotal,
      skipStats,
      samples: skipSamples
    });
  }

  return { imported, mapping };
}

module.exports = {
  importExcelFile,
  importGoogleSheetSource
};
