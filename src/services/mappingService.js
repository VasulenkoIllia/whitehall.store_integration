const DEFAULT_SYNONYMS = {
  article: ['артикул', 'sku', 'код', 'код товара', 'article'],
  size: ['розмір', 'размер', 'size'],
  quantity: ['кількість', 'количество', 'qty', 'quantity', 'остаток', 'залишок'],
  price: ['ціна', 'цена', 'price', 'дроп ціна', 'дроп цена', 'drop price'],
  extra: ['назва', 'name', 'title', 'товар']
};

function normalizeHeader(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function detectMappingFromRow(headers, synonyms = DEFAULT_SYNONYMS) {
  const normalized = headers.map(normalizeHeader);
  const mapping = {};

  Object.keys(synonyms).forEach((field) => {
    const candidates = synonyms[field].map(normalizeHeader);
    const index = normalized.findIndex((h) => candidates.includes(h));
    if (index !== -1) {
      mapping[field] = index + 1; // Excel columns are 1-based in exceljs
    }
  });

  return mapping;
}

function hasRequiredFields(mapping) {
  const hasValue = (entry) => {
    if (!entry) return false;
    if (typeof entry === 'object' && entry.type === 'static') {
      return entry.value !== null && entry.value !== undefined && String(entry.value).trim() !== '';
    }
    return Boolean(entry);
  };
  return Boolean(hasValue(mapping.article) && hasValue(mapping.price) && hasValue(mapping.quantity));
}

module.exports = {
  DEFAULT_SYNONYMS,
  normalizeHeader,
  detectMappingFromRow,
  hasRequiredFields
};
