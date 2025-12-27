function buildKey(article, size) {
  return `${article || ''}__${size || ''}`;
}

function pickBest(existing, candidate) {
  if (!existing) {
    return candidate;
  }
  if (candidate.supplierPriority < existing.supplierPriority) {
    return candidate;
  }
  if (candidate.supplierPriority > existing.supplierPriority) {
    return existing;
  }
  if (candidate.priceFinal < existing.priceFinal) {
    return candidate;
  }
  return existing;
}

function deduplicate(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const key = buildKey(row.article, row.size);
    const chosen = pickBest(map.get(key), row);
    map.set(key, chosen);
  });
  return Array.from(map.values());
}

module.exports = {
  deduplicate
};
