const API = '/admin/api';

const state = {
  suppliers: [],
  sources: [],
  selectedSupplierId: null
};

const elements = {
  supplierList: document.getElementById('supplier-list'),
  supplierTitle: document.getElementById('supplier-title'),
  supplierForm: document.getElementById('supplier-form'),
  supplierToggle: document.getElementById('toggle-supplier'),
  supplierDelete: document.getElementById('delete-supplier'),
  addSupplier: document.getElementById('add-supplier'),
  sourceForm: document.getElementById('source-form'),
  sourcesTable: document.querySelector('#sources-table tbody'),
  mappingForm: document.getElementById('mapping-form'),
  mappingHint: document.getElementById('mapping-hint'),
  importSupplier: document.getElementById('import-supplier'),
  runFinalize: document.getElementById('run-finalize'),
  runExport: document.getElementById('run-export'),
  actionResult: document.getElementById('action-result'),
  jobsTable: document.querySelector('#jobs-table tbody'),
  logsOutput: document.getElementById('logs-output'),
  apiStatus: document.getElementById('api-status'),
  lastAction: document.getElementById('last-action')
};

function setLastAction(message) {
  elements.lastAction.textContent = message;
}

async function apiFetch(path, options) {
  const res = await fetch(`${API}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json();
}

async function checkHealth() {
  try {
    const res = await fetch('/health');
    elements.apiStatus.textContent = res.ok ? 'ok' : 'error';
  } catch (err) {
    elements.apiStatus.textContent = 'offline';
  }
}

function renderSupplierList() {
  elements.supplierList.innerHTML = '';
  if (!state.suppliers.length) {
    const empty = document.createElement('div');
    empty.className = 'supplier-item';
    empty.textContent = 'Немає постачальників';
    elements.supplierList.appendChild(empty);
    return;
  }

  state.suppliers.forEach((supplier) => {
    const card = document.createElement('div');
    card.className = 'supplier-item';
    if (supplier.id === state.selectedSupplierId) {
      card.classList.add('active');
    }
    card.dataset.id = supplier.id;
    card.innerHTML = `
      <span class="supplier-name">${supplier.name}</span>
      <span class="supplier-meta">Markup: ${supplier.markup_percent}% · Priority: ${supplier.priority}</span>
    `;
    elements.supplierList.appendChild(card);
  });
}

function setSupplierMode(supplier) {
  const hasSupplier = Boolean(supplier);
  elements.supplierTitle.textContent = hasSupplier ? supplier.name : 'Оберіть постачальника';
  elements.supplierForm.name.value = supplier?.name || '';
  elements.supplierForm.markup.value = supplier?.markup_percent ?? 0;
  elements.supplierForm.priority.value = supplier?.priority ?? 3;
  elements.supplierForm.id.value = supplier?.id || '';
  elements.supplierToggle.disabled = !hasSupplier;
  elements.supplierDelete.disabled = !hasSupplier;
  elements.importSupplier.disabled = !hasSupplier;
  elements.sourceForm.querySelector('button').disabled = !hasSupplier;
  elements.mappingForm.querySelector('button').disabled = !hasSupplier;
  elements.supplierToggle.textContent = supplier?.is_active ? 'Деактивувати' : 'Активувати';
}

async function loadSuppliers() {
  state.suppliers = await apiFetch('/suppliers');
  if (!state.selectedSupplierId && state.suppliers.length) {
    state.selectedSupplierId = state.suppliers[0].id;
  }
  renderSupplierList();
  const selected = state.suppliers.find((s) => s.id === state.selectedSupplierId) || null;
  setSupplierMode(selected);
  await loadSources();
  await loadMapping();
}

async function loadSources() {
  if (!state.selectedSupplierId) {
    elements.sourcesTable.innerHTML = '';
    return;
  }
  const sources = await apiFetch(`/sources?supplierId=${state.selectedSupplierId}`);
  state.sources = sources;
  elements.sourcesTable.innerHTML = '';
  sources.forEach((source) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${source.id}</td>
      <td>${source.source_type}</td>
      <td>${source.source_url}</td>
      <td>${source.sheet_name || '-'}</td>
      <td>${source.is_active ? 'active' : 'disabled'}</td>
      <td>
        <button data-action="import" data-id="${source.id}">Import</button>
        <button data-action="edit" data-id="${source.id}">Edit</button>
        <button data-action="toggle" data-id="${source.id}">
          ${source.is_active ? 'Disable' : 'Enable'}
        </button>
      </td>
    `;
    elements.sourcesTable.appendChild(row);
  });
}

async function loadMapping() {
  if (!state.selectedSupplierId) {
    elements.mappingHint.textContent = 'Немає мапінгу.';
    return;
  }
  const mapping = await apiFetch(`/mappings/${state.selectedSupplierId}`);
  if (mapping) {
    elements.mappingForm.headerRow.value = mapping.header_row || '';
    elements.mappingForm.mapping.value = JSON.stringify(mapping.mapping, null, 2);
    elements.mappingHint.textContent = JSON.stringify(mapping, null, 2);
  } else {
    elements.mappingForm.headerRow.value = '';
    elements.mappingForm.mapping.value = '';
    elements.mappingHint.textContent = 'Немає мапінгу.';
  }
}

async function loadJobs() {
  const jobs = await apiFetch('/jobs?limit=50');
  elements.jobsTable.innerHTML = '';
  jobs.forEach((job) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${job.id}</td>
      <td>${job.type}</td>
      <td>${job.status}</td>
      <td>${job.started_at || '-'}</td>
      <td>${job.finished_at || '-'}</td>
      <td><button data-action="logs" data-id="${job.id}">Logs</button></td>
    `;
    elements.jobsTable.appendChild(row);
  });
}

async function loadLogs(jobId) {
  const logs = await apiFetch(`/logs?jobId=${jobId}`);
  const lines = logs.map((log) => {
    const data = log.data ? JSON.stringify(log.data) : '';
    return `[${log.created_at}] ${log.level.toUpperCase()} ${log.message} ${data}`;
  });
  elements.logsOutput.textContent = lines.join('\n') || 'Немає логів.';
}

elements.supplierList.addEventListener('click', async (event) => {
  const item = event.target.closest('.supplier-item');
  if (!item || !item.dataset.id) return;
  state.selectedSupplierId = Number(item.dataset.id);
  renderSupplierList();
  const selected = state.suppliers.find((s) => s.id === state.selectedSupplierId);
  setSupplierMode(selected);
  await loadSources();
  await loadMapping();
});

elements.addSupplier.addEventListener('click', () => {
  state.selectedSupplierId = null;
  renderSupplierList();
  setSupplierMode(null);
  elements.supplierForm.reset();
  elements.supplierForm.id.value = '';
});

elements.supplierForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const payload = {
    name: elements.supplierForm.name.value.trim(),
    markup_percent: Number(elements.supplierForm.markup.value || 0),
    priority: Number(elements.supplierForm.priority.value || 3)
  };
  const id = elements.supplierForm.id.value;
  if (id) {
    await apiFetch(`/suppliers/${id}`, {
      method: 'PUT',
      body: JSON.stringify(payload)
    });
    setLastAction('Постачальник оновлений');
  } else {
    const created = await apiFetch('/suppliers', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
    state.selectedSupplierId = created.id;
    setLastAction('Постачальник доданий');
  }
  await loadSuppliers();
});

elements.supplierToggle.addEventListener('click', async () => {
  const id = elements.supplierForm.id.value;
  if (!id) return;
  const supplier = state.suppliers.find((s) => s.id === Number(id));
  await apiFetch(`/suppliers/${id}`, {
    method: 'PUT',
    body: JSON.stringify({ is_active: !supplier.is_active })
  });
  setLastAction('Статус постачальника змінено');
  await loadSuppliers();
});

elements.supplierDelete.addEventListener('click', async () => {
  const id = elements.supplierForm.id.value;
  if (!id) return;
  const confirmed = confirm('Видалити постачальника?');
  if (!confirmed) return;
  await apiFetch(`/suppliers/${id}`, { method: 'DELETE' });
  state.selectedSupplierId = null;
  setLastAction('Постачальник видалений');
  await loadSuppliers();
});

elements.sourceForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (!state.selectedSupplierId) return;
  const payload = {
    supplier_id: state.selectedSupplierId,
    source_type: elements.sourceForm.type.value,
    source_url: elements.sourceForm.url.value,
    sheet_name: elements.sourceForm.sheet.value || null
  };
  await apiFetch('/sources', {
    method: 'POST',
    body: JSON.stringify(payload)
  });
  elements.sourceForm.reset();
  setLastAction('Джерело додано');
  await loadSources();
});

elements.sourcesTable.addEventListener('click', async (event) => {
  const button = event.target.closest('button');
  if (!button) return;
  const id = button.dataset.id;
  const source = state.sources.find((s) => s.id === Number(id));
  if (!source) return;

  if (button.dataset.action === 'toggle') {
    await apiFetch(`/sources/${id}`, {
      method: 'PUT',
      body: JSON.stringify({ is_active: !source.is_active })
    });
    await loadSources();
  }

  if (button.dataset.action === 'edit') {
    const url = prompt('Нове посилання?', source.source_url);
    if (url === null) return;
    const sheet = prompt('Нова назва аркуша?', source.sheet_name || '');
    await apiFetch(`/sources/${id}`, {
      method: 'PUT',
      body: JSON.stringify({
        source_url: url,
        sheet_name: sheet || null
      })
    });
    await loadSources();
  }

  if (button.dataset.action === 'import') {
    const result = await fetch('/jobs/import-source', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sourceId: Number(id) })
    }).then((res) => res.json());
    elements.actionResult.textContent = JSON.stringify(result, null, 2);
    setLastAction('Імпорт джерела');
    await loadJobs();
  }
});

elements.mappingForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (!state.selectedSupplierId) return;
  let mappingJson;
  try {
    mappingJson = JSON.parse(elements.mappingForm.mapping.value);
  } catch (err) {
    alert('Невірний JSON');
    return;
  }
  const payload = {
    mapping: mappingJson,
    header_row: elements.mappingForm.headerRow.value
      ? Number(elements.mappingForm.headerRow.value)
      : null
  };
  const result = await apiFetch(`/mappings/${state.selectedSupplierId}`, {
    method: 'POST',
    body: JSON.stringify(payload)
  });
  elements.mappingHint.textContent = JSON.stringify(result, null, 2);
  setLastAction('Мапінг збережено');
});

elements.importSupplier.addEventListener('click', async () => {
  if (!state.selectedSupplierId) return;
  const result = await fetch('/jobs/import-supplier', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ supplierId: state.selectedSupplierId })
  }).then((res) => res.json());
  elements.actionResult.textContent = JSON.stringify(result, null, 2);
  setLastAction('Імпорт постачальника');
  await loadJobs();
});

elements.runFinalize.addEventListener('click', async () => {
  const result = await fetch('/jobs/finalize', { method: 'POST' }).then((res) => res.json());
  elements.actionResult.textContent = JSON.stringify(result, null, 2);
  setLastAction('Finalize');
  await loadJobs();
});

elements.runExport.addEventListener('click', async () => {
  const result = await fetch('/jobs/export', { method: 'POST' }).then((res) => res.json());
  elements.actionResult.textContent = JSON.stringify(result, null, 2);
  setLastAction('Export');
  await loadJobs();
});

elements.jobsTable.addEventListener('click', async (event) => {
  const button = event.target.closest('button');
  if (!button) return;
  await loadLogs(button.dataset.id);
});

async function init() {
  await checkHealth();
  await loadSuppliers();
  await loadJobs();
}

init().catch((err) => {
  elements.actionResult.textContent = err.message;
});
