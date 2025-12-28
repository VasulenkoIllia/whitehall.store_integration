import React, { useEffect, useMemo, useState } from 'react';
import {
  App as AntApp,
  Layout,
  Tabs,
  Card,
  Typography,
  Space,
  Button,
  Tag,
  Table,
  List,
  Row,
  Col,
  Select,
  Checkbox,
  Radio,
  Modal,
  Form,
  Input,
  InputNumber,
  Switch,
  Popconfirm,
  Divider
} from 'antd';

const { Header, Content } = Layout;
const { Title, Text } = Typography;

const API = '/admin/api';

const WEEKDAY_OPTIONS = [
  { label: 'Пн', value: '1' },
  { label: 'Вт', value: '2' },
  { label: 'Ср', value: '3' },
  { label: 'Чт', value: '4' },
  { label: 'Пт', value: '5' },
  { label: 'Сб', value: '6' },
  { label: 'Нд', value: '0' }
];

const MONTH_DAY_OPTIONS = Array.from({ length: 31 }, (_, idx) => ({
  label: String(idx + 1),
  value: String(idx + 1)
}));

function columnLetter(index) {
  let result = '';
  let value = index;
  while (value > 0) {
    const mod = (value - 1) % 26;
    result = String.fromCharCode(65 + mod) + result;
    value = Math.floor((value - 1) / 26);
  }
  return result;
}

function parseCronToSchedule(cron) {
  if (!cron) {
    return { mode: 'daily', hour: 3, minute: 0, daysOfWeek: ['1'], daysOfMonth: ['1'] };
  }
  const parts = cron.trim().split(/\s+/);
  if (parts.length < 5) {
    return { mode: 'daily', hour: 3, minute: 0, daysOfWeek: ['1'], daysOfMonth: ['1'] };
  }
  const [minute, hour, dayOfMonth, _month, dayOfWeek] = parts;
  const safeMinute = /^\d+$/.test(minute) ? Number(minute) : 0;
  const safeHour = /^\d+$/.test(hour) ? Number(hour) : 3;

  if (dayOfMonth !== '*' && dayOfWeek === '*') {
    const days = dayOfMonth.split(',').map((value) => value.trim()).filter(Boolean);
    return {
      mode: 'monthly',
      hour: safeHour,
      minute: safeMinute,
      daysOfMonth: days.length ? days : ['1'],
      daysOfWeek: ['1']
    };
  }

  if (dayOfWeek !== '*' && dayOfMonth === '*') {
    const days = dayOfWeek
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean)
      .map((value) => (value === '7' ? '0' : value));
    return {
      mode: 'weekly',
      hour: safeHour,
      minute: safeMinute,
      daysOfWeek: days.length ? days : ['1'],
      daysOfMonth: ['1']
    };
  }

  return {
    mode: 'daily',
    hour: safeHour,
    minute: safeMinute,
    daysOfWeek: ['1'],
    daysOfMonth: ['1']
  };
}

function scheduleToCron(schedule) {
  const minute = Math.min(Math.max(Number(schedule.minute) || 0, 0), 59);
  const hour = Math.min(Math.max(Number(schedule.hour) || 0, 0), 23);

  if (schedule.mode === 'weekly') {
    const days = schedule.daysOfWeek?.length ? schedule.daysOfWeek : ['1'];
    return `${minute} ${hour} * * ${days.join(',')}`;
  }

  if (schedule.mode === 'monthly') {
    const days = schedule.daysOfMonth?.length ? schedule.daysOfMonth : ['1'];
    return `${minute} ${hour} ${days.join(',')} * *`;
  }

  return `${minute} ${hour} * * *`;
}

function formatSchedule(schedule) {
  if (!schedule) {
    return '-';
  }
  const hour = String(Number(schedule.hour) || 0).padStart(2, '0');
  const minute = String(Number(schedule.minute) || 0).padStart(2, '0');
  if (schedule.mode === 'weekly') {
    const days = schedule.daysOfWeek || [];
    const labels = WEEKDAY_OPTIONS.filter((item) => days.includes(item.value)).map(
      (item) => item.label
    );
    return `Щотижня: ${labels.length ? labels.join(', ') : '-'} о ${hour}:${minute}`;
  }
  if (schedule.mode === 'monthly') {
    const days = schedule.daysOfMonth || [];
    return `Щомісяця: ${days.length ? days.join(', ') : '-'} о ${hour}:${minute}`;
  }
  return `Щодня о ${hour}:${minute}`;
}

function normalizeMappingEntry(entry) {
  if (entry && typeof entry === 'object') {
    if (entry.type === 'static') {
      const value = entry.value ?? '';
      return { mode: 'static', value, allowEmpty: value === '' };
    }
    if (entry.type === 'column') {
      const index = Number(entry.index ?? entry.value);
      return { mode: 'column', value: Number.isFinite(index) ? index : null, allowEmpty: false };
    }
    if (Number.isFinite(Number(entry.index))) {
      return { mode: 'column', value: Number(entry.index), allowEmpty: false };
    }
    if (typeof entry.value !== 'undefined') {
      const value = entry.value ?? '';
      return { mode: 'static', value, allowEmpty: value === '' };
    }
  }
  if (typeof entry === 'number' && Number.isFinite(entry)) {
    return { mode: 'column', value: entry, allowEmpty: false };
  }
  if (typeof entry === 'string') {
    const trimmed = entry.trim();
    if (/^\d+$/.test(trimmed)) {
      return { mode: 'column', value: Number(trimmed), allowEmpty: false };
    }
    return { mode: 'static', value: trimmed, allowEmpty: trimmed === '' };
  }
  return { mode: 'column', value: null, allowEmpty: false };
}

function createEmptyMappingFields() {
  return {
    article: { mode: 'column', value: null, allowEmpty: false },
    size: { mode: 'column', value: null, allowEmpty: false },
    quantity: { mode: 'column', value: null, allowEmpty: false },
    price: { mode: 'column', value: null, allowEmpty: false },
    extra: { mode: 'column', value: null, allowEmpty: false }
  };
}

function isMappingFieldSet(entry, options = {}) {
  if (!entry) {
    return false;
  }
  const allowEmpty = options.allowEmpty !== false;
  if (entry.mode === 'static') {
    if (entry.allowEmpty && allowEmpty) {
      return true;
    }
    return entry.value !== null && entry.value !== undefined && String(entry.value).trim() !== '';
  }
  return Number.isFinite(Number(entry.value));
}


async function apiFetch(path, options) {
  const res = await fetch(`${API}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });
  if (!res.ok) {
    let errorPayload = null;
    try {
      errorPayload = await res.json();
    } catch {
      errorPayload = null;
    }
    const fallbackText = errorPayload?.error || (await res.text());
    throw new Error(fallbackText || res.statusText);
  }
  return res.json();
}

async function jobFetch(path, body) {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) {
    let errorPayload = null;
    try {
      errorPayload = await res.json();
    } catch {
      errorPayload = null;
    }
    const fallbackText = errorPayload?.error || (await res.text());
    throw new Error(fallbackText || res.statusText);
  }
  return res.json();
}

const FRIENDLY_ERROR_MAP = [
  {
    match: /The caller does not have permission/i,
    message:
      'Немає доступу до Google Sheets. Поділіться таблицею з email сервіс-акаунта.'
  },
  {
    match: /Google Sheets API has not been used/i,
    message:
      'Google Sheets API вимкнено. Увімкніть API у Google Cloud і спробуйте знову.'
  },
  {
    match: /Google credentials are not set/i,
    message: 'Не заповнені Google ключі у .env.'
  },
  {
    match: /Invalid Google Sheets URL/i,
    message: 'Некоректне посилання або ID Google Sheets.'
  },
  {
    match: /Header row out of range/i,
    message: 'Рядок заголовків виходить за межі таблиці. Перевірте номер рядка.'
  },
  {
    match: /Аркуш не знайдено або перейменовано/i,
    message: 'Аркуш не знайдено або перейменовано. Оновіть назву аркуша у мапінгу.'
  },
  {
    match: /Google Sheets не знайдено або доступ закритий/i,
    message: 'Google Sheets недоступна або доступ закритий. Поділіться таблицею з сервіс-акаунтом.'
  },
  {
    match: /source not found/i,
    message: 'Джерело не знайдено.'
  },
  {
    match: /supplier name already exists/i,
    message: 'Постачальник з такою назвою вже існує.'
  },
  {
    match: /source already exists/i,
    message: 'Джерело з таким посиланням вже існує.'
  },
  {
    match: /No import_all job found/i,
    message: 'Спочатку запустіть “Імпорт усіх”.'
  },
  {
    match: /Another job is running/i,
    message: 'Інший процес вже виконується. Дочекайтесь завершення.'
  }
];

const BLOCKING_JOB_TYPES = new Set([
  'update_pipeline',
  'import_all',
  'import_source',
  'import_supplier',
  'finalize',
  'export',
  'horoshop_sync',
  'horoshop_import'
]);

function formatErrorMessage(error) {
  const message = (error?.message || '').trim();
  if (!message) {
    return 'Невідома помилка';
  }
  const matched = FRIENDLY_ERROR_MAP.find((item) => item.match.test(message));
  return matched ? matched.message : message;
}

function formatDuration(ms) {
  if (!Number.isFinite(ms) || ms < 0) {
    return null;
  }
  const totalMinutes = Math.max(1, Math.round(ms / 60000));
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  return `${hours} год ${minutes} хв`;
}

function formatDateTime(value) {
  if (!value) {
    return '-';
  }
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return String(value);
  }
  return new Intl.DateTimeFormat('uk-UA', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(date);
}

function App() {
  const { message } = AntApp.useApp();
  const isReadOnly = useMemo(() => !window.location.pathname.startsWith('/admin'), []);
  const [apiStatus, setApiStatus] = useState('unknown');
  const [stats, setStats] = useState(null);
  const [logs, setLogs] = useState([]);
  const [jobs, setJobs] = useState([]);
  const [actionResult, setActionResult] = useState('Немає дій.');
  const [actionInProgress, setActionInProgress] = useState(false);
  const [selectedLog, setSelectedLog] = useState(null);
  const [logModalOpen, setLogModalOpen] = useState(false);
  const [logsErrorOnly, setLogsErrorOnly] = useState(false);
  const hasBlockingJob = useMemo(
    () =>
      jobs.some(
        (job) =>
          job.status === 'running' &&
          BLOCKING_JOB_TYPES.has(String(job.type || '').toLowerCase())
      ),
    [jobs]
  );
  const isBusy = actionInProgress || hasBlockingJob;

  const [suppliers, setSuppliers] = useState([]);
  const [selectedSupplier, setSelectedSupplier] = useState(null);
  const [supplierCreateOpen, setSupplierCreateOpen] = useState(false);
  const [supplierSettingsOpen, setSupplierSettingsOpen] = useState(false);
  const [selectedSupplierIds, setSelectedSupplierIds] = useState([]);
  const [bulkMarkupOpen, setBulkMarkupOpen] = useState(false);
  const [bulkMarkupValue, setBulkMarkupValue] = useState(null);
  const [sources, setSources] = useState([]);
  const [sourceModalOpen, setSourceModalOpen] = useState(false);
  const [sourceEditing, setSourceEditing] = useState(null);
  const [mappingFields, setMappingFields] = useState(() => createEmptyMappingFields());
  const [mappingHeaders, setMappingHeaders] = useState([]);
  const [mappingSampleRows, setMappingSampleRows] = useState([]);
  const [mappingSourceId, setMappingSourceId] = useState(null);
  const [mappingSheets, setMappingSheets] = useState([]);
  const [mappingSheetName, setMappingSheetName] = useState(null);
  const [mappingSheetsLoading, setMappingSheetsLoading] = useState(false);
  const [headerRow, setHeaderRow] = useState(1);
  const [headerRowBackup, setHeaderRowBackup] = useState(1);
  const [mappingLoading, setMappingLoading] = useState(false);

  const [mergedData, setMergedData] = useState({ rows: [], total: 0, jobId: null });
  const [mergedPage, setMergedPage] = useState(1);
  const [mergedPageSize, setMergedPageSize] = useState(50);
  const [mergedSearch, setMergedSearch] = useState('');
  const [mergedSort, setMergedSort] = useState('article_asc');

  const [finalData, setFinalData] = useState({ rows: [], total: 0, jobId: null });
  const [finalPage, setFinalPage] = useState(1);
  const [finalPageSize, setFinalPageSize] = useState(50);
  const [finalSearch, setFinalSearch] = useState('');
  const [finalSort, setFinalSort] = useState('article_asc');
  const [finalSupplierId, setFinalSupplierId] = useState(null);

  const [horoshopData, setHoroshopData] = useState({ rows: [], total: 0 });
  const [horoshopPage, setHoroshopPage] = useState(1);
  const [horoshopPageSize, setHoroshopPageSize] = useState(50);
  const [horoshopSearch, setHoroshopSearch] = useState('');

  const [horoshopApiData, setHoroshopApiData] = useState({ rows: [], total: 0 });
  const [horoshopApiPage, setHoroshopApiPage] = useState(1);
  const [horoshopApiPageSize, setHoroshopApiPageSize] = useState(50);
  const [horoshopApiSearch, setHoroshopApiSearch] = useState('');
  const [horoshopApiSupplier, setHoroshopApiSupplier] = useState('');
  const [horoshopSuppliers, setHoroshopSuppliers] = useState([]);
  const [manualHoroshopSupplier, setManualHoroshopSupplier] = useState('');

  const [compareData, setCompareData] = useState({ rows: [], total: 0 });
  const [comparePage, setComparePage] = useState(1);
  const [comparePageSize, setComparePageSize] = useState(50);
  const [compareSearch, setCompareSearch] = useState('');
  const [compareSupplierId, setCompareSupplierId] = useState(null);
  const [compareMissingOnly, setCompareMissingOnly] = useState(true);

  const [cronSettings, setCronSettings] = useState([]);
  const [cronSaving, setCronSaving] = useState(false);
  const [cronUi, setCronUi] = useState({});

  const [supplierForm] = Form.useForm();
  const [supplierCreateForm] = Form.useForm();
  const [sourceForm] = Form.useForm();
  const [priceOverrideForm] = Form.useForm();
  const minProfitEnabledCreate = Form.useWatch('min_profit_enabled', supplierCreateForm);
  const minProfitEnabledEdit = Form.useWatch('min_profit_enabled', supplierForm);

  const [priceOverrideModalOpen, setPriceOverrideModalOpen] = useState(false);
  const [priceOverrideRow, setPriceOverrideRow] = useState(null);
  const [priceOverrideLoading, setPriceOverrideLoading] = useState(false);

  const showError = (err) => {
    message.error(formatErrorMessage(err));
  };

  const closeSupplierSettings = () => {
    setSupplierSettingsOpen(false);
    setSelectedSupplier(null);
    setSources([]);
    setMappingHeaders([]);
    setMappingSampleRows([]);
    setMappingSourceId(null);
    setMappingSheets([]);
    setMappingSheetName(null);
    setMappingFields(createEmptyMappingFields());
    setHeaderRow(1);
    setHeaderRowBackup(1);
  };

  const openPriceOverride = (row = null) => {
    setPriceOverrideRow(row);
    setPriceOverrideModalOpen(true);
    priceOverrideForm.setFieldsValue({
      article: row?.article || '',
      size: row?.size || '',
      price_final: row?.override_price ?? row?.price_final ?? null,
      notes: row?.override_notes || '',
      is_active: row?.override_id ? true : true
    });
  };

  const closePriceOverride = () => {
    setPriceOverrideModalOpen(false);
    setPriceOverrideRow(null);
    priceOverrideForm.resetFields();
  };

  const savePriceOverride = async () => {
    try {
      const values = await priceOverrideForm.validateFields();
      setPriceOverrideLoading(true);
      if (priceOverrideRow?.override_id) {
        await apiFetch(`/price-overrides/${priceOverrideRow.override_id}`, {
          method: 'PUT',
          body: JSON.stringify({
            price_final: values.price_final,
            notes: values.notes || null,
            is_active: values.is_active
          })
        });
      } else {
        await apiFetch('/price-overrides', {
          method: 'POST',
          body: JSON.stringify({
            article: values.article,
            size: values.size || null,
            price_final: values.price_final,
            notes: values.notes || null
          })
        });
      }
      message.success('Ручну ціну збережено');
      closePriceOverride();
      refreshFinalPreview();
    } catch (err) {
      showError(err);
    } finally {
      setPriceOverrideLoading(false);
    }
  };

  const refreshDashboard = async (errorOnly = false) => {
    const logsPath = errorOnly ? '/logs?limit=50&level=error' : '/logs?limit=50';
    const [statsData, logsData, jobsData] = await Promise.all([
      apiFetch('/stats'),
      apiFetch(logsPath),
      apiFetch('/jobs?limit=20')
    ]);
    setStats(statsData);
    setLogs(logsData);
    setJobs(jobsData);
  };

  const downloadExport = (path, params = {}) => {
    const searchParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value === null || value === '' || typeof value === 'undefined') {
        return;
      }
      searchParams.set(key, String(value));
    });
    const url = `${API}${path}${searchParams.toString() ? `?${searchParams}` : ''}`;
    const link = document.createElement('a');
    link.href = url;
    link.rel = 'noopener';
    document.body.appendChild(link);
    link.click();
    link.remove();
  };

  const refreshSuppliers = async () => {
    const data = await apiFetch('/suppliers');
    setSuppliers(data);
  };

  const applyMappingData = (mappingData, sourceId, sourceList) => {
    const storedHeaderRow = Number.isFinite(Number(mappingData?.header_row))
      ? Number(mappingData.header_row)
      : 1;
    setHeaderRow(storedHeaderRow);
    if (storedHeaderRow > 0) {
      setHeaderRowBackup(storedHeaderRow);
    }
    setMappingFields({
      article: normalizeMappingEntry(mappingData?.mapping?.article),
      size: normalizeMappingEntry(mappingData?.mapping?.size),
      quantity: normalizeMappingEntry(mappingData?.mapping?.quantity),
      price: normalizeMappingEntry(mappingData?.mapping?.price),
      extra: normalizeMappingEntry(mappingData?.mapping?.extra)
    });
    const source = (sourceList || sources).find((item) => item.id === sourceId);
    setMappingSheetName(mappingData?.mapping_meta?.sheet_name || source?.sheet_name || null);
  };

  const refreshSupplierDetails = async (supplier) => {
    if (!supplier) return;
    const sourcesData = await apiFetch(`/sources?supplierId=${supplier.id}`);
    setSources(sourcesData);
    setMappingHeaders([]);
    setMappingSampleRows([]);
    setMappingSheets([]);
    const nextSourceId =
      mappingSourceId && sourcesData.some((item) => item.id === mappingSourceId)
        ? mappingSourceId
        : sourcesData[0]?.id || null;
    setMappingSourceId(nextSourceId);
    if (!nextSourceId) {
      applyMappingData(null, null, sourcesData);
      return;
    }
    const mappingData = await apiFetch(`/mappings/${supplier.id}?sourceId=${nextSourceId}`);
    applyMappingData(mappingData, nextSourceId, sourcesData);
  };

  const refreshMergedPreview = async (
    page = mergedPage,
    pageSize = mergedPageSize,
    search = mergedSearch,
    sort = mergedSort
  ) => {
    const offset = (page - 1) * pageSize;
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : '';
    const sortParam = sort ? `&sort=${encodeURIComponent(sort)}` : '';
    const result = await apiFetch(
      `/merged-preview?limit=${pageSize}&offset=${offset}${searchParam}${sortParam}`
    );
    setMergedData(result);
    setMergedPage(page);
    setMergedPageSize(pageSize);
  };

  const refreshFinalPreview = async (
    page = finalPage,
    pageSize = finalPageSize,
    search = finalSearch,
    sort = finalSort,
    supplierId = finalSupplierId
  ) => {
    const offset = (page - 1) * pageSize;
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : '';
    const sortParam = sort ? `&sort=${encodeURIComponent(sort)}` : '';
    const supplierParam = supplierId ? `&supplierId=${supplierId}` : '';
    const result = await apiFetch(
      `/final-preview?limit=${pageSize}&offset=${offset}${searchParam}${sortParam}${supplierParam}`
    );
    setFinalData(result);
    setFinalPage(page);
    setFinalPageSize(pageSize);
  };

  const refreshHoroshopPreview = async (
    page = horoshopPage,
    pageSize = horoshopPageSize,
    search = horoshopSearch
  ) => {
    const offset = (page - 1) * pageSize;
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : '';
    const result = await apiFetch(
      `/horoshop-preview?limit=${pageSize}&offset=${offset}${searchParam}`
    );
    setHoroshopData(result);
    setHoroshopPage(page);
    setHoroshopPageSize(pageSize);
  };

  const refreshHoroshopSuppliers = async () => {
    const data = await apiFetch('/horoshop-suppliers');
    const list = Array.isArray(data?.suppliers) ? data.suppliers : [];
    const dropSupplier = list.find(
      (supplier) => String(supplier || '').toLowerCase() === 'drop'
    );
    setHoroshopSuppliers(list);
    setHoroshopApiSupplier((current) => {
      if (!current) {
        return '';
      }
      const matched = list.find(
        (supplier) => String(supplier || '').toLowerCase() === String(current).toLowerCase()
      );
      return matched || '';
    });
    setManualHoroshopSupplier((current) => {
      if (current) {
        const matched = list.find(
          (supplier) => String(supplier || '').toLowerCase() === String(current).toLowerCase()
        );
        if (matched) {
          return matched;
        }
      }
      if (dropSupplier) {
        return dropSupplier;
      }
      return list[0] || '';
    });
  };

  const refreshHoroshopApiPreview = async (
    page = horoshopApiPage,
    pageSize = horoshopApiPageSize,
    search = horoshopApiSearch,
    supplier = horoshopApiSupplier
  ) => {
    const offset = (page - 1) * pageSize;
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : '';
    const supplierParam = supplier ? `&supplier=${encodeURIComponent(supplier)}` : '';
    const result = await apiFetch(
      `/horoshop-api-preview?limit=${pageSize}&offset=${offset}${searchParam}${supplierParam}`
    );
    setHoroshopApiData(result);
    setHoroshopApiPage(page);
    setHoroshopApiPageSize(pageSize);
  };

  const refreshComparePreview = async (
    page = comparePage,
    pageSize = comparePageSize,
    search = compareSearch,
    supplierId = compareSupplierId,
    missingOnly = compareMissingOnly
  ) => {
    const offset = (page - 1) * pageSize;
    const searchParam = search ? `&search=${encodeURIComponent(search)}` : '';
    const supplierParam = supplierId ? `&supplierId=${supplierId}` : '';
    const missingParam = missingOnly ? '&missingOnly=true' : '';
    const result = await apiFetch(
      `/compare-preview?limit=${pageSize}&offset=${offset}${searchParam}${supplierParam}${missingParam}`
    );
    setCompareData(result);
    setComparePage(page);
    setComparePageSize(pageSize);
  };

  const refreshCronSettings = async () => {
    const data = await apiFetch('/cron-settings');
    const filtered = data.filter(
      (item) =>
        item.name === 'update_pipeline' ||
        item.name === 'horoshop_sync' ||
        item.name === 'cleanup'
    );
    const normalized = filtered.map((item) => ({
      ...item,
      meta: item.name === 'update_pipeline' ? item.meta || { supplier: 'drop' } : item.meta || {}
    }));
    setCronSettings(normalized);
    const ui = {};
    normalized.forEach((item) => {
      ui[item.name] = parseCronToSchedule(item.cron);
    });
    setCronUi(ui);
  };

  const checkHealth = async () => {
    try {
      const res = await fetch('/health');
      setApiStatus(res.ok ? 'ok' : 'error');
    } catch (err) {
      setApiStatus('offline');
    }
  };

  useEffect(() => {
    checkHealth();
    refreshDashboard(logsErrorOnly).catch((err) => showError(err));
    refreshSuppliers().catch((err) => showError(err));
    refreshMergedPreview().catch((err) => showError(err));
    refreshFinalPreview().catch((err) => showError(err));
    refreshHoroshopPreview().catch((err) => showError(err));
    refreshHoroshopApiPreview().catch((err) => showError(err));
    refreshHoroshopSuppliers().catch((err) => showError(err));
    refreshComparePreview().catch((err) => showError(err));
    refreshCronSettings().catch((err) => showError(err));
  }, []);

  useEffect(() => {
    refreshDashboard(logsErrorOnly).catch((err) => showError(err));
  }, [logsErrorOnly]);

  useEffect(() => {
    if (!sources.length) {
      setMappingSourceId(null);
      setMappingHeaders([]);
      setMappingSampleRows([]);
      setMappingSheets([]);
      setMappingSheetName(null);
      return;
    }
    if (!mappingSourceId || !sources.some((source) => source.id === mappingSourceId)) {
      setMappingSourceId(sources[0].id);
    }
  }, [sources, mappingSourceId]);

  useEffect(() => {
    if (!mappingSourceId) {
      return;
    }
    setMappingSheets([]);
    setMappingHeaders([]);
    setMappingSampleRows([]);
    if (selectedSupplier && sources.some((item) => item.id === mappingSourceId)) {
      apiFetch(`/mappings/${selectedSupplier.id}?sourceId=${mappingSourceId}`)
        .then((mappingData) => applyMappingData(mappingData, mappingSourceId))
        .catch((err) => showError(err));
    } else {
      const source = sources.find((item) => item.id === mappingSourceId);
      setMappingSheetName((current) => current || source?.sheet_name || null);
    }
  }, [mappingSourceId, sources, selectedSupplier]);

  useEffect(() => {
    if (!mappingSheetName) {
      return;
    }
    setMappingHeaders([]);
    setMappingSampleRows([]);
  }, [mappingSheetName]);

  const openSupplierCreate = () => {
    supplierCreateForm.resetFields();
    supplierCreateForm.setFieldsValue({
      priority: 100,
      min_profit_enabled: true,
      min_profit_amount: 500
    });
    setSupplierCreateOpen(true);
  };

  const openBulkMarkup = () => {
    setBulkMarkupValue(null);
    setBulkMarkupOpen(true);
  };

  const applyBulkMarkup = async () => {
    if (!selectedSupplierIds.length) {
      message.error('Оберіть постачальників');
      return;
    }
    if (bulkMarkupValue === null || typeof bulkMarkupValue === 'undefined') {
      message.error('Вкажіть націнку');
      return;
    }
    try {
      await apiFetch('/suppliers/bulk', {
        method: 'PUT',
        body: JSON.stringify({
          supplier_ids: selectedSupplierIds,
          markup_percent: bulkMarkupValue
        })
      });
      setBulkMarkupOpen(false);
      setBulkMarkupValue(null);
      setSelectedSupplierIds([]);
      await refreshSuppliers();
      message.success('Націнку оновлено');
    } catch (err) {
      showError(err);
    }
  };

  const openSupplierSettings = async (supplier) => {
    setSelectedSupplier(supplier);
    supplierForm.setFieldsValue({
      name: supplier?.name || '',
      markup: supplier?.markup_percent || 0,
      priority: Number.isFinite(supplier?.priority) ? supplier.priority : 100,
      active: supplier?.is_active ?? true,
      min_profit_enabled: supplier?.min_profit_enabled ?? true,
      min_profit_amount: Number.isFinite(Number(supplier?.min_profit_amount))
        ? Number(supplier.min_profit_amount)
        : 500
    });
    setSupplierSettingsOpen(true);
    await refreshSupplierDetails(supplier);
  };

  const saveSupplier = async (values) => {
    if (!selectedSupplier) {
      return;
    }
    try {
      const minProfitAmountValue = Number(values.min_profit_amount);
      const minProfitAmount = values.min_profit_enabled
        ? Math.max(500, Number.isFinite(minProfitAmountValue) ? minProfitAmountValue : 500)
        : 0;
      const updated = await apiFetch(`/suppliers/${selectedSupplier.id}`, {
        method: 'PUT',
        body: JSON.stringify({
          name: values.name,
          markup_percent: values.markup || 0,
          priority: Number.isFinite(values.priority) ? values.priority : 100,
          is_active: values.active,
          min_profit_enabled: values.min_profit_enabled,
          min_profit_amount: minProfitAmount
        })
      });
      setSelectedSupplier(updated);
      supplierForm.setFieldsValue({
        name: updated?.name || '',
        markup: updated?.markup_percent || 0,
        priority: Number.isFinite(updated?.priority) ? updated.priority : 100,
        active: updated?.is_active ?? true,
        min_profit_enabled: updated?.min_profit_enabled ?? true,
        min_profit_amount: Number.isFinite(Number(updated?.min_profit_amount))
          ? Number(updated.min_profit_amount)
          : 500
      });
      await refreshSuppliers();
      await refreshSupplierDetails(selectedSupplier);
    } catch (err) {
      showError(err);
    }
  };

  const createSupplier = async (values) => {
    try {
      const minProfitAmountValue = Number(values.min_profit_amount);
      const minProfitAmount = values.min_profit_enabled
        ? Math.max(500, Number.isFinite(minProfitAmountValue) ? minProfitAmountValue : 500)
        : 0;
      const created = await apiFetch('/suppliers', {
        method: 'POST',
        body: JSON.stringify({
          name: values.name,
          markup_percent: values.markup || 0,
          priority: Number.isFinite(values.priority) ? values.priority : 100,
          min_profit_enabled: values.min_profit_enabled ?? true,
          min_profit_amount: minProfitAmount
        })
      });
      setSupplierCreateOpen(false);
      supplierCreateForm.resetFields();
      await refreshSuppliers();
      await openSupplierSettings(created);
    } catch (err) {
      if (err.message.includes('supplier name already exists')) {
        message.error('Постачальник з такою назвою вже існує');
      } else {
        showError(err);
      }
    }
  };

  const toggleSupplier = async (supplier) => {
    try {
      const updated = await apiFetch(`/suppliers/${supplier.id}`, {
        method: 'PUT',
        body: JSON.stringify({ is_active: !supplier.is_active })
      });
      if (selectedSupplier && updated.id === selectedSupplier.id) {
        setSelectedSupplier(updated);
        supplierForm.setFieldsValue({
          name: updated?.name || '',
          markup: updated?.markup_percent || 0,
          priority: Number.isFinite(updated?.priority) ? updated.priority : 100,
          active: updated?.is_active ?? true,
          min_profit_enabled: updated?.min_profit_enabled ?? true,
          min_profit_amount: Number.isFinite(Number(updated?.min_profit_amount))
            ? Number(updated.min_profit_amount)
            : 500
        });
      }
      await refreshSuppliers();
    } catch (err) {
      showError(err);
    }
  };

  const deleteSupplier = async (supplier) => {
    try {
      await apiFetch(`/suppliers/${supplier.id}`, { method: 'DELETE' });
      await refreshSuppliers();
      if (selectedSupplier && supplier.id === selectedSupplier.id) {
        closeSupplierSettings();
      }
    } catch (err) {
      showError(err);
    }
  };

  const openSourceModal = (source) => {
    setSourceEditing(source || null);
    sourceForm.setFieldsValue({
      name: source?.name || '',
      type: source?.source_type || 'google_sheet',
      url: source?.source_url || ''
    });
    setSourceModalOpen(true);
  };

  const saveSource = async (values) => {
    if (!selectedSupplier) return;
    try {
      if (sourceEditing) {
        await apiFetch(`/sources/${sourceEditing.id}`, {
          method: 'PUT',
          body: JSON.stringify({
            name: values.name,
            source_type: values.type,
            source_url: values.url
          })
        });
      } else {
        await apiFetch('/sources', {
          method: 'POST',
          body: JSON.stringify({
            supplier_id: selectedSupplier.id,
            source_type: values.type,
            source_url: values.url,
            name: values.name
          })
        });
      }
      setSourceModalOpen(false);
      setSourceEditing(null);
      sourceForm.resetFields();
      await refreshSupplierDetails(selectedSupplier);
    } catch (err) {
      if (err.message.includes('source already exists')) {
        message.error('Джерело з таким посиланням вже існує');
      } else {
        showError(err);
      }
    }
  };

  const toggleSource = async (source) => {
    try {
      await apiFetch(`/sources/${source.id}`, {
        method: 'PUT',
        body: JSON.stringify({ is_active: !source.is_active })
      });
      await refreshSupplierDetails(selectedSupplier);
    } catch (err) {
      showError(err);
    }
  };

  const deleteSource = async (source) => {
    try {
      await apiFetch(`/sources/${source.id}`, { method: 'DELETE' });
      await refreshSupplierDetails(selectedSupplier);
    } catch (err) {
      showError(err);
    }
  };

  const saveMapping = async () => {
    if (!selectedSupplier) return;
    if (!mappingSheetName) {
      message.error('Оберіть аркуш для мапінгу');
      return;
    }
    const requiredMissing = ['article', 'quantity', 'price'].filter(
      (field) => !isMappingFieldSet(mappingFields[field], { allowEmpty: false })
    );
    if (requiredMissing.length) {
      message.error('Заповніть мапінг: артикул, кількість, ціна');
      return;
    }
    const mapping = {};
    const headers = {};
    Object.entries(mappingFields).forEach(([field, entry]) => {
      if (!entry) {
        return;
      }
      if (entry.mode === 'static') {
        if (!isMappingFieldSet(entry)) {
          return;
        }
        mapping[field] = { type: 'static', value: entry.value };
        return;
      }
      const index = Number(entry.value);
      if (!Number.isFinite(index)) {
        return;
      }
      mapping[field] = index;
      headers[field] = mappingHeaders[index - 1] || '';
    });
    try {
      if (mappingSourceId && mappingSheetName) {
        const source = sources.find((item) => item.id === mappingSourceId);
        if (source && source.sheet_name !== mappingSheetName) {
          await apiFetch(`/sources/${mappingSourceId}`, {
            method: 'PUT',
            body: JSON.stringify({ sheet_name: mappingSheetName })
          });
        }
      }
      const headerValue = Number.isFinite(Number(headerRow)) ? headerRow : 1;
      await apiFetch(`/mappings/${selectedSupplier.id}`, {
        method: 'POST',
        body: JSON.stringify({
          source_id: mappingSourceId,
          mapping,
          header_row: headerValue,
          mapping_meta: {
            source_id: mappingSourceId,
            sheet_name: mappingSheetName,
            header_row: headerValue,
            headers
          }
        })
      });
      await refreshSupplierDetails(selectedSupplier);
      message.success('Мапінг збережено');
    } catch (err) {
      showError(err);
    }
  };

  const runImportSupplier = async (supplier) => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/import-supplier', { supplierId: supplier.id });
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runImportSource = async (source) => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/import-source', { sourceId: source.id });
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runFinalize = async () => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/finalize');
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runExport = async () => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/export', {
        supplier: manualHoroshopSupplier || null
      });
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
      await refreshHoroshopApiPreview();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runImportAll = async () => {
    try {
      setActionInProgress(true);
      setActionResult('Імпорт усіх: запуск...');
      const result = await jobFetch('/jobs/import-all');
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
      await refreshMergedPreview(1, mergedPageSize, mergedSearch, mergedSort);
    } catch (err) {
      setActionResult(`Помилка імпорту усіх: ${formatErrorMessage(err)}`);
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runHoroshopSync = async () => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/horoshop-sync');
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
      await refreshHoroshopSuppliers();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const runHoroshopImport = async () => {
    try {
      setActionInProgress(true);
      const result = await jobFetch('/jobs/horoshop-import');
      setActionResult(JSON.stringify(result, null, 2));
      await refreshDashboard();
    } catch (err) {
      showError(err);
    } finally {
      setActionInProgress(false);
    }
  };

  const cancelJob = async (jobId) => {
    try {
      const result = await jobFetch(`/jobs/${jobId}/cancel`, { reason: 'Manual cancel' });
      message.success(`Job #${result.jobId} скасовано`);
      await refreshDashboard(logsErrorOnly);
    } catch (err) {
      showError(err);
    }
  };

  const loadSourceSheets = async () => {
    if (!mappingSourceId) {
      message.error('Оберіть джерело');
      return;
    }
    setMappingSheetsLoading(true);
    try {
      const result = await apiFetch(`/source-sheets?sourceId=${mappingSourceId}`);
      setMappingSheets(result.sheets || []);
      setMappingSheetName(result.selectedSheetName || null);
    } catch (err) {
      showError(err);
    } finally {
      setMappingSheetsLoading(false);
    }
  };

  const loadSourcePreview = async () => {
    if (!mappingSourceId) {
      message.error('Оберіть джерело');
      return;
    }
    if (!mappingSheetName) {
      message.error('Оберіть аркуш');
      return;
    }
    setMappingLoading(true);
    try {
      const headerParam = Number.isFinite(Number(headerRow)) ? headerRow : 1;
      const result = await apiFetch(
        `/source-preview?sourceId=${mappingSourceId}&headerRow=${headerParam}&sheetName=${encodeURIComponent(
          mappingSheetName
        )}`
      );
      if (result.sheetName && result.sheetName !== mappingSheetName) {
        setMappingSheetName(result.sheetName);
      }
      setMappingHeaders(result.headers || []);
      setMappingSampleRows(result.sampleRows || []);
    } catch (err) {
      showError(err);
    } finally {
      setMappingLoading(false);
    }
  };

  const mappingOptions = useMemo(
    () =>
      mappingHeaders.map((header, idx) => ({
        label: `${columnLetter(idx + 1)} — ${header || 'Колонка'}`,
        value: idx + 1
      })),
    [mappingHeaders]
  );

  const mappingFieldDefs = useMemo(
    () => [
      { key: 'article', label: 'Артикул', required: true },
      { key: 'quantity', label: 'Кількість', required: true },
      { key: 'price', label: 'Ціна', required: true },
      { key: 'size', label: 'Розмір', required: false },
      { key: 'extra', label: 'Назва / опис', required: false }
    ],
    []
  );

  const getLogErrorSummary = (log) => {
    if (!log || log.level !== 'error') {
      return '';
    }
    const data = log.data || {};
    const supplierLabel = data.supplierName || data.supplier_name || data.supplier;
    const sourceLabel = data.sourceName || data.source_name || data.source;
    const sheetLabel = data.sheetName || data.sheet_name;
    if (typeof data === 'string') {
      return data;
    }
    let baseMessage = '';
    if (data.error) {
      baseMessage = data.error;
    } else if (Array.isArray(data.errors)) {
      baseMessage = data.errors.join('; ');
    } else {
      const fallback = JSON.stringify(data);
      baseMessage = fallback === '{}' ? '' : fallback;
    }
    if (!baseMessage) {
      return '';
    }
    const context = [];
    if (supplierLabel) {
      context.push(`Постачальник: ${supplierLabel}`);
    }
    if (sourceLabel) {
      context.push(`Джерело: ${sourceLabel}`);
    }
    if (sheetLabel) {
      context.push(`Аркуш: ${sheetLabel}`);
    }
    return context.length ? `${baseMessage} (${context.join(', ')})` : baseMessage;
  };

  const previewColumns = useMemo(
    () =>
      mappingHeaders.map((header, idx) => ({
        title: `${columnLetter(idx + 1)} ${header || ''}`,
        dataIndex: `c${idx}`,
        key: `c${idx}`,
        width: 160
      })),
    [mappingHeaders]
  );

  const previewRows = useMemo(
    () =>
      mappingSampleRows.map((row, rowIndex) => {
        const item = { key: rowIndex };
        mappingHeaders.forEach((_, idx) => {
          item[`c${idx}`] = row[idx] ?? '';
        });
        return item;
      }),
    [mappingSampleRows, mappingHeaders]
  );

  const saveCronSettings = async () => {
    setCronSaving(true);
    try {
      await apiFetch('/cron-settings', {
        method: 'PUT',
        body: JSON.stringify({ settings: cronSettings })
      });
      await refreshCronSettings();
      message.success('Cron settings saved');
    } catch (err) {
      showError(err);
    } finally {
      setCronSaving(false);
    }
  };

  const updateCronUi = (name, updates) => {
    setCronUi((prev) => {
      const next = { ...(prev[name] || { mode: 'daily', hour: 3, minute: 0 }) };
      const merged = { ...next, ...updates };
      setCronSettings((current) =>
        current.map((setting) => {
          if (setting.name !== name) return setting;
          return { ...setting, cron: scheduleToCron(merged) };
        })
      );
      return { ...prev, [name]: merged };
    });
  };

  const updateCronMeta = (name, metaUpdates) => {
    setCronSettings((current) =>
      current.map((setting) => {
        if (setting.name !== name) return setting;
        return { ...setting, meta: { ...(setting.meta || {}), ...metaUpdates } };
      })
    );
  };

  const cronDescription = useMemo(
    () => ({
      update_pipeline: 'Планування оновлення (імпорт → finalize → export → Horoshop)',
      horoshop_sync: 'Регламентний sync Horoshop (дзеркало)',
      cleanup: 'Очищення історії (10 днів)'
    }),
    []
  );

  const supplierColumns = useMemo(() => {
    const base = [
      { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
      { title: 'Назва', dataIndex: 'name', key: 'name' },
      {
        title: 'Націнка %',
        dataIndex: 'markup_percent',
        key: 'markup_percent',
        width: 120
      },
      {
        title: 'Мін. націнка',
        dataIndex: 'min_profit_enabled',
        key: 'min_profit_enabled',
        width: 140,
        render: (value) => (value ? <Tag color="blue">увімкнено</Tag> : <Tag>вимкнено</Tag>)
      },
      {
        title: 'Сума',
        dataIndex: 'min_profit_amount',
        key: 'min_profit_amount',
        width: 120,
        render: (value) => (value === null || typeof value === 'undefined' ? '-' : value)
      },
      { title: 'Пріоритет', dataIndex: 'priority', key: 'priority', width: 120 },
      {
        title: 'Статус',
        dataIndex: 'is_active',
        key: 'is_active',
        width: 120,
        render: (value) => (value ? <Tag color="green">активний</Tag> : <Tag>вимкнено</Tag>)
      }
    ];
    if (isReadOnly) {
      return base;
    }
    return [
      ...base,
      {
        title: 'Дії',
        key: 'actions',
        width: 160,
        render: (_, record) => (
          <Space>
            <Button size="small" type="primary" onClick={() => openSupplierSettings(record)}>
              Налаштування
            </Button>
          </Space>
        )
      }
    ];
  }, [isReadOnly, openSupplierSettings]);

  const sourceColumns = useMemo(() => {
    const base = [
      { title: 'Назва', dataIndex: 'name', key: 'name', width: 200 },
      {
        title: 'URL',
        dataIndex: 'source_url',
        key: 'source_url',
        render: (value) => (
          <span className="truncate" title={value}>
            {value}
          </span>
        )
      },
      {
        title: 'Статус',
        dataIndex: 'is_active',
        key: 'is_active',
        width: 120,
        render: (value) => (value ? <Tag color="green">активний</Tag> : <Tag>вимкнено</Tag>)
      }
    ];
    if (isReadOnly) {
      return base;
    }
    return [
      ...base,
      {
        title: 'Дії',
        key: 'actions',
        width: 220,
        render: (_, record) => (
          <Space>
            <Button size="small" onClick={() => openSourceModal(record)}>
              Редагувати
            </Button>
            <Button size="small" onClick={() => toggleSource(record)}>
              {record.is_active ? 'Вимкнути' : 'Увімкнути'}
            </Button>
            <Popconfirm
              title="Видалити джерело?"
              okText="Видалити"
              okType="danger"
              onConfirm={() => deleteSource(record)}
            >
              <Button size="small" danger>
                Видалити
              </Button>
            </Popconfirm>
          </Space>
        )
      }
    ];
  }, [isReadOnly, openSourceModal, toggleSource, deleteSource]);

  const mergedColumns = [
    { title: 'Артикул', dataIndex: 'article', key: 'article', width: 160 },
    { title: 'Назва', dataIndex: 'extra', key: 'extra', width: 240 },
    { title: 'Розмір', dataIndex: 'size', key: 'size', width: 100 },
    { title: 'Кількість', dataIndex: 'quantity', key: 'quantity', width: 120 },
    { title: 'Ціна', dataIndex: 'price', key: 'price', width: 120 },
    { title: 'Постачальник', dataIndex: 'supplier_name', key: 'supplier_name', width: 180 },
    {
      title: 'Створено',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 200,
      render: (value) => formatDateTime(value)
    }
  ];

  const finalColumns = useMemo(() => {
    const base = [
      { title: 'Артикул', dataIndex: 'article', key: 'article', width: 160 },
      { title: 'Назва', dataIndex: 'extra', key: 'extra', width: 240 },
      { title: 'Розмір', dataIndex: 'size', key: 'size', width: 100 },
      { title: 'Кількість', dataIndex: 'quantity', key: 'quantity', width: 120 },
      { title: 'Ціна базова', dataIndex: 'price_base', key: 'price_base', width: 140 },
      { title: 'Ціна фінальна', dataIndex: 'price_final', key: 'price_final', width: 140 },
      {
        title: 'Ручна',
        key: 'override',
        width: 120,
        render: (_, record) =>
          record.override_id ? <Tag color="gold">ручна</Tag> : <Text className="muted">-</Text>
      },
      { title: 'Постачальник', dataIndex: 'supplier_name', key: 'supplier_name', width: 180 },
      {
        title: 'Створено',
        dataIndex: 'created_at',
        key: 'created_at',
        width: 200,
        render: (value) => formatDateTime(value)
      }
    ];
    if (isReadOnly) {
      return base;
    }
    return [
      ...base,
      {
        title: 'Дії',
        key: 'actions',
        width: 140,
        render: (_, record) => (
          <Button size="small" onClick={() => openPriceOverride(record)}>
            {record.override_id ? 'Змінити' : 'Ручна ціна'}
          </Button>
        )
      }
    ];
  }, [isReadOnly, openPriceOverride]);

  const horoshopColumns = [
    { title: 'Артикул', dataIndex: 'article', key: 'article', width: 160 },
    { title: 'Постачальник', dataIndex: 'supplier', key: 'supplier', width: 180 },
    { title: 'Наявність', dataIndex: 'presence_ua', key: 'presence_ua', width: 160 },
    {
      title: 'Вітрина',
      dataIndex: 'display_in_showcase',
      key: 'display_in_showcase',
      width: 120,
      render: (value) => (value ? <Tag color="green">так</Tag> : <Tag>ні</Tag>)
    },
    { title: 'Parent', dataIndex: 'parent_article', key: 'parent_article', width: 160 },
    { title: 'Ціна', dataIndex: 'price', key: 'price', width: 120 },
    {
      title: 'Синхронізовано',
      dataIndex: 'synced_at',
      key: 'synced_at',
      width: 200,
      render: (value) => formatDateTime(value)
    }
  ];

  const horoshopApiColumns = [
    { title: 'Артикул', dataIndex: 'article', key: 'article', width: 160 },
    { title: 'Постачальник', dataIndex: 'supplier', key: 'supplier', width: 160 },
    { title: 'Наявність', dataIndex: 'presence_ua', key: 'presence_ua', width: 160 },
    {
      title: 'Вітрина',
      dataIndex: 'display_in_showcase',
      key: 'display_in_showcase',
      width: 120,
      render: (value) => (value ? <Tag color="green">так</Tag> : <Tag>ні</Tag>)
    },
    { title: 'Parent', dataIndex: 'parent_article', key: 'parent_article', width: 160 },
    { title: 'Ціна', dataIndex: 'price', key: 'price', width: 120 }
  ];

  const compareColumns = [
    { title: 'Артикул', dataIndex: 'article', key: 'article', width: 160 },
    { title: 'Розмір', dataIndex: 'size', key: 'size', width: 100 },
    { title: 'Кількість', dataIndex: 'quantity', key: 'quantity', width: 120 },
    { title: 'Ціна вхідна', dataIndex: 'price_base', key: 'price_base', width: 140 },
    { title: 'Ціна продажна', dataIndex: 'price_final', key: 'price_final', width: 140 },
    { title: 'Назва', dataIndex: 'extra', key: 'extra', width: 240 },
    { title: 'Постачальник', dataIndex: 'supplier_name', key: 'supplier_name', width: 180 },
    { title: 'SKU', dataIndex: 'sku_article', key: 'sku_article', width: 180 },
    {
      title: 'Є артикул',
      dataIndex: 'horoshop_article',
      key: 'horoshop_article',
      width: 140,
      render: (value) => (value ? <Tag color="green">так</Tag> : <Tag>ні</Tag>)
    },
    {
      title: 'Є SKU',
      dataIndex: 'horoshop_sku',
      key: 'horoshop_sku',
      width: 120,
      render: (value) => (value ? <Tag color="green">так</Tag> : <Tag>ні</Tag>)
    },
    { title: 'Наявність (SKU)', dataIndex: 'horoshop_presence', key: 'horoshop_presence', width: 160 },
    {
      title: 'Вітрина (SKU)',
      dataIndex: 'horoshop_display',
      key: 'horoshop_display',
      width: 140,
      render: (value) => {
        if (value === null || typeof value === 'undefined') {
          return <Text className="muted">-</Text>;
        }
        return value ? <Tag color="green">так</Tag> : <Tag>ні</Tag>;
      }
    }
  ];

  const logColumns = [
    {
      title: 'Час',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 200,
      render: (value) => formatDateTime(value)
    },
    {
      title: 'Рівень',
      dataIndex: 'level',
      key: 'level',
      width: 120,
      render: (value) => {
        if (value === 'error') return <Tag color="red">error</Tag>;
        if (value === 'info') return <Tag color="blue">info</Tag>;
        return <Tag>log</Tag>;
      }
    },
    { title: 'Повідомлення', dataIndex: 'message', key: 'message' },
    {
      title: 'Помилка',
      key: 'error',
      width: 260,
      render: (_, record) => {
        const summary = getLogErrorSummary(record);
        if (!summary) {
          return <Text className="muted">-</Text>;
        }
        const short = summary.length > 120 ? `${summary.slice(0, 120)}…` : summary;
        return (
          <span className="error-text" title={summary}>
            {short}
          </span>
        );
      }
    },
    {
      title: 'Деталі',
      key: 'details',
      width: 120,
      render: (_, record) => (
        <Button
          size="small"
          onClick={() => {
            setSelectedLog(record);
            setLogModalOpen(true);
          }}
        >
          Деталі
        </Button>
      )
    }
  ];

  const jobColumns = useMemo(() => {
    const base = [
      { title: 'ID', dataIndex: 'id', key: 'id', width: 80 },
      { title: 'Тип', dataIndex: 'type', key: 'type', width: 160 },
      {
        title: 'Статус',
        dataIndex: 'status',
        key: 'status',
        width: 140,
        render: (value) => {
          if (value === 'success') return <Tag color="green">success</Tag>;
          if (value === 'failed') return <Tag color="red">failed</Tag>;
          if (value === 'running') return <Tag color="blue">running</Tag>;
          if (value === 'queued') return <Tag>queued</Tag>;
          if (value === 'canceled') return <Tag color="orange">canceled</Tag>;
          return <Tag>{value}</Tag>;
        }
      },
      {
        title: 'Старт',
        dataIndex: 'started_at',
        key: 'started_at',
        width: 200,
        render: (value) => formatDateTime(value)
      },
      {
        title: 'Фініш',
        dataIndex: 'finished_at',
        key: 'finished_at',
        width: 200,
        render: (value) => formatDateTime(value)
      }
    ];
    if (isReadOnly) {
      return base;
    }
    return [
      ...base,
      {
        title: 'Дії',
        key: 'actions',
        width: 140,
        render: (_, record) => (
          <Popconfirm
            title={`Скасувати job #${record.id}?`}
            okText="Скасувати"
            okType="danger"
            onConfirm={() => cancelJob(record.id)}
            disabled={!['running', 'queued'].includes(record.status)}
          >
            <Button size="small" danger disabled={!['running', 'queued'].includes(record.status)}>
              Скасувати
            </Button>
          </Popconfirm>
        )
      }
    ];
  }, [isReadOnly, cancelJob]);

  const updateSchedule = cronSettings.find((item) => item.name === 'update_pipeline') || null;
  const scheduleUi = cronUi.update_pipeline || parseCronToSchedule(updateSchedule?.cron || '');
  const scheduleEnabled = updateSchedule?.is_enabled ?? false;
  const scheduleSupplier = updateSchedule?.meta?.supplier || 'drop';
  const activeJob =
    jobs.find((job) => job.status === 'running') || jobs.find((job) => job.status === 'queued');

  const dashboardTab = (
    <div className="grid">
      <Card className="card">
        <Title level={4}>Статус системи</Title>
        <Space direction="vertical" size="small">
          <Text>
            API: <Tag color={apiStatus === 'ok' ? 'green' : 'red'}>{apiStatus}</Tag>
          </Text>
          <Text>Остання дія: {actionResult ? 'оновлено' : '-'}</Text>
          <Text>
            Активний процес:{' '}
            {activeJob ? (
              <>
                <Tag color={activeJob.status === 'running' ? 'blue' : 'orange'}>
                  {activeJob.status}
                </Tag>
                #{activeJob.id} ({activeJob.type})
              </>
            ) : (
              <Text className="muted">немає</Text>
            )}
          </Text>
          {stats?.lastJob && (
            <Text>
              Останній джоб: #{stats.lastJob.id} ({stats.lastJob.type})
            </Text>
          )}
          <Text>
            Планове оновлення:{' '}
            <Tag color={scheduleEnabled ? 'green' : 'red'}>
              {scheduleEnabled ? 'увімкнено' : 'вимкнено'}
            </Tag>
          </Text>
          <Text>Графік: {formatSchedule(scheduleUi)}</Text>
          <Text>Постачальник для Horoshop: {scheduleSupplier}</Text>
          <Text>
            Останній плановий запуск:{' '}
            {stats?.lastUpdatePipeline?.finished_at
              ? formatDateTime(stats.lastUpdatePipeline.finished_at)
              : stats?.lastUpdatePipeline?.started_at
                ? formatDateTime(stats.lastUpdatePipeline.started_at)
                : '-'}
          </Text>
          <Text>
            Статус останнього планового запуску:{' '}
            {stats?.lastUpdatePipeline?.status ? (
              <Tag
                color={
                  stats.lastUpdatePipeline.status === 'success'
                    ? 'green'
                    : stats.lastUpdatePipeline.status === 'failed'
                      ? 'red'
                      : 'blue'
                }
              >
                {stats.lastUpdatePipeline.status}
              </Tag>
            ) : (
              <Text className="muted">немає</Text>
            )}
          </Text>
          {stats?.lastHoroshopSync && (
            <div>
              <Text>
                Horoshop sync:{' '}
                <Tag
                  color={
                    stats.lastHoroshopSync.status === 'success'
                      ? 'green'
                      : stats.lastHoroshopSync.status === 'failed'
                        ? 'red'
                        : 'blue'
                  }
                >
                  {stats.lastHoroshopSync.status}
                </Tag>
              </Text>
              {stats.lastHoroshopSync.status === 'running' &&
                stats.lastHoroshopSync.estimate?.processed !== null && (
                  <Text>
                    Прогрес: {stats.lastHoroshopSync.estimate.processed}
                    {stats.lastHoroshopSync.estimate.expected_total
                      ? ` / ${stats.lastHoroshopSync.estimate.expected_total}`
                      : ''}
                  </Text>
                )}
              {stats.lastHoroshopSync.status === 'running' &&
                stats.lastHoroshopSync.estimate?.eta_ms !== null && (
                  <Text>
                    Орієнтовний час до завершення:{' '}
                    {formatDuration(stats.lastHoroshopSync.estimate.eta_ms)}
                  </Text>
                )}
              {stats.lastHoroshopSync.status === 'success' &&
                stats.lastHoroshopSync.total !== null && (
                <Text>
                  Оновлено товарів: {stats.lastHoroshopSync.total}
                  {typeof stats.lastHoroshopSync.deleted === 'number'
                    ? `, видалено: ${stats.lastHoroshopSync.deleted}`
                    : ''}
                </Text>
                )}
              {stats.lastHoroshopSync.status === 'success' &&
                stats.lastHoroshopSync.duration_ms !== null && (
                  <Text>
                    Час синхронізації: {formatDuration(stats.lastHoroshopSync.duration_ms)}
                  </Text>
                )}
              {stats.lastHoroshopSync.estimate?.avg_duration_ms !== null && (
                <Text>
                  Орієнтовний час повної синхронізації (середнє):{' '}
                  {formatDuration(stats.lastHoroshopSync.estimate.avg_duration_ms)}
                </Text>
              )}
              {stats.lastHoroshopSync.error && (
                <Text className="error-text">Помилка: {stats.lastHoroshopSync.error}</Text>
              )}
            </div>
          )}
          {stats?.lastUpdatePipeline && (
            <div>
              <Text>
                Плановий процес:{' '}
                <Tag
                  color={
                    stats.lastUpdatePipeline.status === 'success'
                      ? 'green'
                      : stats.lastUpdatePipeline.status === 'failed'
                        ? 'red'
                        : 'blue'
                  }
                >
                  {stats.lastUpdatePipeline.status}
                </Tag>
              </Text>
              {stats.lastUpdatePipeline.message && (
                <Text>Останній крок: {stats.lastUpdatePipeline.message}</Text>
              )}
              {stats.lastUpdatePipeline.summary?.horoshopImport?.total !== undefined && (
                <Text>
                  Відправлено в Horoshop: {stats.lastUpdatePipeline.summary.horoshopImport.total}
                </Text>
              )}
              {stats.lastUpdatePipeline.summary?.export?.apiTotal !== undefined && (
                <Text>
                  API рядків: {stats.lastUpdatePipeline.summary.export.apiTotal}
                </Text>
              )}
              {stats.lastUpdatePipeline.duration_ms !== null && (
                <Text>
                  Час виконання: {formatDuration(stats.lastUpdatePipeline.duration_ms)}
                </Text>
              )}
              {stats.lastUpdatePipeline.error && (
                <Text className="error-text">Помилка: {stats.lastUpdatePipeline.error}</Text>
              )}
            </div>
          )}
        </Space>
      </Card>
      <Card className="card">
        <Title level={4}>Обсяги</Title>
        <List
          size="small"
          dataSource={[
            { label: 'Постачальники', value: stats?.suppliers ?? 0 },
            { label: 'Джерела', value: stats?.sources ?? 0 },
            { label: 'Сирі рядки', value: stats?.products_raw ?? 0 },
            { label: 'Фінальні рядки', value: stats?.products_final ?? 0 }
          ]}
          renderItem={(item) => (
            <List.Item>
              <Text>{item.label}</Text>
              <Text strong>{item.value}</Text>
            </List.Item>
          )}
        />
      </Card>
      <Card className="card">
        <Title level={4}>Остання дія</Title>
        <pre className="result-box">{actionResult}</pre>
      </Card>
    </div>
  );

  const suppliersTab = (
    <div className="tab-section">
      {!isReadOnly && (
        <Space className="tab-actions" wrap>
          <Button type="primary" onClick={openSupplierCreate}>
            Додати постачальника
          </Button>
          <Button onClick={openBulkMarkup} disabled={!selectedSupplierIds.length}>
            Масова націнка
          </Button>
          {selectedSupplierIds.length ? (
            <Tag>Вибрано: {selectedSupplierIds.length}</Tag>
          ) : null}
        </Space>
      )}
      <Table
        dataSource={suppliers}
        columns={supplierColumns}
        rowKey="id"
        size="small"
        pagination={{ pageSize: 20 }}
        rowSelection={
          isReadOnly
            ? undefined
            : {
                selectedRowKeys: selectedSupplierIds,
                onChange: (keys) => setSelectedSupplierIds(keys)
              }
        }
      />
    </div>
  );

  const mergedTab = (
    <div className="tab-section">
      <Text className="muted">
        Це проміжний змерджений набір (до фінальної обробки та дедуплікації).
      </Text>
      <Space className="tab-actions" wrap>
        <Input
          placeholder="Пошук по артикулу або назві"
          value={mergedSearch}
          onChange={(event) => {
            const value = event.target.value;
            setMergedSearch(value);
            if (!value) {
              refreshMergedPreview(1, mergedPageSize, '', mergedSort);
            }
          }}
          style={{ minWidth: 260 }}
          allowClear
          onPressEnter={() => refreshMergedPreview(1, mergedPageSize, mergedSearch, mergedSort)}
        />
        <Select
          value={mergedSort}
          onChange={(value) => {
            setMergedSort(value);
            refreshMergedPreview(1, mergedPageSize, mergedSearch, value);
          }}
          options={[
            { label: 'Артикул A → Z', value: 'article_asc' },
            { label: 'Артикул Z → A', value: 'article_desc' }
          ]}
          style={{ minWidth: 180 }}
        />
        <Button onClick={() => refreshMergedPreview()}>Оновити</Button>
        <Button
          onClick={() =>
            downloadExport('/merged-export', {
              search: mergedSearch,
              sort: mergedSort,
              jobId: mergedData.jobId
            })
          }
        >
          Експорт XLSX
        </Button>
        <Tag>
          Job ID: {mergedData.jobId ?? 'немає'}
        </Tag>
        <Tag>Всього рядків: {mergedData.total}</Tag>
      </Space>
      <Table
        dataSource={mergedData.rows}
        columns={mergedColumns}
        rowKey={(row, index) => `${row.article}-${index}`}
        size="small"
        pagination={{
          current: mergedPage,
          pageSize: mergedPageSize,
          total: mergedData.total,
          onChange: (page, pageSize) =>
            refreshMergedPreview(page, pageSize, mergedSearch, mergedSort)
        }}
        scroll={{ x: 900 }}
      />
    </div>
  );

  const finalizeTab = (
    <div className="tab-section">
      <Text className="muted">
        Це фінальні дані після націнки, округлення та дедуплікації — етап перед експортом.
      </Text>
      <Space className="tab-actions" wrap>
        <Input
          placeholder="Пошук по артикулу або назві"
          value={finalSearch}
          onChange={(event) => {
            const value = event.target.value;
            setFinalSearch(value);
            if (!value) {
              refreshFinalPreview(1, finalPageSize, '', finalSort);
            }
          }}
          style={{ minWidth: 260 }}
          allowClear
          onPressEnter={() =>
            refreshFinalPreview(1, finalPageSize, finalSearch, finalSort, finalSupplierId)
          }
        />
        <Select
          value={finalSort}
          onChange={(value) => {
            setFinalSort(value);
            refreshFinalPreview(1, finalPageSize, finalSearch, value, finalSupplierId);
          }}
          options={[
            { label: 'Артикул A → Z', value: 'article_asc' },
            { label: 'Артикул Z → A', value: 'article_desc' }
          ]}
          style={{ minWidth: 180 }}
        />
        <Select
          value={finalSupplierId}
          onChange={(value) => {
            setFinalSupplierId(value || null);
            refreshFinalPreview(1, finalPageSize, finalSearch, finalSort, value || null);
          }}
          options={[
            { label: 'Усі постачальники', value: null },
            ...suppliers.map((supplier) => ({
              label: supplier.name,
              value: supplier.id
            }))
          ]}
          style={{ minWidth: 200 }}
          placeholder="Постачальник"
        />
        {!isReadOnly && (
          <Button type="primary" onClick={() => openPriceOverride(null)}>
            Ручна ціна
          </Button>
        )}
        <Button
          onClick={() =>
            downloadExport('/final-export', {
              search: finalSearch,
              supplierId: finalSupplierId
            })
          }
        >
          Експорт XLSX
        </Button>
        <Button onClick={() => refreshFinalPreview()}>Оновити</Button>
        <Tag>Job ID: {finalData.jobId ?? 'немає'}</Tag>
        <Tag>Всього рядків: {finalData.total}</Tag>
      </Space>
      <Table
        dataSource={finalData.rows}
        columns={finalColumns}
        rowKey={(row, index) => `${row.article}-${index}`}
        size="small"
        pagination={{
          current: finalPage,
          pageSize: finalPageSize,
          total: finalData.total,
          onChange: (page, pageSize) => refreshFinalPreview(page, pageSize, finalSearch, finalSort)
        }}
        scroll={{ x: 1000 }}
      />
    </div>
  );

  const horoshopTab = (
    <div className="tab-section">
      <Text className="muted">Поточний стан каталогу Horoshop (дзеркало).</Text>
      <Space className="tab-actions" wrap>
        <Input
          placeholder="Пошук по артикулу або постачальнику"
          value={horoshopSearch}
          onChange={(event) => {
            const value = event.target.value;
            setHoroshopSearch(value);
            if (!value) {
              refreshHoroshopPreview(1, horoshopPageSize, '');
            }
          }}
          style={{ minWidth: 260 }}
          allowClear
          onPressEnter={() => refreshHoroshopPreview(1, horoshopPageSize, horoshopSearch)}
        />
        <Button onClick={() => refreshHoroshopPreview()}>Оновити</Button>
        <Button onClick={() => downloadExport('/horoshop-export', { search: horoshopSearch })}>
          Експорт XLSX
        </Button>
        <Tag>Всього: {horoshopData.total}</Tag>
      </Space>
      <Table
        dataSource={horoshopData.rows}
        columns={horoshopColumns}
        rowKey={(row, index) => `${row.article}-${index}`}
        size="small"
        pagination={{
          current: horoshopPage,
          pageSize: horoshopPageSize,
          total: horoshopData.total,
          onChange: (page, pageSize) => refreshHoroshopPreview(page, pageSize, horoshopSearch)
        }}
        scroll={{ x: 900 }}
      />
    </div>
  );

  const horoshopApiTab = (
    <div className="tab-section">
      <Text className="muted">Preview того, що піде в Horoshop API (тільки зміни).</Text>
      <Space className="tab-actions" wrap>
        <Input
          placeholder="Пошук по артикулу"
          value={horoshopApiSearch}
          onChange={(event) => {
            const value = event.target.value;
            setHoroshopApiSearch(value);
            if (!value) {
              refreshHoroshopApiPreview(1, horoshopApiPageSize, '', horoshopApiSupplier);
            }
          }}
          style={{ minWidth: 260 }}
          allowClear
          onPressEnter={() =>
            refreshHoroshopApiPreview(1, horoshopApiPageSize, horoshopApiSearch, horoshopApiSupplier)
          }
        />
        <Select
          value={horoshopApiSupplier}
          onChange={(value) => {
            setHoroshopApiSupplier(value);
            refreshHoroshopApiPreview(1, horoshopApiPageSize, horoshopApiSearch, value);
          }}
          options={[
            { label: 'Усі постачальники', value: '' },
            ...horoshopSuppliers.map((supplier) => ({
              label: supplier,
              value: supplier
            }))
          ]}
          style={{ minWidth: 200 }}
          placeholder="Постачальник"
        />
        <Button onClick={() => refreshHoroshopApiPreview()}>Оновити</Button>
        <Tag>Всього: {horoshopApiData.total}</Tag>
      </Space>
      <Table
        dataSource={horoshopApiData.rows}
        columns={horoshopApiColumns}
        rowKey={(row, index) => `${row.article}-${index}`}
        size="small"
        pagination={{
          current: horoshopApiPage,
          pageSize: horoshopApiPageSize,
          total: horoshopApiData.total,
          onChange: (page, pageSize) =>
            refreshHoroshopApiPreview(page, pageSize, horoshopApiSearch, horoshopApiSupplier)
        }}
        scroll={{ x: 900 }}
      />
    </div>
  );

  const compareTab = (
    <div className="tab-section">
      <Text className="muted">
        Порівняння фінального набору з Horoshop. Показує, чи є артикул/SKU на
        сайті.
      </Text>
      <Space className="tab-actions" wrap>
        <Input
          placeholder="Пошук по артикулу, SKU або назві"
          value={compareSearch}
          onChange={(event) => {
            const value = event.target.value;
            setCompareSearch(value);
            if (!value) {
              refreshComparePreview(1, comparePageSize, '', compareSupplierId, compareMissingOnly);
            }
          }}
          style={{ minWidth: 260 }}
          allowClear
          onPressEnter={() =>
            refreshComparePreview(
              1,
              comparePageSize,
              compareSearch,
              compareSupplierId,
              compareMissingOnly
            )
          }
        />
        <Select
          value={compareSupplierId}
          onChange={(value) => {
            setCompareSupplierId(value || null);
            refreshComparePreview(1, comparePageSize, compareSearch, value || null, compareMissingOnly);
          }}
          options={[
            { label: 'Усі постачальники', value: null },
            ...suppliers.map((supplier) => ({
              label: supplier.name,
              value: supplier.id
            }))
          ]}
          style={{ minWidth: 200 }}
          placeholder="Постачальник"
        />
        <Checkbox
          checked={compareMissingOnly}
          onChange={(event) => {
            const checked = event.target.checked;
            setCompareMissingOnly(checked);
            refreshComparePreview(1, comparePageSize, compareSearch, compareSupplierId, checked);
          }}
        >
          Показати тільки відсутні SKU
        </Checkbox>
        <Button onClick={() => refreshComparePreview()}>Оновити</Button>
        <Button
          onClick={() =>
            downloadExport('/compare-export', {
              search: compareSearch,
              supplierId: compareSupplierId,
              missingOnly: compareMissingOnly
            })
          }
        >
          Експорт XLSX
        </Button>
        <Tag>Всього: {compareData.total}</Tag>
      </Space>
      <Table
        dataSource={compareData.rows}
        columns={compareColumns}
        rowKey={(row, index) => `${row.article}-${row.size}-${index}`}
        size="small"
        pagination={{
          current: comparePage,
          pageSize: comparePageSize,
          total: compareData.total,
          onChange: (page, pageSize) =>
            refreshComparePreview(page, pageSize, compareSearch, compareSupplierId, compareMissingOnly)
        }}
        scroll={{ x: 1400 }}
      />
    </div>
  );

  const cronTab = (
    <div className="tab-section">
      <Card className="card">
        <Title level={4}>Планування оновлення</Title>
        {cronSettings.length ? (
          cronSettings.map((item) => (
            <div key={item.name} className="cron-item">
              <Text strong>{cronDescription[item.name] || item.name}</Text>
              <div className="cron-details">
                <Row gutter={[12, 12]}>
                  <Col xs={24} md={12}>
                    <Text className="muted">Періодичність</Text>
                    <Select
                      className="cron-field"
                      value={cronUi[item.name]?.mode || 'daily'}
                      onChange={(value) => updateCronUi(item.name, { mode: value })}
                      options={[
                        { label: 'Щодня', value: 'daily' },
                        { label: 'Щотижня', value: 'weekly' },
                        { label: 'Щомісяця', value: 'monthly' }
                      ]}
                    />
                  </Col>
                  <Col xs={24} md={12}>
                    <Text className="muted">Час запуску</Text>
                    <Space>
                      <InputNumber
                        min={0}
                        max={23}
                        value={cronUi[item.name]?.hour ?? 3}
                        onChange={(value) => updateCronUi(item.name, { hour: value })}
                      />
                      <Text className="muted">:</Text>
                      <InputNumber
                        min={0}
                        max={59}
                        value={cronUi[item.name]?.minute ?? 0}
                        onChange={(value) => updateCronUi(item.name, { minute: value })}
                      />
                    </Space>
                  </Col>
                  {cronUi[item.name]?.mode === 'weekly' && (
                    <Col span={24}>
                      <Text className="muted">Дні тижня</Text>
                      <Checkbox.Group
                        options={WEEKDAY_OPTIONS}
                        value={cronUi[item.name]?.daysOfWeek || []}
                        onChange={(value) =>
                          updateCronUi(item.name, {
                            daysOfWeek: value.length ? value : ['1']
                          })
                        }
                      />
                    </Col>
                  )}
                  {cronUi[item.name]?.mode === 'monthly' && (
                    <Col span={24}>
                      <Text className="muted">Дати місяця</Text>
                      <Checkbox.Group
                        options={MONTH_DAY_OPTIONS}
                        value={cronUi[item.name]?.daysOfMonth || []}
                        onChange={(value) =>
                          updateCronUi(item.name, {
                            daysOfMonth: value.length ? value : ['1']
                          })
                        }
                        className="cron-days"
                      />
                    </Col>
                  )}
                  {item.name === 'update_pipeline' && (
                    <Col span={24}>
                      <Text className="muted">Постачальник для оновлення Horoshop</Text>
                      <Select
                        value={item.meta?.supplier || 'drop'}
                        onChange={(value) => updateCronMeta(item.name, { supplier: value })}
                        options={[
                          { label: 'drop', value: 'drop' },
                          ...horoshopSuppliers
                            .filter((supplier) => supplier && supplier !== 'drop')
                            .map((supplier) => ({ label: supplier, value: supplier }))
                        ]}
                        style={{ minWidth: 220 }}
                      />
                    </Col>
                  )}
                </Row>
              </div>
              <div className="cron-toggle">
                <Switch
                  checked={item.is_enabled}
                  onChange={(checked) => {
                    setCronSettings((prev) =>
                      prev.map((setting) =>
                        setting.name === item.name ? { ...setting, is_enabled: checked } : setting
                      )
                    );
                  }}
                />
              </div>
            </div>
          ))
        ) : (
          <Text className="muted">Налаштування планування недоступні.</Text>
        )}
        <Divider />
        <Button type="primary" loading={cronSaving} onClick={saveCronSettings}>
          Зберегти планування
        </Button>
      </Card>
    </div>
  );

  return (
    <Layout className="admin-layout">
      <Header className="main-header">
        <div>
          <Text className="eyebrow">Horoshop Admin</Text>
          <Title level={3} className="header-title">
            Панель керування
          </Title>
        </div>
        <div className="status-card">
          <div className="status-row">
            <Text>API</Text>
            <Tag color={apiStatus === 'ok' ? 'green' : 'red'}>{apiStatus}</Tag>
          </div>
          <div className="status-row">
            <Text>Дії</Text>
            <Text>{actionResult ? 'оновлено' : '-'}</Text>
          </div>
        </div>
      </Header>
      <Content className="admin-content">
        <Tabs
          className="main-tabs"
          items={[
            { key: 'dashboard', label: 'Головна', children: dashboardTab },
            { key: 'suppliers', label: 'Постачальники', children: suppliersTab },
            { key: 'merged', label: 'Змерджений файл', children: mergedTab },
            { key: 'finalize', label: 'Finalize', children: finalizeTab },
            { key: 'compare', label: 'Порівняння', children: compareTab },
            { key: 'horoshop', label: 'Horoshop', children: horoshopTab },
            { key: 'horoshop-api', label: 'Preview API', children: horoshopApiTab },
            ...(!isReadOnly
              ? [
                  {
                    key: 'manual',
                    label: 'Ручне керування',
                    children: (
                      <div className="tab-section">
                        <Card className="card">
                          <Title level={4}>Панель керування</Title>
                          <Text className="muted">
                            Запускайте фіналізацію та експорт вручну у будь-який момент.
                          </Text>
                          <Space className="action-row" wrap>
                            <Select
                              value={manualHoroshopSupplier || undefined}
                              onChange={(value) => setManualHoroshopSupplier(value || '')}
                              options={horoshopSuppliers.map((supplier) => ({
                                label: supplier,
                                value: supplier
                              }))}
                              style={{ minWidth: 220 }}
                              placeholder="Постачальник з Horoshop"
                              allowClear
                            />
                            <Button onClick={runImportAll} disabled={isBusy}>
                              Імпорт усіх
                            </Button>
                            <Button onClick={runHoroshopSync} disabled={isBusy}>
                              Sync Horoshop
                            </Button>
                            <Button onClick={runFinalize} disabled={isBusy}>
                              Finalize
                            </Button>
                            <Button type="primary" onClick={runExport} disabled={isBusy}>
                              Export
                            </Button>
                            <Button type="primary" onClick={runHoroshopImport} disabled={isBusy}>
                              Відправити в Horoshop
                            </Button>
                          </Space>
                          <Divider />
                          <Title level={5}>Останній результат</Title>
                          <pre className="result-box">{actionResult}</pre>
                        </Card>
                      </div>
                    )
                  }
                ]
              : []),
            {
              key: 'logs',
              label: 'Джоби та логи',
              children: (
                <div className="grid">
                  <Card className="card">
                    <Title level={4}>Останні джоби</Title>
                    <Table
                      dataSource={jobs}
                      columns={jobColumns}
                      rowKey="id"
                      size="small"
                      pagination={{ pageSize: 10 }}
                    />
                  </Card>
                  <Card className="card">
                    <Title level={4}>Логи</Title>
                    <Space className="tab-actions" wrap>
                      <Checkbox
                        checked={logsErrorOnly}
                        onChange={(event) => setLogsErrorOnly(event.target.checked)}
                      >
                        Показати тільки помилки
                      </Checkbox>
                      <Button onClick={() => refreshDashboard(logsErrorOnly)}>Оновити</Button>
                    </Space>
                    <Table
                      dataSource={logs}
                      columns={logColumns}
                      rowKey="id"
                      size="small"
                      pagination={{ pageSize: 10 }}
                    />
                  </Card>
                </div>
              )
            },
            ...(!isReadOnly
              ? [{ key: 'cron', label: 'Планування оновлення', children: cronTab }]
              : [])
          ]}
        />
      </Content>

      <Modal
        title="Додати постачальника"
        open={supplierCreateOpen}
        centered
        onCancel={() => setSupplierCreateOpen(false)}
        onOk={() => supplierCreateForm.submit()}
      >
        <Form form={supplierCreateForm} layout="vertical" onFinish={createSupplier}>
          <Form.Item label="Назва" name="name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item label="Націнка %" name="markup">
            <InputNumber min={0} step={0.01} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item label="Мінімальна націнка">
            <Space align="center">
              <Form.Item name="min_profit_enabled" valuePropName="checked" noStyle>
                <Switch />
              </Form.Item>
              <Form.Item name="min_profit_amount" noStyle>
                <InputNumber
                  min={500}
                  step={1}
                  style={{ width: 140 }}
                  disabled={minProfitEnabledCreate === false}
                />
              </Form.Item>
              <Text className="muted">грн</Text>
            </Space>
          </Form.Item>
          <Form.Item label="Пріоритет" name="priority">
            <InputNumber min={1} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Масова націнка"
        open={bulkMarkupOpen}
        centered
        onCancel={() => setBulkMarkupOpen(false)}
        onOk={applyBulkMarkup}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Text className="muted">Обрано постачальників: {selectedSupplierIds.length}</Text>
          <InputNumber
            min={0}
            step={0.01}
            style={{ width: '100%' }}
            value={bulkMarkupValue}
            onChange={(value) => setBulkMarkupValue(value)}
            placeholder="Націнка %"
          />
        </Space>
      </Modal>

      <Modal
        title={selectedSupplier ? `Налаштування: ${selectedSupplier.name}` : 'Налаштування постачальника'}
        open={supplierSettingsOpen}
        centered
        width="min(96vw, 1100px)"
        footer={null}
        onCancel={closeSupplierSettings}
        className="settings-modal"
      >
        {selectedSupplier ? (
          <Tabs
            items={[
              {
                key: 'settings',
                label: 'Параметри',
                children: (
                  <div className="tab-section">
                    <Space className="settings-actions" wrap>
                      <Button
                        onClick={() => runImportSupplier(selectedSupplier)}
                        disabled={isBusy || isReadOnly}
                      >
                        Імпорт постачальника
                      </Button>
                      <Button onClick={() => toggleSupplier(selectedSupplier)} disabled={isReadOnly}>
                        {selectedSupplier.is_active ? 'Деактивувати' : 'Активувати'}
                      </Button>
                      <Popconfirm
                        title="Видалити постачальника?"
                        okText="Видалити"
                        okType="danger"
                        onConfirm={() => deleteSupplier(selectedSupplier)}
                        disabled={isReadOnly}
                      >
                        <Button danger disabled={isReadOnly}>
                          Видалити
                        </Button>
                      </Popconfirm>
                    </Space>
                    <Divider />
                    <Form form={supplierForm} layout="vertical" onFinish={saveSupplier}>
                      <Row gutter={[12, 12]}>
                        <Col xs={24} md={12}>
                          <Form.Item label="Назва" name="name" rules={[{ required: true }]}>
                            <Input />
                          </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                          <Form.Item label="Націнка %" name="markup">
                            <InputNumber min={0} step={0.01} style={{ width: '100%' }} />
                          </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                          <Form.Item label="Мінімальна націнка">
                            <Space align="center">
                              <Form.Item name="min_profit_enabled" valuePropName="checked" noStyle>
                                <Switch />
                              </Form.Item>
                              <Form.Item name="min_profit_amount" noStyle>
                                <InputNumber
                                  min={500}
                                  step={1}
                                  style={{ width: 140 }}
                                  disabled={minProfitEnabledEdit === false}
                                />
                              </Form.Item>
                              <Text className="muted">грн</Text>
                            </Space>
                          </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                          <Form.Item label="Пріоритет" name="priority">
                            <InputNumber min={1} style={{ width: '100%' }} />
                          </Form.Item>
                        </Col>
                        <Col xs={24} md={12}>
                          <Form.Item label="Активний" name="active" valuePropName="checked">
                            <Switch />
                          </Form.Item>
                        </Col>
                      </Row>
                      <Button type="primary" htmlType="submit" disabled={isReadOnly}>
                        Зберегти
                      </Button>
                    </Form>
                  </div>
                )
              },
              {
                key: 'sources',
                label: 'Джерела',
                children: (
                  <div className="tab-section">
                    <Space className="tab-actions">
                      <Button
                        type="primary"
                        onClick={() => openSourceModal(null)}
                        disabled={isReadOnly}
                      >
                        Додати джерело
                      </Button>
                    </Space>
                    <Table
                      dataSource={sources}
                      columns={sourceColumns}
                      rowKey="id"
                      size="small"
                      pagination={false}
                    />
                  </div>
                )
              },
              {
                key: 'mapping',
                label: 'Мапінг',
                children: (
                  <Card title="Мапінг колонок" className="card">
                    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                      <Text className="muted">
                        1) Завантажте список аркушів. 2) Оберіть аркуш і рядок заголовків.
                        3) Завантажте поля та зробіть мапінг. Якщо заголовків немає —
                        увімкніть режим «Без заголовків».
                      </Text>
                      <Row gutter={[12, 12]}>
                        <Col xs={24} md={12}>
                          <Text className="muted">Джерело для мапінгу</Text>
                          <Select
                            value={mappingSourceId}
                            onChange={(value) => setMappingSourceId(value)}
                            options={sources.map((source) => ({
                              label: source.name || `Source #${source.id}`,
                              value: source.id
                            }))}
                            style={{ width: '100%' }}
                            placeholder="Оберіть джерело"
                          />
                        </Col>
                        <Col xs={24} md={12}>
                          <Text className="muted">Аркуш</Text>
                          <Select
                            value={mappingSheetName}
                            onChange={(value) => setMappingSheetName(value)}
                            options={mappingSheets.map((sheet) => ({
                              label: sheet,
                              value: sheet
                            }))}
                            style={{ width: '100%' }}
                            placeholder="Завантажте аркуші"
                            loading={mappingSheetsLoading}
                          />
                        </Col>
                        <Col xs={24} md={12}>
                          <Text className="muted">Рядок заголовків</Text>
                          <div className="mapping-header-toggle">
                            <Checkbox
                              checked={headerRow === 0}
                              onChange={(event) => {
                                if (event.target.checked) {
                                  if (headerRow > 0) {
                                    setHeaderRowBackup(headerRow);
                                  }
                                  setHeaderRow(0);
                                } else {
                                  setHeaderRow(headerRowBackup || 1);
                                }
                              }}
                            >
                              Без заголовків (дані з 1-го рядка)
                            </Checkbox>
                          </div>
                          <InputNumber
                            min={1}
                            value={headerRow === 0 ? headerRowBackup || 1 : headerRow}
                            onChange={(value) => {
                              const nextValue = Number(value) || 1;
                              setHeaderRow(nextValue);
                              setHeaderRowBackup(nextValue);
                            }}
                            style={{ width: '100%' }}
                            disabled={headerRow === 0}
                          />
                        </Col>
                        <Col xs={24} md={12} className="mapping-actions">
                          <Space wrap>
                            <Button onClick={loadSourceSheets} loading={mappingSheetsLoading}>
                              Завантажити аркуші
                            </Button>
                            <Button onClick={loadSourcePreview} loading={mappingLoading}>
                              Завантажити поля
                            </Button>
                          </Space>
                        </Col>
                      </Row>

                      {!mappingHeaders.length && (
                        <Text className="muted">
                          Завантажте поля, щоб показати доступні колонки.
                        </Text>
                      )}
                      <div className="mapping-rows">
                        {mappingFieldDefs.map((field) => {
                          const entry = mappingFields[field.key] || {
                            mode: 'column',
                            value: null,
                            allowEmpty: false
                          };
                          return (
                            <div key={field.key} className="mapping-row">
                              <div className="mapping-label">
                                <Text strong>{field.label}</Text>
                                {field.required && <Tag color="red">обовʼязково</Tag>}
                              </div>
                                <Radio.Group
                                  size="small"
                                  value={entry.mode}
                                  onChange={(event) => {
                                    const mode = event.target.value;
                                    setMappingFields((prev) => {
                                      const current = prev[field.key] || {
                                        mode: 'column',
                                        value: null,
                                        allowEmpty: false
                                      };
                                      if (current.mode === mode) {
                                        return prev;
                                      }
                                      const nextValue = mode === 'static' ? '' : null;
                                      return {
                                        ...prev,
                                        [field.key]: { mode, value: nextValue, allowEmpty: false }
                                      };
                                    });
                                  }}
                                  disabled={entry.allowEmpty}
                                >
                                  <Radio.Button value="column">Колонка</Radio.Button>
                                  <Radio.Button value="static">Значення</Radio.Button>
                                </Radio.Group>
                              <Checkbox
                                checked={Boolean(entry.allowEmpty)}
                                disabled={field.required && field.key !== 'size'}
                                onChange={(event) => {
                                  const checked = event.target.checked;
                                  setMappingFields((prev) => {
                                    if (!checked) {
                                      const current = prev[field.key] || {
                                        mode: 'column',
                                        value: null,
                                        allowEmpty: false
                                      };
                                      if (current.mode === 'static' && current.value === '') {
                                        return {
                                          ...prev,
                                          [field.key]: { mode: 'column', value: null, allowEmpty: false }
                                        };
                                      }
                                      return {
                                        ...prev,
                                        [field.key]: { ...current, allowEmpty: false }
                                      };
                                    }
                                    return {
                                      ...prev,
                                      [field.key]: { mode: 'static', value: '', allowEmpty: true }
                                    };
                                  });
                                }}
                              >
                                Немає значення
                              </Checkbox>
                              {entry.mode === 'static' ? (
                                <Input
                                  placeholder="Вкажіть значення"
                                  value={entry.value}
                                  onChange={(event) =>
                                    setMappingFields((prev) => ({
                                      ...prev,
                                      [field.key]: {
                                        ...(prev[field.key] || { mode: 'static' }),
                                        mode: 'static',
                                        value: event.target.value,
                                        allowEmpty: false
                                      }
                                    }))
                                  }
                                  disabled={entry.allowEmpty}
                                />
                              ) : (
                                <Select
                                  placeholder="Оберіть колонку"
                                  options={mappingOptions}
                                  value={entry.value}
                                  onChange={(value) =>
                                    setMappingFields((prev) => ({
                                      ...prev,
                                      [field.key]: { mode: 'column', value, allowEmpty: false }
                                    }))
                                  }
                                  allowClear={!field.required}
                                  disabled={!mappingHeaders.length || entry.allowEmpty}
                                />
                              )}
                            </div>
                          );
                        })}
                      </div>

                      <Button
                        type="primary"
                        onClick={saveMapping}
                        disabled={!mappingHeaders.length || isReadOnly}
                      >
                        Зберегти мапінг
                      </Button>

                      {mappingHeaders.length ? (
                        <div>
                          <Text strong>Приклад рядків</Text>
                          <Table
                            dataSource={previewRows}
                            columns={previewColumns}
                            size="small"
                            pagination={false}
                            scroll={{ x: 'max-content' }}
                          />
                        </div>
                      ) : null}
                    </Space>
                  </Card>
                )
              }
            ]}
          />
        ) : (
          <Text>Оберіть постачальника у списку.</Text>
        )}
      </Modal>

      <Modal
        title={sourceEditing ? 'Редагувати джерело' : 'Додати джерело'}
        open={sourceModalOpen}
        centered
        onCancel={() => {
          setSourceModalOpen(false);
          setSourceEditing(null);
        }}
        onOk={() => sourceForm.submit()}
      >
        <Form form={sourceForm} layout="vertical" onFinish={saveSource}>
          <Form.Item label="Назва" name="name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item label="Тип" name="type" initialValue="google_sheet">
            <Input disabled />
          </Form.Item>
          <Form.Item label="Google Sheets URL або ID" name="url" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={priceOverrideRow?.override_id ? 'Ручна ціна' : 'Додати ручну ціну'}
        open={priceOverrideModalOpen}
        centered
        onCancel={closePriceOverride}
        onOk={savePriceOverride}
        confirmLoading={priceOverrideLoading}
      >
        <Form form={priceOverrideForm} layout="vertical">
          <Form.Item label="Артикул" name="article" rules={[{ required: true }]}>
            <Input disabled={Boolean(priceOverrideRow?.override_id)} />
          </Form.Item>
          <Form.Item label="Розмір" name="size">
            <Input disabled={Boolean(priceOverrideRow?.override_id)} />
          </Form.Item>
          <Form.Item label="Ціна фінальна" name="price_final" rules={[{ required: true }]}>
            <InputNumber min={0} step={1} style={{ width: '100%' }} />
          </Form.Item>
          <Form.Item label="Коментар" name="notes">
            <Input.TextArea rows={3} />
          </Form.Item>
          {priceOverrideRow?.override_id ? (
            <Form.Item label="Активна" name="is_active" valuePropName="checked">
              <Switch />
            </Form.Item>
          ) : null}
          {priceOverrideRow?.override_id ? (
            <Text className="muted">
              Якщо вимкнути ручну ціну, повернеться розрахунок по націнці.
            </Text>
          ) : (
            <Text className="muted">
              Ручна ціна застосовується одразу до фінального набору.
            </Text>
          )}
        </Form>
      </Modal>

      <Modal
        title="Деталі логу"
        open={logModalOpen}
        centered
        onCancel={() => {
          setLogModalOpen(false);
          setSelectedLog(null);
        }}
        footer={null}
        width="min(90vw, 720px)"
      >
        {selectedLog ? (
          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
            <Space>
              <Text strong>Рівень:</Text>
              <Tag color={selectedLog.level === 'error' ? 'red' : 'blue'}>
                {selectedLog.level}
              </Tag>
            </Space>
            <Text>
              <Text strong>Час:</Text> {formatDateTime(selectedLog.created_at)}
            </Text>
            <Text>
              <Text strong>Повідомлення:</Text> {selectedLog.message}
            </Text>
            <div>
              <Text strong>Дані:</Text>
              <pre className="result-box">
                {selectedLog.data
                  ? JSON.stringify(selectedLog.data, null, 2)
                  : 'Немає додаткових даних'}
              </pre>
            </div>
          </Space>
        ) : (
          <Text>Немає даних.</Text>
        )}
      </Modal>
    </Layout>
  );
}

export default App;
