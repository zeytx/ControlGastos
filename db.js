/* ============================================
   FINANZAS LOCALES - IndexedDB Engine
   Salary cycles + cards by statement cutoff
   ============================================ */

const FinanceDB = (() => {
  const DB_NAME = 'finanzas-locales-db';
  const DB_VERSION = 7;
  const SETTINGS_KEY = 'main';

  const STORE_NAMES = {
    transactions: 'transactions',
    accounts: 'accounts',
    debts: 'debts',
    goals: 'goals',
    recurring: 'recurring',
    categories: 'categories',
    settings: 'settings',
    cards: 'cards',
    financialCycles: 'financial_cycles'
  };

  const TRANSACTION_TYPES = new Set([
    'expense',
    'income',
    'transfer',
    'debt_payment',
    'goal_contribution',
    'card_charge',
    'card_payment'
  ]);

  const ACCOUNT_KINDS = new Set(['cash', 'bank', 'card', 'savings']);
  const DEBT_KINDS = new Set(['loan', 'installment', 'personal', 'credit_card']);
  const CARD_SOURCE_TYPES = new Set(['manual', 'legacy', 'recurring', 'system']);

  const DEFAULT_CATEGORIES = [
    { id: 'salary', name: 'Sueldo', type: 'income' },
    { id: 'freelance', name: 'Ingreso extra', type: 'income' },
    { id: 'food', name: 'Comida', type: 'expense' },
    { id: 'transport', name: 'Transporte', type: 'expense' },
    { id: 'home', name: 'Casa', type: 'expense' },
    { id: 'health', name: 'Salud', type: 'expense' },
    { id: 'shopping', name: 'Compras', type: 'expense' },
    { id: 'services', name: 'Servicios', type: 'expense' },
    { id: 'entertainment', name: 'Entretenimiento', type: 'expense' },
    { id: 'education', name: 'Educacion', type: 'expense' },
    { id: 'debt-payment', name: 'Pago de deuda', type: 'expense' },
    { id: 'goal-contribution', name: 'Aporte a meta', type: 'expense' },
    { id: 'other-expense', name: 'Otros gastos', type: 'expense' },
    { id: 'other-income', name: 'Otros ingresos', type: 'income' }
  ];

  const DEFAULT_ACCOUNTS = [
    {
      id: 'cash-main',
      name: 'Efectivo',
      kind: 'cash',
      openingBalance: 0,
      includeInNetWorth: true,
      archived: false
    },
    {
      id: 'bank-main',
      name: 'Banco principal',
      kind: 'bank',
      openingBalance: 0,
      includeInNetWorth: true,
      archived: false
    },
    {
      id: 'savings-main',
      name: 'Ahorros',
      kind: 'savings',
      openingBalance: 0,
      includeInNetWorth: true,
      archived: false
    }
  ];

  const DEFAULT_SETTINGS = {
    id: SETTINGS_KEY,
    theme: 'dark',
    monthlyBudget: 0,
    installPromptDismissedAt: '',
    lastBackupAt: '',
    financialCycleConfig: {
      primarySalaryRecurringId: '',
      savingsAccountId: 'savings-main',
      sweepSourceAccountId: 'bank-main',
      liquidAccountIds: ['cash-main', 'bank-main'],
      onboardingCompleted: false,
      savingsSweepEnabled: true
    }
  };

  let dbPromise = null;
  let initialized = false;

  function createId(prefix) {
    const random = Math.random().toString(36).slice(2, 10);
    return `${prefix}-${Date.now().toString(36)}-${random}`;
  }

  function roundAmount(value) {
    const parsed = parseFloat(value);
    if (!Number.isFinite(parsed)) return 0;
    return Math.round(parsed * 100) / 100;
  }

  function clampDay(value, fallback = 1) {
    const parsed = parseInt(value, 10);
    if (!Number.isFinite(parsed)) return fallback;
    return Math.max(1, Math.min(28, parsed));
  }

  function normalizeDate(value) {
    const raw = String(value || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const today = new Date();
    return `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}-${String(today.getDate()).padStart(2, '0')}`;
  }

  function compareDate(a, b) {
    return normalizeDate(a).localeCompare(normalizeDate(b));
  }

  function maxDate(a, b) {
    return compareDate(a, b) >= 0 ? normalizeDate(a) : normalizeDate(b);
  }

  function addDays(value, days) {
    const date = new Date(`${normalizeDate(value)}T12:00:00`);
    date.setDate(date.getDate() + days);
    return date.toISOString().slice(0, 10);
  }

  function addMonths(value, months, preferredDay = null) {
    const base = new Date(`${normalizeDate(value)}T12:00:00`);
    const day = preferredDay || base.getDate();
    const year = base.getFullYear();
    const monthIndex = base.getMonth() + months;
    const target = new Date(year, monthIndex, 1, 12, 0, 0);
    const lastDay = new Date(target.getFullYear(), target.getMonth() + 1, 0).getDate();
    target.setDate(Math.min(day, lastDay));
    return target.toISOString().slice(0, 10);
  }

  function getYearMonth(date) {
    return normalizeDate(date).slice(0, 7);
  }

  function getToday() {
    return new Date().toISOString().slice(0, 10);
  }

  function getMostRecentOccurrence(dayOfMonth, referenceDate = getToday()) {
    const normalized = normalizeDate(referenceDate);
    const year = parseInt(normalized.slice(0, 4), 10);
    const month = parseInt(normalized.slice(5, 7), 10);
    const day = parseInt(normalized.slice(8, 10), 10);
    const clampedDay = clampDay(dayOfMonth, 1);

    if (day >= clampedDay) {
      return new Date(year, month - 1, clampedDay, 12, 0, 0).toISOString().slice(0, 10);
    }

    return new Date(year, month - 2, clampedDay, 12, 0, 0).toISOString().slice(0, 10);
  }

  function getNextOccurrence(dayOfMonth, referenceDate = getToday()) {
    const normalized = normalizeDate(referenceDate);
    const year = parseInt(normalized.slice(0, 4), 10);
    const month = parseInt(normalized.slice(5, 7), 10);
    const day = parseInt(normalized.slice(8, 10), 10);
    const clampedDay = clampDay(dayOfMonth, 1);

    if (day <= clampedDay) {
      return new Date(year, month - 1, clampedDay, 12, 0, 0).toISOString().slice(0, 10);
    }

    return new Date(year, month, clampedDay, 12, 0, 0).toISOString().slice(0, 10);
  }

  function resolveDefaultRecurringStartDate(input = {}, existing = null) {
    const raw = String(input.startDate ?? existing?.startDate ?? '').trim();
    if (raw) return normalizeDate(raw);

    const dayOfMonth = clampDay(input.dayOfMonth ?? existing?.dayOfMonth ?? 1, 1);
    const isPrimarySalary = !!(input.isPrimarySalary ?? existing?.isPrimarySalary);
    const opensFinancialCycle = input.opensFinancialCycle ?? existing?.opensFinancialCycle ?? isPrimarySalary;

    if (isPrimarySalary || opensFinancialCycle) {
      return getMostRecentOccurrence(dayOfMonth);
    }

    return getNextOccurrence(dayOfMonth);
  }

  function parseJson(value, fallback) {
    if (!value || typeof value !== 'object') return fallback;
    return value;
  }

  function toNumber(value, fallback = 0) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function requestToPromise(request) {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error('IndexedDB request failed.'));
    });
  }

  function transactionToPromise(transaction) {
    return new Promise((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error || new Error('IndexedDB transaction aborted.'));
      transaction.onerror = () => reject(transaction.error || new Error('IndexedDB transaction failed.'));
    });
  }

  function ensureStore(db, name, options) {
    if (!db.objectStoreNames.contains(name)) {
      db.createObjectStore(name, options);
    }
  }

  function createIndexes(upgradeTransaction) {
    const txStore = upgradeTransaction.objectStore(STORE_NAMES.transactions);
    if (!txStore.indexNames.contains('by_date')) txStore.createIndex('by_date', 'date', { unique: false });
    if (!txStore.indexNames.contains('by_type')) txStore.createIndex('by_type', 'type', { unique: false });
    if (!txStore.indexNames.contains('by_budget_cycle')) txStore.createIndex('by_budget_cycle', 'budgetCycleId', { unique: false });
    if (!txStore.indexNames.contains('by_card')) txStore.createIndex('by_card', 'cardId', { unique: false });
    if (!txStore.indexNames.contains('by_statement_cycle')) txStore.createIndex('by_statement_cycle', 'statementCycleKey', { unique: false });
    if (!txStore.indexNames.contains('by_recurring_occurrence')) {
      txStore.createIndex('by_recurring_occurrence', 'recurringOccurrenceKey', { unique: false });
    }

    const recurringStore = upgradeTransaction.objectStore(STORE_NAMES.recurring);
    if (!recurringStore.indexNames.contains('by_day')) recurringStore.createIndex('by_day', 'dayOfMonth', { unique: false });

    const cycleStore = upgradeTransaction.objectStore(STORE_NAMES.financialCycles);
    if (!cycleStore.indexNames.contains('by_start')) cycleStore.createIndex('by_start', 'startDate', { unique: false });
    if (!cycleStore.indexNames.contains('by_salary_recurring')) {
      cycleStore.createIndex('by_salary_recurring', 'salaryRecurringId', { unique: false });
    }
  }

  async function openRawDB() {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = () => {
        const db = request.result;
        ensureStore(db, STORE_NAMES.transactions, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.accounts, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.debts, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.goals, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.recurring, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.categories, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.settings, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.cards, { keyPath: 'id' });
        ensureStore(db, STORE_NAMES.financialCycles, { keyPath: 'id' });
        createIndexes(request.transaction);
      };

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error('No se pudo abrir la base local.'));
    });
  }

  async function getDB() {
    if (!dbPromise) dbPromise = openRawDB();
    return dbPromise;
  }

  async function initDB() {
    const db = await getDB();
    if (!initialized) {
      initialized = true;
      await seedDefaults();
      await migrateLegacyStoresIfNeeded();
      await syncFinanceEngine();
    }
    return db;
  }

  async function getAll(storeName) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readonly');
    const store = transaction.objectStore(storeName);
    const request = store.getAll();
    const result = await requestToPromise(request);
    await transactionToPromise(transaction);
    return result;
  }

  async function getOne(storeName, id) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readonly');
    const store = transaction.objectStore(storeName);
    const request = store.get(id);
    const result = await requestToPromise(request);
    await transactionToPromise(transaction);
    return result || null;
  }

  async function putMany(storeName, values) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readwrite');
    const store = transaction.objectStore(storeName);
    values.forEach((value) => store.put(value));
    await transactionToPromise(transaction);
  }

  async function deleteOne(storeName, id) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readwrite');
    transaction.objectStore(storeName).delete(id);
    await transactionToPromise(transaction);
  }

  async function clearStore(storeName) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readwrite');
    transaction.objectStore(storeName).clear();
    await transactionToPromise(transaction);
  }

  async function replaceStore(storeName, values) {
    const db = await getDB();
    const transaction = db.transaction(storeName, 'readwrite');
    const store = transaction.objectStore(storeName);
    store.clear();
    values.forEach((value) => store.put(value));
    await transactionToPromise(transaction);
  }

  function normalizeSettings(input = {}) {
    const merged = {
      ...DEFAULT_SETTINGS,
      ...parseJson(input, {}),
      financialCycleConfig: {
        ...DEFAULT_SETTINGS.financialCycleConfig,
        ...parseJson(input.financialCycleConfig, {})
      }
    };
    merged.id = SETTINGS_KEY;
    merged.monthlyBudget = roundAmount(merged.monthlyBudget);
    merged.financialCycleConfig.primarySalaryRecurringId = String(
      merged.financialCycleConfig.primarySalaryRecurringId || ''
    ).trim();
    merged.financialCycleConfig.savingsAccountId = String(
      merged.financialCycleConfig.savingsAccountId || ''
    ).trim();
    merged.financialCycleConfig.sweepSourceAccountId = String(
      merged.financialCycleConfig.sweepSourceAccountId || ''
    ).trim();
    merged.financialCycleConfig.liquidAccountIds = Array.isArray(merged.financialCycleConfig.liquidAccountIds)
      ? merged.financialCycleConfig.liquidAccountIds.filter(Boolean)
      : DEFAULT_SETTINGS.financialCycleConfig.liquidAccountIds.slice();
    merged.financialCycleConfig.onboardingCompleted = !!merged.financialCycleConfig.onboardingCompleted;
    merged.financialCycleConfig.savingsSweepEnabled = merged.financialCycleConfig.savingsSweepEnabled !== false;
    return merged;
  }

  function normalizeCategory(input = {}) {
    return {
      id: String(input.id || createId('cat')).trim(),
      name: String(input.name || 'Categoria').trim().slice(0, 60),
      type: input.type === 'income' ? 'income' : 'expense'
    };
  }

  function normalizeAccount(input = {}, existing = null) {
    const now = new Date().toISOString();
    const kind = ACCOUNT_KINDS.has(input.kind) ? input.kind : existing?.kind || 'bank';
    return {
      id: String(input.id || existing?.id || createId('acc')).trim(),
      name: String(input.name || existing?.name || 'Cuenta').trim().slice(0, 60),
      kind,
      openingBalance: roundAmount(input.openingBalance ?? existing?.openingBalance ?? 0),
      includeInNetWorth: input.includeInNetWorth ?? existing?.includeInNetWorth ?? true,
      archived: !!(input.archived ?? existing?.archived),
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeGoal(input = {}, existing = null) {
    const now = new Date().toISOString();
    return {
      id: String(input.id || existing?.id || createId('goal')).trim(),
      name: String(input.name || existing?.name || 'Meta').trim().slice(0, 80),
      targetAmount: roundAmount(input.targetAmount ?? existing?.targetAmount ?? 0),
      currentAmount: roundAmount(input.currentAmount ?? existing?.currentAmount ?? 0),
      targetDate: input.targetDate ? normalizeDate(input.targetDate) : '',
      accountId: String(input.accountId ?? existing?.accountId ?? '').trim(),
      archived: !!(input.archived ?? existing?.archived),
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeDebt(input = {}, existing = null) {
    const now = new Date().toISOString();
    const kind = DEBT_KINDS.has(input.kind) ? input.kind : existing?.kind || 'loan';
    return {
      id: String(input.id || existing?.id || createId('debt')).trim(),
      name: String(input.name || existing?.name || 'Deuda').trim().slice(0, 80),
      kind,
      totalAmount: roundAmount(input.totalAmount ?? existing?.totalAmount ?? 0),
      outstandingAmount: roundAmount(input.outstandingAmount ?? existing?.outstandingAmount ?? 0),
      dueDay: clampDay(input.dueDay ?? existing?.dueDay ?? 1),
      minimumPayment: roundAmount(input.minimumPayment ?? existing?.minimumPayment ?? 0),
      installmentCount: Math.max(0, parseInt(input.installmentCount ?? existing?.installmentCount ?? 0, 10) || 0),
      installmentsPaid: Math.max(0, parseInt(input.installmentsPaid ?? existing?.installmentsPaid ?? 0, 10) || 0),
      accountId: String(input.accountId ?? existing?.accountId ?? '').trim(),
      archived: !!(input.archived ?? existing?.archived),
      legacyMigratedToCardId: String(input.legacyMigratedToCardId ?? existing?.legacyMigratedToCardId ?? '').trim(),
      closingDay: kind === 'credit_card' ? clampDay(input.closingDay ?? existing?.closingDay ?? 10, 10) : 0,
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeRecurring(input = {}, existing = null) {
    const now = new Date().toISOString();
    const type = input.type === 'income' ? 'income' : 'expense';
    const startDate = resolveDefaultRecurringStartDate(input, existing);
    const endMonthRaw = String(input.endMonth ?? existing?.endMonth ?? '').trim();
    const endMonth = /^\d{4}-\d{2}$/.test(endMonthRaw)
      ? endMonthRaw
      : /^\d{4}-\d{2}-\d{2}$/.test(endMonthRaw)
        ? endMonthRaw.slice(0, 7)
        : '';
    return {
      id: String(input.id || existing?.id || createId('rec')).trim(),
      type,
      amount: roundAmount(input.amount ?? existing?.amount ?? 0),
      dayOfMonth: clampDay(input.dayOfMonth ?? existing?.dayOfMonth ?? 1),
      startDate,
      endMonth,
      description: String(input.description || existing?.description || 'Recurrente').trim().slice(0, 80),
      categoryId: String(input.categoryId ?? existing?.categoryId ?? '').trim(),
      accountId: String(input.accountId ?? existing?.accountId ?? '').trim(),
      active: input.active ?? existing?.active ?? true,
      isPrimarySalary: !!(input.isPrimarySalary ?? existing?.isPrimarySalary),
      opensFinancialCycle: input.opensFinancialCycle ?? existing?.opensFinancialCycle ?? !!(input.isPrimarySalary ?? existing?.isPrimarySalary),
      savingsSweepEnabled: input.savingsSweepEnabled ?? existing?.savingsSweepEnabled ?? true,
      savingsAccountId: String(input.savingsAccountId ?? existing?.savingsAccountId ?? '').trim(),
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeCard(input = {}, existing = null) {
    const now = new Date().toISOString();
    const bankName = String(input.bankName || existing?.bankName || '').trim() || 'Pendiente';
    const last4Raw = String(input.last4 || existing?.last4 || '').replace(/\D/g, '').slice(-4);
    const last4 = last4Raw.padStart(4, '0');
    const creditLimit = roundAmount(input.creditLimit ?? existing?.creditLimit ?? 0);
    const openingDebtAmount = roundAmount(
      input.openingDebtAmount ??
      input.openingBalance ??
      existing?.openingDebtAmount ??
      existing?.openingBalance ??
      0
    );
    const needsReview = !!(input.needsReview ?? (creditLimit <= 0));
    return {
      id: String(input.id || existing?.id || createId('card')).trim(),
      bankName,
      last4,
      label: `${bankName} \u2022 ${last4}`,
      closingDay: clampDay(input.closingDay ?? existing?.closingDay ?? 10, 10),
      dueDay: clampDay(input.dueDay ?? existing?.dueDay ?? 25, 25),
      paymentAccountId: String(input.paymentAccountId ?? existing?.paymentAccountId ?? '').trim(),
      archived: !!(input.archived ?? existing?.archived),
      needsReview,
      creditLimit,
      openingDebtAmount,
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeCycle(input = {}, existing = null) {
    const now = new Date().toISOString();
    return {
      id: String(input.id || existing?.id || createId('cycle')).trim(),
      salaryRecurringId: String(input.salaryRecurringId || existing?.salaryRecurringId || '').trim(),
      salaryTransactionId: String(input.salaryTransactionId || existing?.salaryTransactionId || '').trim(),
      startDate: normalizeDate(input.startDate || existing?.startDate || getToday()),
      endDate: normalizeDate(input.endDate || existing?.endDate || getToday()),
      status: input.status === 'closed' ? 'closed' : 'open',
      sweepTransferId: String(input.sweepTransferId ?? existing?.sweepTransferId ?? '').trim(),
      savingsAccountId: String(input.savingsAccountId ?? existing?.savingsAccountId ?? '').trim(),
      liquidAccountIds: Array.isArray(input.liquidAccountIds ?? existing?.liquidAccountIds)
        ? (input.liquidAccountIds ?? existing?.liquidAccountIds).filter(Boolean)
        : [],
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };
  }

  function normalizeTransaction(input = {}, existing = null) {
    const now = new Date().toISOString();
    const type = TRANSACTION_TYPES.has(input.type) ? input.type : existing?.type || 'expense';
    const date = normalizeDate(input.date || existing?.date || getToday());
    const sourceType = CARD_SOURCE_TYPES.has(input.sourceType) || String(input.sourceType || '').includes('image')
      ? input.sourceType || existing?.sourceType || 'manual'
      : existing?.sourceType || 'manual';
    const rawInstallmentCount = parseInt(input.installmentCount ?? existing?.installmentCount ?? 0, 10) || 0;
    const normalizedInstallmentCount = type === 'card_charge'
      ? Math.max(1, rawInstallmentCount || 1)
      : 0;
    const rawInstallmentIndex = parseInt(input.installmentIndex ?? existing?.installmentIndex ?? 0, 10) || 0;
    const normalizedInstallmentIndex = type === 'card_charge'
      ? Math.max(1, rawInstallmentIndex || 1)
      : 0;
    const normalized = {
      id: String(input.id || existing?.id || createId('tx')).trim(),
      type,
      amount: roundAmount(input.amount ?? existing?.amount ?? 0),
      date,
      activityDate: date,
      yearMonth: getYearMonth(date),
      description: String(input.description || existing?.description || 'Movimiento').trim().slice(0, 80),
      categoryId: String(input.categoryId ?? existing?.categoryId ?? '').trim(),
      fromAccountId: String(input.fromAccountId ?? existing?.fromAccountId ?? '').trim(),
      toAccountId: String(input.toAccountId ?? existing?.toAccountId ?? '').trim(),
      linkedEntityType: String(input.linkedEntityType ?? existing?.linkedEntityType ?? '').trim(),
      linkedEntityId: String(input.linkedEntityId ?? existing?.linkedEntityId ?? '').trim(),
      sourceType,
      notes: String(input.notes ?? existing?.notes ?? '').trim().slice(0, 300),
      cardId: String(input.cardId ?? existing?.cardId ?? '').trim(),
      statementCycleKey: String(input.statementCycleKey ?? existing?.statementCycleKey ?? '').trim(),
      budgetCycleId: String(input.budgetCycleId ?? existing?.budgetCycleId ?? '').trim(),
      purchaseDate: type === 'card_charge'
        ? normalizeDate(input.purchaseDate ?? existing?.purchaseDate ?? date)
        : '',
      installmentGroupId: type === 'card_charge'
        ? String(input.installmentGroupId ?? existing?.installmentGroupId ?? '').trim()
        : '',
      installmentIndex: normalizedInstallmentIndex,
      installmentCount: normalizedInstallmentCount,
      originalPurchaseAmount: type === 'card_charge'
        ? roundAmount(input.originalPurchaseAmount ?? input.amount ?? existing?.originalPurchaseAmount ?? existing?.amount ?? 0)
        : 0,
      installmentAmount: type === 'card_charge'
        ? roundAmount(input.installmentAmount ?? input.amount ?? existing?.installmentAmount ?? existing?.amount ?? 0)
        : 0,
      recurringOccurrenceKey: String(input.recurringOccurrenceKey ?? existing?.recurringOccurrenceKey ?? '').trim(),
      autoGenerated: !!(input.autoGenerated ?? existing?.autoGenerated),
      systemTag: String(input.systemTag ?? existing?.systemTag ?? '').trim(),
      createdAt: existing?.createdAt || input.createdAt || now,
      updatedAt: now
    };

    if (type === 'income') normalized.fromAccountId = '';
    if (type === 'expense') normalized.toAccountId = '';
    if (type === 'card_charge') {
      normalized.fromAccountId = '';
      normalized.toAccountId = '';
      normalized.purchaseDate = normalizeDate(normalized.purchaseDate || normalized.date);
      normalized.installmentCount = Math.max(1, normalized.installmentCount || 1);
      normalized.installmentIndex = Math.min(
        Math.max(1, normalized.installmentIndex || 1),
        normalized.installmentCount
      );
      normalized.originalPurchaseAmount = roundAmount(normalized.originalPurchaseAmount || normalized.amount);
      normalized.installmentAmount = roundAmount(normalized.installmentAmount || normalized.amount);
    }
    if (type === 'card_payment') {
      normalized.toAccountId = '';
      normalized.categoryId = normalized.categoryId || 'debt-payment';
    }
    if (type === 'debt_payment') {
      normalized.toAccountId = '';
      normalized.categoryId = 'debt-payment';
    }
    if (type === 'goal_contribution') {
      normalized.categoryId = 'goal-contribution';
    }

    return normalized;
  }

  async function seedDefaults() {
    const [settings, categories, accounts] = await Promise.all([
      getOne(STORE_NAMES.settings, SETTINGS_KEY),
      getAll(STORE_NAMES.categories),
      getAll(STORE_NAMES.accounts)
    ]);

    if (!settings) {
      await putMany(STORE_NAMES.settings, [normalizeSettings(DEFAULT_SETTINGS)]);
    } else {
      await putMany(STORE_NAMES.settings, [normalizeSettings(settings)]);
    }

    if (!categories.length) {
      await putMany(STORE_NAMES.categories, DEFAULT_CATEGORIES.map((item) => normalizeCategory(item)));
    }

    if (!accounts.length) {
      await putMany(STORE_NAMES.accounts, DEFAULT_ACCOUNTS.map((item) => normalizeAccount(item)));
    }
  }

  async function migrateLegacyStoresIfNeeded() {
    const db = await getDB();
    const legacyStores = ['gastos', 'recurrentes'];
    const present = legacyStores.filter((name) => db.objectStoreNames.contains(name));
    if (!present.length) return;

    const transaction = db.transaction(
      [STORE_NAMES.transactions, STORE_NAMES.recurring].concat(present),
      'readwrite'
    );

    const putTransactions = [];
    const putRecurring = [];

    if (present.includes('gastos')) {
      const legacyTxStore = transaction.objectStore('gastos');
      await new Promise((resolve, reject) => {
        const request = legacyTxStore.openCursor();
        request.onsuccess = (event) => {
          const cursor = event.target.result;
          if (!cursor) {
            resolve();
            return;
          }
          putTransactions.push(
            normalizeTransaction({
              id: cursor.value.id || createId('tx'),
              type: cursor.value.type || 'expense',
              amount: cursor.value.amount,
              date: cursor.value.date,
              description: cursor.value.description || cursor.value.concept || 'Movimiento migrado',
              categoryId: cursor.value.categoryId || 'other-expense',
              fromAccountId: cursor.value.accountId || 'bank-main',
              toAccountId: '',
              sourceType: 'legacy',
              notes: cursor.value.notes || ''
            })
          );
          cursor.continue();
        };
        request.onerror = () => reject(request.error || new Error('Error migrando gastos.'));
      });
    }

    if (present.includes('recurrentes')) {
      const legacyRecurringStore = transaction.objectStore('recurrentes');
      await new Promise((resolve, reject) => {
        const request = legacyRecurringStore.openCursor();
        request.onsuccess = (event) => {
          const cursor = event.target.result;
          if (!cursor) {
            resolve();
            return;
          }
          putRecurring.push(
            normalizeRecurring({
              id: cursor.value.id || createId('rec'),
              type: cursor.value.type || 'expense',
              amount: cursor.value.amount,
              dayOfMonth: cursor.value.dayOfMonth || cursor.value.day || 1,
              description: cursor.value.description || 'Recurrente migrado',
              categoryId: cursor.value.categoryId || (cursor.value.type === 'income' ? 'other-income' : 'other-expense'),
              accountId: cursor.value.accountId || 'bank-main',
              active: cursor.value.active !== false
            })
          );
          cursor.continue();
        };
        request.onerror = () => reject(request.error || new Error('Error migrando recurrentes.'));
      });
    }

    putTransactions.forEach((item) => transaction.objectStore(STORE_NAMES.transactions).put(item));
    putRecurring.forEach((item) => transaction.objectStore(STORE_NAMES.recurring).put(item));
    await transactionToPromise(transaction);
  }

  function sortByDateAsc(items) {
    return [...items].sort((a, b) => {
      const dateCompare = compareDate(a.date || a.startDate, b.date || b.startDate);
      if (dateCompare !== 0) return dateCompare;
      return String(a.id || '').localeCompare(String(b.id || ''));
    });
  }

  function getRecurringProjectionBounds(recurring, transactions) {
    const today = getToday();
    const authoredDates = transactions
      .filter((item) => !item.recurringOccurrenceKey && item.systemTag !== 'cycle_sweep')
      .map((item) => item.statementCycleKey ? maxDate(item.statementCycleKey, item.date) : item.date)
      .filter(Boolean)
      .sort(compareDate);
    const recurringDates = recurring
      .map((item) => item.startDate)
      .filter(Boolean)
      .sort(compareDate);
    const first = authoredDates[0] || recurringDates[0] || today;
    const latestAuthored = authoredDates[authoredDates.length - 1] || today;
    const baseStart = compareDate(first, today) < 0 ? first : today;
    return {
      startDate: addMonths(baseStart, -1, 1),
      endDate: maxDate(addMonths(today, 2, 28), addMonths(latestAuthored, 1, 28))
    };
  }

  function eachMonth(startDate, endDate, callback) {
    let cursor = normalizeDate(startDate).slice(0, 7) + '-01';
    const limit = normalizeDate(endDate);
    while (compareDate(cursor, limit) <= 0) {
      callback(cursor);
      cursor = addMonths(cursor, 1, 1);
    }
  }

  function resolveRecurringAccountId(item, settings) {
    if (item.accountId) return item.accountId;
    if (item.type === 'income') {
      return settings?.financialCycleConfig?.sweepSourceAccountId || 'bank-main';
    }
    return '';
  }

  function materializeRecurringTransactions(recurring, transactions, settings) {
    const byKey = new Map(
      transactions
        .filter((item) => item.recurringOccurrenceKey)
        .map((item) => [item.recurringOccurrenceKey, item])
    );
    const activeRecurringIds = new Set(recurring.filter((item) => item.active).map((item) => item.id));
    const results = [];
    const { startDate, endDate } = getRecurringProjectionBounds(recurring, transactions);

    recurring
      .filter((item) => item.active)
      .forEach((item) => {
        const resolvedAccountId = resolveRecurringAccountId(item, settings);
        eachMonth(startDate, endDate, (monthStart) => {
          const occurrenceDate = addMonths(monthStart, 0, item.dayOfMonth);
          if (item.startDate && compareDate(occurrenceDate, item.startDate) < 0) return;
          if (item.endMonth && getYearMonth(occurrenceDate) > item.endMonth) return;
          const key = `${item.id}@${occurrenceDate}`;
          const existing = byKey.get(key) || null;
          const payload = normalizeTransaction(
            {
              id: existing?.id,
              type: item.type,
              amount: item.amount,
              date: occurrenceDate,
              description: item.description,
              categoryId: item.categoryId || (item.type === 'income' ? 'salary' : 'other-expense'),
              fromAccountId: item.type === 'expense' ? resolvedAccountId : '',
              toAccountId: item.type === 'income' ? resolvedAccountId : '',
              linkedEntityType: 'recurring',
              linkedEntityId: item.id,
              sourceType: 'recurring',
              recurringOccurrenceKey: key,
              autoGenerated: true
            },
            existing
          );
          results.push(payload);
        });
      });

    const preservedTransactions = transactions.filter((item) => {
      if (!item.recurringOccurrenceKey) return true;
      if (activeRecurringIds.has(item.linkedEntityId)) return false;
      return compareDate(item.date, getToday()) < 0;
    });
    const mergedMap = new Map(preservedTransactions.map((item) => [item.id, item]));
    results.forEach((item) => mergedMap.set(item.id, item));
    return Array.from(mergedMap.values());
  }

  function choosePrimarySalaryRecurring(recurring, settings) {
    if (settings.financialCycleConfig.primarySalaryRecurringId) {
      const found = recurring.find((item) => item.id === settings.financialCycleConfig.primarySalaryRecurringId);
      if (found) return found;
    }
    return recurring.find((item) => item.isPrimarySalary) || null;
  }

  function nextExpectedSalaryDate(currentStartDate, recurring) {
    return addMonths(currentStartDate, 1, recurring.dayOfMonth);
  }

  function buildFinancialCycles(primarySalaryRecurring, transactions, settings) {
    if (!primarySalaryRecurring) return [];

    const salaryTransactions = sortByDateAsc(
      transactions.filter(
        (item) =>
          item.type === 'income' &&
          item.linkedEntityType === 'recurring' &&
          item.linkedEntityId === primarySalaryRecurring.id
      )
    );

    if (!salaryTransactions.length) return [];

    return salaryTransactions.map((salaryTransaction, index) => {
      const nextSalaryTransaction = salaryTransactions[index + 1] || null;
      const nextStartDate = nextSalaryTransaction
        ? nextSalaryTransaction.date
        : nextExpectedSalaryDate(salaryTransaction.date, primarySalaryRecurring);
      const endDate = addDays(nextStartDate, -1);
      const today = getToday();
      return normalizeCycle({
        id: `cycle-${salaryTransaction.id}`,
        salaryRecurringId: primarySalaryRecurring.id,
        salaryTransactionId: salaryTransaction.id,
        startDate: salaryTransaction.date,
        endDate,
        status: compareDate(endDate, today) < 0 ? 'closed' : 'open',
        sweepTransferId: '',
        savingsAccountId: settings.financialCycleConfig.savingsAccountId,
        liquidAccountIds: settings.financialCycleConfig.liquidAccountIds
      });
    });
  }

  function findCycleContainingDate(cycles, date) {
    const normalizedDate = normalizeDate(date);
    return cycles.find(
      (cycle) => compareDate(cycle.startDate, normalizedDate) <= 0 && compareDate(normalizedDate, cycle.endDate) <= 0
    ) || null;
  }

  function getStatementClosingDate(activityDate, closingDay) {
    const normalized = normalizeDate(activityDate);
    const year = parseInt(normalized.slice(0, 4), 10);
    const month = parseInt(normalized.slice(5, 7), 10);
    const day = parseInt(normalized.slice(8, 10), 10);
    const closing = clampDay(closingDay, 10);

    if (day >= closing) {
      const next = new Date(year, month, 1, 12, 0, 0);
      next.setDate(closing);
      return next.toISOString().slice(0, 10);
    }

    const current = new Date(year, month - 1, closing, 12, 0, 0);
    return current.toISOString().slice(0, 10);
  }

  function getUpcomingStatementClosingDate(referenceDate, closingDay) {
    const normalized = normalizeDate(referenceDate);
    const year = parseInt(normalized.slice(0, 4), 10);
    const month = parseInt(normalized.slice(5, 7), 10);
    const day = parseInt(normalized.slice(8, 10), 10);
    const closing = clampDay(closingDay, 10);

    if (day <= closing) {
      return new Date(year, month - 1, closing, 12, 0, 0).toISOString().slice(0, 10);
    }

    return new Date(year, month, closing, 12, 0, 0).toISOString().slice(0, 10);
  }

  function getNextDueDate(closingDate, dueDay) {
    const closing = new Date(`${normalizeDate(closingDate)}T12:00:00`);
    const closingMonth = closing.getMonth();
    const closingYear = closing.getFullYear();
    const due = clampDay(dueDay, 25);

    const sameMonthCandidate = new Date(closingYear, closingMonth, due, 12, 0, 0);
    if (sameMonthCandidate > closing) return sameMonthCandidate.toISOString().slice(0, 10);

    const nextMonthCandidate = new Date(closingYear, closingMonth + 1, due, 12, 0, 0);
    return nextMonthCandidate.toISOString().slice(0, 10);
  }

  function getRelevantCycle(cycles, referenceDate = getToday()) {
    const sortedCycles = [...cycles].sort((a, b) => compareDate(a.startDate, b.startDate));
    return (
      sortedCycles.find((cycle) => compareDate(cycle.startDate, referenceDate) <= 0 && compareDate(referenceDate, cycle.endDate) <= 0) ||
      sortedCycles.find((cycle) => compareDate(cycle.startDate, referenceDate) >= 0) ||
      sortedCycles[sortedCycles.length - 1] ||
      null
    );
  }

  function getNextCycle(cycles, baseCycle) {
    if (!baseCycle) return null;
    return [...cycles]
      .sort((a, b) => compareDate(a.startDate, b.startDate))
      .find((cycle) => compareDate(cycle.startDate, baseCycle.startDate) > 0) || null;
  }

  function splitInstallmentAmounts(totalAmount, count) {
    const safeCount = Math.max(1, parseInt(count, 10) || 1);
    const totalCents = Math.round(roundAmount(totalAmount) * 100);
    const baseCents = Math.floor(totalCents / safeCount);
    const remainder = totalCents - baseCents * safeCount;
    return Array.from({ length: safeCount }, (_, index) => {
      const cents = index === safeCount - 1 ? baseCents + remainder : baseCents;
      return roundAmount(cents / 100);
    });
  }

  function buildInstallmentTransactions(record, card) {
    const purchaseDate = normalizeDate(record.purchaseDate || record.date);
    const installmentCount = Math.max(1, record.installmentCount || 1);
    const installmentGroupId = record.installmentGroupId || createId('inst');
    const firstClosingDate = getStatementClosingDate(purchaseDate, card.closingDay);
    const installmentAmounts = splitInstallmentAmounts(record.originalPurchaseAmount || record.amount, installmentCount);

    return installmentAmounts.map((amount, index) => normalizeTransaction({
      ...record,
      id: '',
      date: purchaseDate,
      purchaseDate,
      amount,
      installmentAmount: amount,
      originalPurchaseAmount: record.originalPurchaseAmount || record.amount,
      installmentGroupId,
      installmentIndex: index + 1,
      installmentCount,
      statementCycleKey: addMonths(firstClosingDate, index, card.closingDay)
    }));
  }

  function assignBudgetCycles(transactions, cards, cycles) {
    const cardMap = new Map(cards.map((item) => [item.id, item]));
    const sortedCycles = [...cycles].sort((a, b) => compareDate(a.startDate, b.startDate));

    return transactions.map((transaction) => {
      const draft = { ...transaction, statementCycleKey: transaction.statementCycleKey || '' };
      if (draft.type === 'card_charge' && draft.cardId) {
        const card = cardMap.get(draft.cardId);
        if (card) {
          const closingDate = draft.statementCycleKey || getStatementClosingDate(draft.date, card.closingDay);
          const budgetCycle =
            sortedCycles.find((cycle) => compareDate(cycle.startDate, closingDate) >= 0) ||
            sortedCycles[sortedCycles.length - 1] ||
            null;
          draft.statementCycleKey = closingDate;
          draft.budgetCycleId = budgetCycle?.id || '';
          return draft;
        }
      }

      const cycle = findCycleContainingDate(sortedCycles, draft.date);
      draft.budgetCycleId = cycle?.id || '';
      return draft;
    });
  }

  function calculateBalances(accounts, transactions, cutoffDate = '') {
    const balances = {};
    accounts.forEach((account) => {
      balances[account.id] = roundAmount(account.openingBalance);
    });

    sortByDateAsc(transactions).forEach((transaction) => {
      if (cutoffDate && compareDate(transaction.date, cutoffDate) >= 0) return;

      if (transaction.type === 'expense') {
        balances[transaction.fromAccountId] = roundAmount((balances[transaction.fromAccountId] || 0) - transaction.amount);
      } else if (transaction.type === 'income') {
        balances[transaction.toAccountId] = roundAmount((balances[transaction.toAccountId] || 0) + transaction.amount);
      } else if (transaction.type === 'transfer' || transaction.type === 'goal_contribution') {
        balances[transaction.fromAccountId] = roundAmount((balances[transaction.fromAccountId] || 0) - transaction.amount);
        if (transaction.toAccountId) {
          balances[transaction.toAccountId] = roundAmount((balances[transaction.toAccountId] || 0) + transaction.amount);
        }
      } else if (transaction.type === 'debt_payment' || transaction.type === 'card_payment') {
        balances[transaction.fromAccountId] = roundAmount((balances[transaction.fromAccountId] || 0) - transaction.amount);
      }
    });

    return balances;
  }

  function deriveStatements(cards, transactions, cycles, cutoffDate = '') {
    const charges = sortByDateAsc(
      transactions.filter(
        (item) =>
          item.type === 'card_charge' &&
          item.cardId
      )
    );
    const payments = sortByDateAsc(
      transactions.filter(
        (item) =>
          item.type === 'card_payment' &&
          item.cardId &&
          (!cutoffDate || compareDate(item.date, cutoffDate) < 0)
      )
    );

    const statementsMap = new Map();

    cards.forEach((card) => {
      if (card.openingDebtAmount > 0) {
        const key = `legacy-${card.id}`;
        statementsMap.set(key, {
          id: key,
          cardId: card.id,
          statementCycleKey: key,
          closingDate: '',
          dueDate: '',
          periodStart: '',
          periodEnd: '',
          budgetCycleId: '',
          chargedAmount: roundAmount(card.openingDebtAmount),
          paidAmount: 0,
          pendingAmount: roundAmount(card.openingDebtAmount),
          purchases: [],
          label: 'Deuda inicial',
          kind: 'opening-debt'
        });
      }
    });

    charges.forEach((charge) => {
      const card = cards.find((item) => item.id === charge.cardId);
      if (!card) return;
      const closingDate = charge.statementCycleKey || getStatementClosingDate(charge.purchaseDate || charge.date, card.closingDay);
      if (cutoffDate && compareDate(closingDate, cutoffDate) >= 0) return;
      const key = `${charge.cardId}@${closingDate}`;
      const periodStart = addMonths(closingDate, -1, card.closingDay);
      const periodEnd = addDays(closingDate, -1);
      if (!statementsMap.has(key)) {
        statementsMap.set(key, {
          id: key,
          cardId: charge.cardId,
          statementCycleKey: closingDate,
          closingDate,
          dueDate: getNextDueDate(closingDate, card.dueDay),
          periodStart,
          periodEnd,
          budgetCycleId: charge.budgetCycleId || '',
          chargedAmount: 0,
          paidAmount: 0,
          pendingAmount: 0,
          purchases: [],
          label: `${periodStart} - ${periodEnd}`,
          kind: 'statement'
        });
      }
      const statement = statementsMap.get(key);
      statement.purchases.push(charge);
      statement.chargedAmount = roundAmount(statement.chargedAmount + charge.amount);
      statement.pendingAmount = roundAmount(statement.chargedAmount - statement.paidAmount);
      if (!statement.budgetCycleId) statement.budgetCycleId = charge.budgetCycleId || '';
    });

    const statementList = Array.from(statementsMap.values()).sort((a, b) => {
      const aKey = a.closingDate || '0000-00-00';
      const bKey = b.closingDate || '0000-00-00';
      return aKey.localeCompare(bKey);
    });

    payments.forEach((payment) => {
      let remaining = payment.amount;
      const targeted = payment.statementCycleKey
        ? statementList.find((item) => item.cardId === payment.cardId && item.statementCycleKey === payment.statementCycleKey)
        : null;

      const candidateStatements = targeted
        ? [targeted].concat(statementList.filter((item) => item !== targeted && item.cardId === payment.cardId))
        : statementList.filter((item) => item.cardId === payment.cardId);

      candidateStatements.forEach((statement) => {
        if (remaining <= 0 || statement.pendingAmount <= 0) return;
        const applied = Math.min(remaining, statement.pendingAmount);
        statement.paidAmount = roundAmount(statement.paidAmount + applied);
        statement.pendingAmount = roundAmount(statement.chargedAmount - statement.paidAmount);
        remaining = roundAmount(remaining - applied);
      });
    });

    statementList.forEach((statement) => {
      statement.pendingAmount = roundAmount(statement.chargedAmount - statement.paidAmount);
      if (!statement.budgetCycleId && statement.closingDate) {
        const cycle = cycles.find((item) => compareDate(item.startDate, statement.closingDate) >= 0) || null;
        statement.budgetCycleId = cycle?.id || '';
      }
    });

    return statementList;
  }

  function getDebtDueDatesWithinCycle(cycle, debt) {
    const dates = [];
    const startMonth = cycle.startDate.slice(0, 7) + '-01';
    const endMonth = cycle.endDate.slice(0, 7) + '-01';
    let cursor = startMonth;
    while (compareDate(cursor, endMonth) <= 0) {
      const dueDate = addMonths(cursor, 0, debt.dueDay);
      if (
        compareDate(dueDate, cycle.startDate) >= 0 &&
        compareDate(dueDate, cycle.endDate) <= 0 &&
        !dates.includes(dueDate)
      ) {
        dates.push(dueDate);
      }
      cursor = addMonths(cursor, 1, 1);
    }
    return dates;
  }

  function calculateDebtPendingForCycle(cycle, debts, transactions, cutoffDate = '') {
    const debtPayments = transactions.filter(
      (item) =>
        item.type === 'debt_payment' &&
        (!cutoffDate || compareDate(item.date, cutoffDate) < 0) &&
        item.linkedEntityType === 'debt'
    );

    return debts
      .filter((debt) => !debt.archived && !debt.legacyMigratedToCardId && debt.outstandingAmount > 0)
      .reduce((sum, debt) => {
        const dueDates = getDebtDueDatesWithinCycle(cycle, debt);
        if (!dueDates.length) return sum;
        const required = roundAmount(Math.min(debt.minimumPayment || debt.outstandingAmount, debt.outstandingAmount) * dueDates.length);
        const paid = debtPayments
          .filter(
            (payment) =>
              payment.linkedEntityId === debt.id &&
              payment.budgetCycleId === cycle.id
          )
          .reduce((acc, payment) => acc + payment.amount, 0);
        return sum + Math.max(0, roundAmount(required - paid));
      }, 0);
  }

  function calculateCyclePendingCards(cycle, statements) {
    return roundAmount(
      statements
        .filter((statement) => statement.budgetCycleId === cycle.id)
        .reduce((sum, statement) => sum + statement.pendingAmount, 0)
    );
  }

  function createSweepTransaction(cycle, amount, settings) {
    return normalizeTransaction({
      id: `sweep-${cycle.id}`,
      type: 'transfer',
      amount,
      date: cycle.status === 'closed' ? addDays(cycle.endDate, 1) : cycle.endDate,
      description: `Barrido a ahorro ${cycle.startDate}`,
      categoryId: '',
      fromAccountId: settings.financialCycleConfig.sweepSourceAccountId,
      toAccountId: settings.financialCycleConfig.savingsAccountId,
      linkedEntityType: 'financial_cycle',
      linkedEntityId: cycle.id,
      sourceType: 'system',
      autoGenerated: true,
      systemTag: 'cycle_sweep',
      notes: 'Movimiento automatico por cierre de ciclo'
    });
  }

  function buildCyclesWithSweeps(cycles, baseTransactions, accounts, cards, debts, settings) {
    const transactionsWithoutSweeps = baseTransactions.filter((item) => item.systemTag !== 'cycle_sweep');
    const effectiveTransactions = [...transactionsWithoutSweeps];
    const sweepTransactions = [];
    const updatedCycles = cycles.map((cycle) => ({ ...cycle, sweepTransferId: '' }));

    for (let index = 0; index < updatedCycles.length; index += 1) {
      const cycle = updatedCycles[index];
      if (cycle.status !== 'closed') continue;
      if (!settings.financialCycleConfig.savingsSweepEnabled) continue;
      if (!settings.financialCycleConfig.savingsAccountId || !settings.financialCycleConfig.sweepSourceAccountId) continue;

      const cutoffDate = addDays(cycle.endDate, 1);
      const balancesBeforeSalary = calculateBalances(accounts, effectiveTransactions, cutoffDate);
      const liquidAvailable = (cycle.liquidAccountIds || [])
        .reduce((sum, accountId) => sum + (balancesBeforeSalary[accountId] || 0), 0);
      const sourceBalance = balancesBeforeSalary[settings.financialCycleConfig.sweepSourceAccountId] || 0;
      const statements = deriveStatements(cards, effectiveTransactions, updatedCycles, cutoffDate);
      const pendingCards = calculateCyclePendingCards(cycle, statements);
      const pendingDebts = calculateDebtPendingForCycle(cycle, debts, effectiveTransactions, cutoffDate);
      const freeNet = roundAmount(liquidAvailable - pendingCards - pendingDebts);
      const sweepAmount = roundAmount(Math.min(Math.max(0, freeNet), Math.max(0, sourceBalance)));

      cycle.pendingCardAmount = pendingCards;
      cycle.pendingDebtAmount = pendingDebts;
      cycle.freeNetAmount = roundAmount(Math.max(0, freeNet));
      cycle.sweptAmount = 0;

      if (sweepAmount > 0) {
        const sweepTransaction = createSweepTransaction(cycle, sweepAmount, settings);
        cycle.sweepTransferId = sweepTransaction.id;
        cycle.sweptAmount = sweepAmount;
        sweepTransactions.push(sweepTransaction);
        effectiveTransactions.push(sweepTransaction);
      }
    }

    return {
      cycles: updatedCycles,
      transactions: sortByDateAsc(effectiveTransactions),
      sweepTransactions
    };
  }

  function buildCycleSummaries(cycles, transactions, statements, accounts) {
    const balancesNow = calculateBalances(accounts, transactions, addDays(getToday(), 1));
    return cycles.map((cycle) => {
      const cycleTransactions = transactions.filter((item) => item.budgetCycleId === cycle.id);
      const income = cycleTransactions
        .filter((item) => item.type === 'income')
        .reduce((sum, item) => sum + item.amount, 0);
      const cashSpend = cycleTransactions
        .filter((item) => ['expense', 'debt_payment', 'goal_contribution'].includes(item.type))
        .reduce((sum, item) => sum + item.amount, 0);
      const cardAssigned = statements
        .filter((statement) => statement.budgetCycleId === cycle.id)
        .reduce((sum, statement) => sum + statement.chargedAmount, 0);
      const cardPending = statements
        .filter((statement) => statement.budgetCycleId === cycle.id)
        .reduce((sum, statement) => sum + statement.pendingAmount, 0);
      const cardPayments = cycleTransactions
        .filter((item) => item.type === 'card_payment')
        .reduce((sum, item) => sum + item.amount, 0);
      const freeNow = (cycle.liquidAccountIds || []).reduce((sum, accountId) => sum + (balancesNow[accountId] || 0), 0);
      return {
        ...cycle,
        income: roundAmount(income),
        cashSpend: roundAmount(cashSpend),
        cardAssigned: roundAmount(cardAssigned),
        cardPending: roundAmount(cardPending),
        cardPayments: roundAmount(cardPayments),
        freeLiquidNow: roundAmount(freeNow),
        transactionCount: cycleTransactions.length
      };
    });
  }

  function buildCardSummaries(cards, statements, cycles) {
    const today = getToday();
    return cards.map((card) => {
      const cardStatements = statements.filter((item) => item.cardId === card.id);
      const currentDebt = roundAmount(cardStatements.reduce((sum, statement) => sum + statement.pendingAmount, 0));
      const nextOpen = cardStatements.find((statement) => statement.pendingAmount > 0) || null;
      const fallbackStatement = [...cardStatements]
        .sort((a, b) => compareDate(b.closingDate || '0000-00-00', a.closingDate || '0000-00-00'))[0] || null;
      const currentStatement = nextOpen || fallbackStatement;
      const currentCycle = currentStatement?.budgetCycleId
        ? cycles.find((cycle) => cycle.id === currentStatement.budgetCycleId) || null
        : null;
      const availableCredit = card.creditLimit > 0
        ? roundAmount(card.creditLimit - currentDebt)
        : 0;
      const utilizationPct = card.creditLimit > 0
        ? roundAmount((currentDebt / card.creditLimit) * 100)
        : 0;
      const openingDebtPending = roundAmount(
        cardStatements
          .filter((statement) => statement.kind === 'opening-debt')
          .reduce((sum, statement) => sum + statement.pendingAmount, 0)
      );
      return {
        ...card,
        currentDebt,
        availableCredit,
        utilizationPct,
        openingDebtPending,
        openStatements: cardStatements.filter((item) => item.pendingAmount > 0).length,
        nextClosingDate: getUpcomingStatementClosingDate(today, card.closingDay),
        nextDueDate: nextOpen?.dueDate || getNextDueDate(getUpcomingStatementClosingDate(today, card.closingDay), card.dueDay),
        currentStatement,
        assignedCycle: currentCycle
      };
    });
  }

  function buildAgendaItems(cycles, cards, recurring, transactions, statements) {
    const today = getToday();
    const activeOrNextCycle = getRelevantCycle(cycles, today);
    const nextCycle = getNextCycle(cycles, activeOrNextCycle);
    const limit = nextCycle?.endDate || activeOrNextCycle?.endDate || addMonths(today, 1, 28);
    const items = [];

    cycles.forEach((cycle) => {
      const shouldShowCycle =
        (activeOrNextCycle && cycle.id === activeOrNextCycle.id) ||
        (compareDate(cycle.startDate, today) >= 0 && compareDate(cycle.startDate, limit) <= 0);
      if (shouldShowCycle) {
        items.push({
          id: `agenda-cycle-${cycle.id}`,
          date: cycle.startDate,
          kind: 'salary',
          title: 'Inicio de ciclo por sueldo',
          subtitle: `Ciclo ${cycle.startDate} - ${cycle.endDate}`
        });
      }
    });

    cards.forEach((card) => {
      const cardLabel = `${card.bankName} • ${card.last4}`;
      const closingDate = getUpcomingStatementClosingDate(today, card.closingDay);
      const dueDate = getNextDueDate(closingDate, card.dueDay);
      [closingDate, addMonths(closingDate, 1, card.closingDay)].forEach((date) => {
        if (compareDate(date, today) >= 0 && compareDate(date, limit) <= 0) {
          items.push({
            id: `agenda-card-close-${card.id}-${date}`,
            date,
            kind: 'card-close',
            title: `${cardLabel} corta`,
            subtitle: `Cierre de tarjeta ${card.bankName}`
          });
        }
      });
      [dueDate, addMonths(dueDate, 1, card.dueDay)].forEach((date) => {
        const hasPendingStatementThatDay = statements.some(
          (statement) => statement.cardId === card.id && statement.pendingAmount > 0 && statement.dueDate === date
        );
        if (compareDate(date, today) >= 0 && compareDate(date, limit) <= 0) {
          if (hasPendingStatementThatDay) return;
          items.push({
            id: `agenda-card-due-${card.id}-${date}`,
            date,
            kind: 'card-due',
            title: `${cardLabel} vence`,
            subtitle: 'Fecha de pago de tarjeta'
          });
        }
      });
    });

    recurring
      .filter((item) => item.active && !(item.isPrimarySalary && item.opensFinancialCycle))
      .forEach((item) => {
        const base = today.slice(0, 7) + '-01';
        [base, addMonths(base, 1, 1)].forEach((monthStart) => {
          const date = addMonths(monthStart, 0, item.dayOfMonth);
          if (item.startDate && compareDate(date, item.startDate) < 0) return;
          if (item.endMonth && getYearMonth(date) > item.endMonth) return;
          if (compareDate(date, today) >= 0 && compareDate(date, limit) <= 0) {
            items.push({
              id: `agenda-rec-${item.id}-${date}`,
              date,
              kind: item.type === 'income' ? 'income' : 'expense',
              title: item.description,
              subtitle: `Recurrente ${item.type === 'income' ? 'de ingreso' : 'de gasto'}`
            });
          }
        });
      });

    statements
      .filter((item) => item.pendingAmount > 0 && item.dueDate)
      .forEach((statement) => {
        if (compareDate(statement.dueDate, today) >= 0 && compareDate(statement.dueDate, limit) <= 0) {
          items.push({
            id: `agenda-statement-${statement.id}`,
            date: statement.dueDate,
            kind: 'statement',
            title: 'Pago pendiente de tarjeta',
            subtitle: `${statement.label} · S/ ${statement.pendingAmount.toFixed(2)}`
          });
        }
      });

    const seenInstallmentGroups = new Set();
    transactions
      .filter((item) => !item.autoGenerated && compareDate(item.date, today) >= 0 && compareDate(item.date, limit) <= 0)
      .forEach((item) => {
        if (item.type === 'card_charge' && item.installmentGroupId && item.installmentCount > 1) {
          if (seenInstallmentGroups.has(item.installmentGroupId)) return;
          seenInstallmentGroups.add(item.installmentGroupId);
          const card = cards.find((c) => c.id === item.cardId);
          const cardLabel = card ? `${card.bankName} • ${card.last4}` : 'Tarjeta';
          items.push({
            id: `agenda-tx-group-${item.installmentGroupId}`,
            date: item.date,
            kind: 'card-charge',
            title: `${item.description} (${item.installmentCount} cuotas)`,
            subtitle: `${cardLabel} · ${roundAmount(item.originalPurchaseAmount || item.amount * item.installmentCount).toFixed(2)}`
          });
          return;
        }
        if (item.type === 'card_charge') return;
        const typeLabels = { income: 'Ingreso', expense: 'Gasto', transfer: 'Transferencia', card_charge: 'Cargo tarjeta', card_payment: 'Pago tarjeta' };
        items.push({
          id: `agenda-tx-${item.id}`,
          date: item.date,
          kind: item.type === 'income' ? 'income' : 'expense',
          title: item.description,
          subtitle: typeLabels[item.type] || item.type
        });
      });

    return items
      .sort((a, b) => compareDate(a.date, b.date))
      .slice(0, 40)
      .map((item) => ({
        ...item,
        title: String(item.title || '').replace(/Â/g, '').replace(/â€¢/g, '•'),
        subtitle: String(item.subtitle || '').replace(/Â/g, '').replace(/â€¢/g, '•')
      }));
  }

  function buildAlerts(settings, cycles, cards, debts, statements) {
    const alerts = [];
    const today = getToday();
    const currentCycle = getRelevantCycle(cycles, today);

    if (!settings.financialCycleConfig.primarySalaryRecurringId) {
      alerts.push({
        kind: 'warning',
        title: 'Falta configurar el sueldo principal',
        text: 'Elige un recurrente de ingreso como sueldo principal para activar los ciclos.'
      });
    }

    if (!settings.financialCycleConfig.savingsAccountId) {
      alerts.push({
        kind: 'info',
        title: 'Sin cuenta de ahorro para barrido',
        text: 'Configura una cuenta de ahorro destino para mover el sobrante automaticamente.'
      });
    }

    cards.forEach((card) => {
      const cardLabel = `${card.bankName} • ${card.last4}`;
      const nextClose = getUpcomingStatementClosingDate(today, card.closingDay);
      const nextDue = getNextDueDate(nextClose, card.dueDay);
      if (compareDate(nextClose, addDays(today, 3)) <= 0) {
        alerts.push({
          kind: 'info',
          title: `${cardLabel} corta pronto`,
          text: `Su siguiente corte es el ${nextClose}.`
        });
      }
      if (compareDate(nextDue, addDays(today, 5)) <= 0) {
        alerts.push({
          kind: 'warning',
          title: `${cardLabel} vence pronto`,
          text: `Revisa el pago del estado con vencimiento ${nextDue}.`
        });
      }
    });

    debts
      .filter((debt) => !debt.archived && !debt.legacyMigratedToCardId && debt.outstandingAmount > 0)
      .forEach((debt) => {
        const dueDate = addMonths(today.slice(0, 7) + '-01', 0, debt.dueDay);
        if (compareDate(dueDate, addDays(today, 5)) <= 0) {
          alerts.push({
            kind: 'warning',
            title: `${debt.name} vence pronto`,
            text: `Saldo pendiente ${debt.outstandingAmount.toFixed(2)}.`
          });
        }
      });

    if (currentCycle) {
      const pendingStatements = statements
        .filter((item) => item.budgetCycleId === currentCycle.id && item.pendingAmount > 0)
        .reduce((sum, item) => sum + item.pendingAmount, 0);
      if (pendingStatements > 0) {
        alerts.push({
          kind: 'info',
          title: 'Este sueldo aun cubre compras de tarjeta',
          text: `Tienes S/ ${roundAmount(pendingStatements).toFixed(2)} pendientes en tarjetas asignadas al ciclo actual.`
        });
      }
    }

    return alerts.slice(0, 8);
  }

  async function syncFinanceEngine() {
    await initDB();

    const [rawSettings, rawCategories, rawAccounts, rawDebts, rawGoals, rawRecurring, rawCards, rawTransactions] =
      await Promise.all([
        getOne(STORE_NAMES.settings, SETTINGS_KEY),
        getAll(STORE_NAMES.categories),
        getAll(STORE_NAMES.accounts),
        getAll(STORE_NAMES.debts),
        getAll(STORE_NAMES.goals),
        getAll(STORE_NAMES.recurring),
        getAll(STORE_NAMES.cards),
        getAll(STORE_NAMES.transactions)
      ]);

    let settings = normalizeSettings(rawSettings || DEFAULT_SETTINGS);
    const categories = rawCategories.map((item) => normalizeCategory(item));
    const accounts = rawAccounts.map((item) => normalizeAccount(item, item));
    const goals = rawGoals.map((item) => normalizeGoal(item, item));
    let recurring = rawRecurring.map((item) => normalizeRecurring(item, item));
    const debts = rawDebts.map((item) => normalizeDebt(item, item));
    const existingCards = rawCards.map((item) => normalizeCard(item, item));
    const manualTransactions = rawTransactions.map((item) => normalizeTransaction(item, item));

    let cards = [...existingCards];
    const debtsToPersist = debts.map((debt) => ({ ...debt }));

    debtsToPersist
      .filter((debt) => debt.kind === 'credit_card' && !debt.legacyMigratedToCardId)
      .forEach((debt) => {
        const card = normalizeCard({
          bankName: debt.name || 'Pendiente',
          last4: '0000',
          closingDay: debt.closingDay || 10,
          dueDay: debt.dueDay || 25,
          paymentAccountId: debt.accountId || settings.financialCycleConfig.sweepSourceAccountId,
          needsReview: true,
          creditLimit: 0,
          openingDebtAmount: debt.outstandingAmount
        });
        cards.push(card);
        debt.legacyMigratedToCardId = card.id;
        debt.archived = true;
      });

    recurring = recurring.map((item) => {
      if (item.accountId) return item;
      if (item.type !== 'income') return item;
      return {
        ...item,
        accountId: settings.financialCycleConfig.sweepSourceAccountId || 'bank-main'
      };
    });

    const primarySalaryRecurring = choosePrimarySalaryRecurring(recurring, settings);
    if (primarySalaryRecurring && settings.financialCycleConfig.primarySalaryRecurringId !== primarySalaryRecurring.id) {
      settings.financialCycleConfig.primarySalaryRecurringId = primarySalaryRecurring.id;
    }

    const recurringTransactions = materializeRecurringTransactions(recurring, manualTransactions, settings);
    const initialCycles = buildFinancialCycles(primarySalaryRecurring, recurringTransactions, settings);
    const transactionsWithCycles = assignBudgetCycles(recurringTransactions, cards, initialCycles);
    const { cycles, transactions } = buildCyclesWithSweeps(
      initialCycles,
      transactionsWithCycles,
      accounts,
      cards,
      debtsToPersist,
      settings
    );
    const finalTransactions = assignBudgetCycles(transactions, cards, cycles).map((item) => normalizeTransaction(item, item));

    await Promise.all([
      replaceStore(STORE_NAMES.settings, [settings]),
      replaceStore(STORE_NAMES.categories, categories),
      replaceStore(STORE_NAMES.accounts, accounts),
      replaceStore(STORE_NAMES.goals, goals),
      replaceStore(STORE_NAMES.recurring, recurring),
      replaceStore(STORE_NAMES.debts, debtsToPersist),
      replaceStore(STORE_NAMES.cards, cards),
      replaceStore(STORE_NAMES.transactions, finalTransactions),
      replaceStore(STORE_NAMES.financialCycles, cycles)
    ]);
  }

  async function getSettings() {
    await initDB();
    return normalizeSettings(await getOne(STORE_NAMES.settings, SETTINGS_KEY));
  }

  async function saveSettings(patch = {}) {
    const current = await getSettings();
    const next = normalizeSettings({
      ...current,
      ...patch,
      financialCycleConfig: {
        ...current.financialCycleConfig,
        ...parseJson(patch.financialCycleConfig, {})
      }
    });
    await putMany(STORE_NAMES.settings, [next]);
    await syncFinanceEngine();
    return next;
  }

  async function configureFinancialCycle(configPatch = {}) {
    const settings = await getSettings();
    return saveSettings({
      financialCycleConfig: {
        ...settings.financialCycleConfig,
        ...configPatch
      }
    });
  }

  async function getCategories() {
    await initDB();
    return (await getAll(STORE_NAMES.categories)).map((item) => normalizeCategory(item));
  }

  async function getAccounts() {
    await initDB();
    return (await getAll(STORE_NAMES.accounts)).map((item) => normalizeAccount(item, item));
  }

  async function getGoals() {
    await initDB();
    return (await getAll(STORE_NAMES.goals)).map((item) => normalizeGoal(item, item));
  }

  async function getDebts() {
    await initDB();
    return (await getAll(STORE_NAMES.debts)).map((item) => normalizeDebt(item, item));
  }

  async function getRecurring() {
    await initDB();
    return (await getAll(STORE_NAMES.recurring)).map((item) => normalizeRecurring(item, item));
  }

  async function getCards() {
    await initDB();
    return (await getAll(STORE_NAMES.cards)).map((item) => normalizeCard(item, item));
  }

  async function getTransactions() {
    await initDB();
    return sortByDateAsc((await getAll(STORE_NAMES.transactions)).map((item) => normalizeTransaction(item, item)));
  }

  async function getFinancialCycles() {
    await initDB();
    return sortByDateAsc((await getAll(STORE_NAMES.financialCycles)).map((item) => normalizeCycle(item, item)));
  }

  async function saveAccount(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.accounts, payload.id) : null;
    const record = normalizeAccount(payload, existing);
    await putMany(STORE_NAMES.accounts, [record]);
    await syncFinanceEngine();
    return record;
  }

  async function saveGoal(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.goals, payload.id) : null;
    const record = normalizeGoal(payload, existing);
    await putMany(STORE_NAMES.goals, [record]);
    await syncFinanceEngine();
    return record;
  }

  async function saveDebt(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.debts, payload.id) : null;
    const record = normalizeDebt(payload, existing);
    await putMany(STORE_NAMES.debts, [record]);
    await syncFinanceEngine();
    return record;
  }

  async function saveRecurring(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.recurring, payload.id) : null;
    const record = normalizeRecurring(payload, existing);
    await putMany(STORE_NAMES.recurring, [record]);
    if (record.isPrimarySalary) {
      const recurring = await getRecurring();
      const updates = recurring
        .filter((item) => item.id !== record.id && item.isPrimarySalary)
        .map((item) => ({ ...item, isPrimarySalary: false, opensFinancialCycle: false }));
      if (updates.length) {
        await putMany(STORE_NAMES.recurring, updates);
      }
      await configureFinancialCycle({
        primarySalaryRecurringId: record.id,
        onboardingCompleted: true
      });
      return record;
    }
    await syncFinanceEngine();
    return record;
  }

  async function saveCard(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.cards, payload.id) : null;
    const record = normalizeCard(payload, existing);
    if (!existing && record.creditLimit <= 0) {
      throw new Error('Define la linea total de la tarjeta para crearla.');
    }
    await putMany(STORE_NAMES.cards, [record]);
    await syncFinanceEngine();
    return record;
  }

  function validateTransaction(record) {
    if (!record.amount || record.amount <= 0) {
      throw new Error('El monto debe ser mayor que cero.');
    }
    if (!record.description) {
      throw new Error('Agrega una descripcion para identificar el movimiento.');
    }
    if (record.type === 'expense' && !record.fromAccountId) {
      throw new Error('El gasto necesita una cuenta origen.');
    }
    if (record.type === 'income' && !record.toAccountId) {
      throw new Error('El ingreso necesita una cuenta destino.');
    }
    if (record.type === 'transfer' && (!record.fromAccountId || !record.toAccountId)) {
      throw new Error('La transferencia necesita cuenta origen y destino.');
    }
    if (record.type === 'debt_payment' && (!record.fromAccountId || !record.linkedEntityId)) {
      throw new Error('El pago de deuda necesita una cuenta origen y una deuda.');
    }
    if (record.type === 'goal_contribution' && !record.fromAccountId) {
      throw new Error('El aporte necesita una cuenta origen.');
    }
    if (record.type === 'card_charge' && !record.cardId) {
      throw new Error('La compra con tarjeta necesita una tarjeta.');
    }
    if (record.type === 'card_charge' && (!record.installmentCount || record.installmentCount < 1)) {
      throw new Error('La compra con tarjeta necesita al menos una cuota.');
    }
    if (record.type === 'card_charge' && record.installmentCount > Math.round(record.amount * 100)) {
      throw new Error('El monto es demasiado pequeno para repartirlo en tantas cuotas.');
    }
    if (record.type === 'card_payment' && (!record.cardId || !record.fromAccountId)) {
      throw new Error('El pago de tarjeta necesita tarjeta y cuenta pagadora.');
    }
  }

  async function saveTransaction(payload) {
    await initDB();
    const existing = payload.id ? await getOne(STORE_NAMES.transactions, payload.id) : null;
    if (existing?.systemTag === 'cycle_sweep' && payload.systemTag !== 'cycle_sweep') {
      throw new Error('Los movimientos automaticos de barrido no se editan manualmente.');
    }
    const record = normalizeTransaction(payload, existing);
    if (existing && record.type === 'card_charge' && record.installmentCount > 1) {
      throw new Error('Las compras en cuotas se corrigen eliminando toda la compra y registrandola de nuevo.');
    }
    validateTransaction(record);
    if (!existing && record.type === 'card_charge' && record.installmentCount > 1) {
      const cards = await getCards();
      const card = cards.find((item) => item.id === record.cardId);
      if (!card) {
        throw new Error('La tarjeta elegida no existe.');
      }
      await putMany(STORE_NAMES.transactions, buildInstallmentTransactions(record, card));
    } else {
      await putMany(STORE_NAMES.transactions, [record]);
    }
    await syncFinanceEngine();
    return record;
  }

  async function deleteTransaction(id) {
    await initDB();
    const existing = await getOne(STORE_NAMES.transactions, id);
    if (!existing) return;
    if (existing.systemTag === 'cycle_sweep') {
      throw new Error('El barrido automatico se recalcula solo.');
    }
    if (existing.installmentGroupId) {
      const groupedTransactions = (await getTransactions()).filter(
        (item) => item.installmentGroupId === existing.installmentGroupId
      );
      if (groupedTransactions.length <= 1) {
        await deleteOne(STORE_NAMES.transactions, id);
        await syncFinanceEngine();
        return;
      }
      const db = await getDB();
      const transaction = db.transaction(STORE_NAMES.transactions, 'readwrite');
      groupedTransactions.forEach((item) => transaction.objectStore(STORE_NAMES.transactions).delete(item.id));
      await transactionToPromise(transaction);
      await syncFinanceEngine();
      return;
    }
    await deleteOne(STORE_NAMES.transactions, id);
    await syncFinanceEngine();
  }

  async function deleteRecurring(id) {
    await initDB();
    await deleteOne(STORE_NAMES.recurring, id);
    const transactions = await getTransactions();
    const futureTransactions = transactions.filter(
      (item) =>
        item.linkedEntityType === 'recurring' &&
        item.linkedEntityId === id &&
        compareDate(item.date, getToday()) >= 0 &&
        item.autoGenerated
    );
    if (futureTransactions.length) {
      const db = await getDB();
      const transaction = db.transaction(STORE_NAMES.transactions, 'readwrite');
      futureTransactions.forEach((item) => transaction.objectStore(STORE_NAMES.transactions).delete(item.id));
      await transactionToPromise(transaction);
    }
    await syncFinanceEngine();
  }

  async function deleteAccount(id) {
    await initDB();
    await deleteOne(STORE_NAMES.accounts, id);
    await syncFinanceEngine();
  }

  async function deleteGoal(id) {
    await initDB();
    await deleteOne(STORE_NAMES.goals, id);
    await syncFinanceEngine();
  }

  async function deleteDebt(id) {
    await initDB();
    await deleteOne(STORE_NAMES.debts, id);
    await syncFinanceEngine();
  }

  async function deleteCard(id) {
    await initDB();
    const transactions = await getTransactions();
    if (transactions.some((item) => item.cardId === id)) {
      throw new Error('No puedes eliminar una tarjeta con movimientos. Archivala o limpia primero sus compras y pagos.');
    }
    await deleteOne(STORE_NAMES.cards, id);
    await syncFinanceEngine();
  }

  async function exportBackup() {
    await syncFinanceEngine();
    const [settings, categories, accounts, transactions, debts, goals, recurring, cards, cycles] = await Promise.all([
      getSettings(),
      getCategories(),
      getAccounts(),
      getTransactions(),
      getDebts(),
      getGoals(),
      getRecurring(),
      getCards(),
      getFinancialCycles()
    ]);

    return {
      version: 1,
      exportedAt: new Date().toISOString(),
      settings,
      categories,
      accounts,
      transactions,
      debts,
      goals,
      recurring,
      cards,
      financial_cycles: cycles
    };
  }

  async function importBackup(payload) {
    const parsed = typeof payload === 'string' ? JSON.parse(payload) : payload;
    if (!parsed || typeof parsed !== 'object') {
      throw new Error('El backup no tiene un formato valido.');
    }

    await Promise.all(Object.values(STORE_NAMES).map((name) => clearStore(name)));
    await putMany(STORE_NAMES.settings, [normalizeSettings(parsed.settings || DEFAULT_SETTINGS)]);
    await putMany(STORE_NAMES.categories, (parsed.categories || DEFAULT_CATEGORIES).map((item) => normalizeCategory(item)));
    await putMany(STORE_NAMES.accounts, (parsed.accounts || DEFAULT_ACCOUNTS).map((item) => normalizeAccount(item, item)));
    await putMany(STORE_NAMES.goals, (parsed.goals || []).map((item) => normalizeGoal(item, item)));
    await putMany(STORE_NAMES.debts, (parsed.debts || []).map((item) => normalizeDebt(item, item)));
    await putMany(STORE_NAMES.recurring, (parsed.recurring || []).map((item) => normalizeRecurring(item, item)));
    await putMany(STORE_NAMES.cards, (parsed.cards || []).map((item) => normalizeCard(item, item)));
    await putMany(STORE_NAMES.transactions, (parsed.transactions || []).map((item) => normalizeTransaction(item, item)));
    if (Array.isArray(parsed.financial_cycles)) {
      await putMany(STORE_NAMES.financialCycles, parsed.financial_cycles.map((item) => normalizeCycle(item, item)));
    }
    await syncFinanceEngine();
  }

  async function clearAllData() {
    await Promise.all(Object.values(STORE_NAMES).map((name) => clearStore(name)));
    await seedDefaults();
    await syncFinanceEngine();
  }

  async function getFinanceSnapshot() {
    await syncFinanceEngine();
    const [settings, categories, accounts, debts, goals, recurring, cards, transactions, cycles] = await Promise.all([
      getSettings(),
      getCategories(),
      getAccounts(),
      getDebts(),
      getGoals(),
      getRecurring(),
      getCards(),
      getTransactions(),
      getFinancialCycles()
    ]);

    const today = getToday();
    const statements = deriveStatements(cards, transactions, cycles);
    const cycleSummaries = buildCycleSummaries(cycles, transactions, statements, accounts)
      .sort((a, b) => compareDate(b.startDate, a.startDate));
    const currentCycle = getRelevantCycle(cycleSummaries, today);
    const balances = calculateBalances(accounts, transactions, addDays(getToday(), 1));
    const cardSummaries = buildCardSummaries(cards, statements, cycles);
    const agenda = buildAgendaItems(cycles, cards, recurring, transactions, statements);
    const alerts = buildAlerts(settings, cycleSummaries, cardSummaries, debts, statements);
    const projectionByCycle = cycleSummaries.reduce((acc, cycle) => {
      if (compareDate(cycle.endDate, today) < 0) {
        acc[cycle.id] = {
          showProjection: false,
          cutoffDate: cycle.endDate,
          balances: {},
          visibleNetWorth: 0
        };
        return acc;
      }
      const projectedBalances = calculateBalances(accounts, transactions, addDays(cycle.endDate, 1));
      const visibleNetWorth = accounts
        .filter((account) => account.includeInNetWorth && !account.archived)
        .reduce((sum, account) => sum + (projectedBalances[account.id] || 0), 0);
      acc[cycle.id] = {
        showProjection: true,
        cutoffDate: cycle.endDate,
        balances: projectedBalances,
        visibleNetWorth: roundAmount(visibleNetWorth)
      };
      return acc;
    }, {});

    return {
      settings,
      categories,
      accounts,
      balances,
      debts: debts.filter((item) => !item.legacyMigratedToCardId),
      goals,
      recurring,
      cards: cardSummaries,
      transactions: sortByDateAsc(transactions).reverse(),
      cycles: cycleSummaries,
      currentCycleId: currentCycle?.id || '',
      currentCycle,
      projectionByCycle,
      statements,
      agenda,
      alerts
    };
  }

  async function applyRecurringForMonth() {
    await syncFinanceEngine();
  }

  function getCategorySuggestions(type = 'expense') {
    const desired = type === 'income' ? 'income' : 'expense';
    return DEFAULT_CATEGORIES.filter((item) => item.type === desired);
  }

  return {
    initDB,
    syncFinanceEngine,
    applyRecurringForMonth,
    getFinanceSnapshot,
    getSettings,
    saveSettings,
    configureFinancialCycle,
    getCategories,
    getAccounts,
    getGoals,
    getDebts,
    getRecurring,
    getCards,
    getTransactions,
    getFinancialCycles,
    saveAccount,
    saveGoal,
    saveDebt,
    saveRecurring,
    saveCard,
    saveTransaction,
    deleteTransaction,
    deleteRecurring,
    deleteAccount,
    deleteGoal,
    deleteDebt,
    deleteCard,
    exportBackup,
    importBackup,
    clearAllData,
    getCategorySuggestions,
    roundAmount,
    normalizeDate,
    getToday,
    addDays,
    addMonths,
    compareDate,
    getStatementClosingDate,
    getNextDueDate
  };
})();
