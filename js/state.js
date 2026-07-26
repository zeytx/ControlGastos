/* Estado en memoria y selectores basicos sobre el snapshot. */

export const state = {
  snapshot: null,
  selectedCycleId: '',
  filters: {
    search: '',
    type: 'all',
    accountId: 'all',
    categoryId: 'all',
    cardId: 'all'
  },
  ui: {
    detailTransactionId: '',
    pendingConfirm: null
  },
  storage: {
    persisted: null,
    estimate: null
  },
  platform: {
    isIOS: false,
    standalone: false
  }
};

export function getSnapshot() {
  return state.snapshot || {
    settings: { financialCycleConfig: {} },
    categories: [],
    accounts: [],
    balances: {},
    debts: [],
    goals: [],
    recurring: [],
    cards: [],
    transactions: [],
    cycles: [],
    currentCycleId: '',
    currentCycle: null,
    projectionByCycle: {},
    statements: [],
    agenda: [],
    alerts: []
  };
}

export function getSelectedCycle() {
  const snapshot = getSnapshot();
  return snapshot.cycles.find((cycle) => cycle.id === state.selectedCycleId) || snapshot.currentCycle || null;
}

export function getSelectedCycleId() {
  return getSelectedCycle()?.id || '';
}

export function getAccountMap() {
  return new Map(getSnapshot().accounts.map((item) => [item.id, item]));
}

export function getCategoryMap() {
  return new Map(getSnapshot().categories.map((item) => [item.id, item]));
}

export function getCardMap() {
  return new Map(getSnapshot().cards.map((item) => [item.id, item]));
}

export function getDebtMap() {
  return new Map(getSnapshot().debts.map((item) => [item.id, item]));
}

export function getGoalMap() {
  return new Map(getSnapshot().goals.map((item) => [item.id, item]));
}

export function getRecurringMap() {
  return new Map(getSnapshot().recurring.map((item) => [item.id, item]));
}

export function getTransactionById(id) {
  return getSnapshot().transactions.find((item) => item.id === id) || null;
}

export function getCardStatements(cardId) {
  return getSnapshot().statements
    .filter((item) => item.cardId === cardId)
    .sort((a, b) => String(b.closingDate || '').localeCompare(String(a.closingDate || '')));
}

export function getOpenStatementsForCard(cardId) {
  return getCardStatements(cardId).filter((item) => item.pendingAmount > 0);
}

export function getSelectedCycleTransactions() {
  const cycleId = getSelectedCycleId();
  const snapshot = getSnapshot();
  if (!cycleId) {
    return [...snapshot.transactions].sort((a, b) => FinanceDB.compareDate(a.date, b.date));
  }
  return snapshot.transactions
    .filter((item) => item.budgetCycleId === cycleId)
    .sort((a, b) => FinanceDB.compareDate(a.date, b.date));
}

export function getTodayDate() {
  return FinanceDB.getToday();
}
