/* Calculos derivados que alimentan el dashboard y los filtros. */
import { TYPE_LABELS } from './constants.js';
import { $ } from './dom.js';
import { formatCurrency, formatLongDate, formatShortDate, getAccountLabel, getCardLabel, getCategoryLabel } from './format.js';
import { getSelectedCycle, getSelectedCycleId, getSelectedCycleTransactions, getSnapshot, state } from './state.js';

export function getAccountProjectionView() {
  const cycleId = getSelectedCycleId();
  const projection = getSnapshot().projectionByCycle?.[cycleId] || null;
  if (!projection?.showProjection) {
    return {
      showProjection: false,
      label: '',
      balances: {},
      visibleNetWorth: 0
    };
  }

  return {
    showProjection: true,
    label: `Proyeccion al ${formatShortDate(projection.cutoffDate)}`,
    balances: projection.balances || {},
    visibleNetWorth: projection.visibleNetWorth || 0
  };
}

export function sumAmounts(items, predicate) {
  return FinanceDB.roundAmount(
    items.reduce((sum, item) => sum + (predicate(item) ? item.amount : 0), 0)
  );
}

export function getDashboardMetrics() {
  const cycle = getSelectedCycle();
  const transactions = getSelectedCycleTransactions();
  const cashSpend = sumAmounts(transactions, (item) => ['expense', 'debt_payment'].includes(item.type));
  const savingsFlow = FinanceDB.roundAmount(
    sumAmounts(transactions, (item) => item.type === 'goal_contribution') + (cycle?.sweptAmount || 0)
  );
  const cardCommitted = FinanceDB.roundAmount(cycle?.cardAssigned || 0);
  const totalCommitted = FinanceDB.roundAmount(cashSpend + savingsFlow + cardCommitted);
  const freeNet = FinanceDB.roundAmount(cycle?.freeNetAmount ?? 0);

  return {
    cycle,
    transactions,
    cashSpend,
    savingsFlow,
    cardCommitted,
    totalCommitted,
    freeNet,
    transactionCount: transactions.length
  };
}

export function getCategoryBreakdown() {
  const transactions = getSelectedCycleTransactions().filter((item) =>
    ['expense', 'card_charge', 'debt_payment', 'goal_contribution'].includes(item.type)
  );

  const totals = new Map();
  transactions.forEach((transaction) => {
    const fallbackLabel =
      transaction.type === 'debt_payment'
        ? 'Pago de deuda'
        : transaction.type === 'goal_contribution'
          ? 'Ahorro'
          : transaction.type === 'card_charge'
            ? 'Compras con tarjeta'
            : 'Otros gastos';
    const label = getCategoryLabel(transaction.categoryId) || fallbackLabel;
    totals.set(label, FinanceDB.roundAmount((totals.get(label) || 0) + transaction.amount));
  });

  const total = Array.from(totals.values()).reduce((sum, value) => sum + value, 0);
  return Array.from(totals.entries())
    .map(([label, amount]) => ({
      label,
      amount,
      ratio: total > 0 ? amount / total : 0
    }))
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 6);
}

export function getCycleTrendPoints() {
  const cycle = getSelectedCycle();
  if (!cycle) return [];

  const relevantTransactions = getSelectedCycleTransactions().filter(
    (item) =>
      FinanceDB.compareDate(item.date, cycle.startDate) >= 0 &&
      FinanceDB.compareDate(item.date, cycle.endDate) <= 0 &&
      ['income', 'expense', 'debt_payment', 'goal_contribution', 'card_payment', 'card_charge'].includes(item.type)
  );

  const deltas = new Map();
  relevantTransactions.forEach((item) => {
    const sign = item.type === 'income' ? 1 : -1;
    deltas.set(item.date, FinanceDB.roundAmount((deltas.get(item.date) || 0) + item.amount * sign));
  });

  const points = [];
  let cursor = cycle.startDate;
  let running = 0;
  while (FinanceDB.compareDate(cursor, cycle.endDate) <= 0) {
    running = FinanceDB.roundAmount(running + (deltas.get(cursor) || 0));
    points.push({ date: cursor, value: running });
    cursor = FinanceDB.addDays(cursor, 1);
  }
  return points;
}

export function getCardCoverageSummary() {
  const cycleId = getSelectedCycleId();
  const statements = getSnapshot().statements.filter((item) => item.budgetCycleId === cycleId);
  const assignedAmount = FinanceDB.roundAmount(
    statements.reduce((sum, statement) => sum + statement.chargedAmount, 0)
  );
  const paidAmount = FinanceDB.roundAmount(
    statements.reduce((sum, statement) => sum + statement.paidAmount, 0)
  );
  const pendingAmount = FinanceDB.roundAmount(
    statements.reduce((sum, statement) => sum + statement.pendingAmount, 0)
  );
  const ratio = assignedAmount > 0 ? Math.min(1, paidAmount / assignedAmount) : 0;

  return {
    assignedAmount,
    paidAmount,
    pendingAmount,
    ratio,
    items: statements
      .filter((statement) => statement.pendingAmount > 0)
      .sort((a, b) => FinanceDB.compareDate(a.dueDate || '9999-12-31', b.dueDate || '9999-12-31'))
      .slice(0, 4)
  };
}

export function getUpcomingTimelinePreview() {
  return (getSnapshot().agenda || []).slice(0, 4);
}

export function getFilteredTransactions() {
  const snapshot = getSnapshot();
  const cycleId = getSelectedCycleId();
  const search = state.filters.search.trim().toLowerCase();

  return snapshot.transactions
    .filter((item) => {
      const matchesCycle = cycleId ? item.budgetCycleId === cycleId : true;
      const matchesType = state.filters.type === 'all' || item.type === state.filters.type;
      const matchesAccount =
        state.filters.accountId === 'all' ||
        item.fromAccountId === state.filters.accountId ||
        item.toAccountId === state.filters.accountId;
      const matchesCard = state.filters.cardId === 'all' || item.cardId === state.filters.cardId;
      const matchesCategory = state.filters.categoryId === 'all' || item.categoryId === state.filters.categoryId;
      const haystack = [
        item.description,
        item.notes,
        item.sourceType,
        getCardLabel(item.cardId),
        getCategoryLabel(item.categoryId),
        getAccountLabel(item.fromAccountId),
        getAccountLabel(item.toAccountId),
        TYPE_LABELS[item.type] || item.type,
        FinanceDB.roundAmount(item.amount).toFixed(2)
      ].join(' ').toLowerCase();
      const matchesSearch = !search || haystack.includes(search);
      return matchesCycle && matchesType && matchesAccount && matchesCard && matchesCategory && matchesSearch;
    })
    .sort((a, b) => FinanceDB.compareDate(a.date, b.date));
}

export function buildBackupAlert() {
  const settings = getSnapshot().settings;
  const lastBackupAt = settings.lastBackupAt || '';
  const txCount = getSnapshot().transactions.length;

  if (!lastBackupAt && txCount > 5) {
    return {
      kind: 'warning',
      title: 'Aun no has hecho tu primer backup',
      text: `Tienes ${txCount} movimientos que se perderan si borras los datos del navegador. Ve a Ajustes > Exportar JSON.`
    };
  }

  if (lastBackupAt) {
    const lastDate = new Date(lastBackupAt);
    const now = new Date();
    const daysSince = Math.floor((now - lastDate) / 86400000);
    if (daysSince > 7) {
      return {
        kind: 'info',
        title: `Han pasado ${daysSince} dias desde tu ultimo backup`,
        text: `Ultimo backup: ${formatLongDate(lastBackupAt.slice(0, 10))}. Exporta un JSON nuevo para proteger tus datos.`
      };
    }
  }

  return null;
}

export function buildBudgetAlert() {
  const budget = getSnapshot().budget;
  if (!budget?.enabled || !budget.overspent) return null;
  return {
    kind: 'warning',
    title: 'Te pasaste del tope de este ciclo',
    text: `Llevas ${formatCurrency(budget.spent)} de ${formatCurrency(budget.limit)}, contando compras con tarjeta asignadas a este sueldo.`
  };
}

export function buildUnassignedChargesAlert() {
  const count = getSnapshot().unassignedCardCharges || 0;
  if (!count) return null;
  return {
    kind: 'info',
    title: `${count} cargo${count === 1 ? '' : 's'} de tarjeta sin sueldo asignado`,
    text: 'Son compras que cierran despues del ultimo sueldo proyectado. Se asignaran solas cuando ese sueldo exista.'
  };
}
