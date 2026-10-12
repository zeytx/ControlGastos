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
      title: 'Guarda una copia de tus datos',
      text: `Tienes ${txCount} movimientos solo en este celular. Ve a Ajustes > Guardar copia y mándala a tu Drive o WhatsApp.`
    };
  }

  if (lastBackupAt) {
    const lastDate = new Date(lastBackupAt);
    const now = new Date();
    const daysSince = Math.floor((now - lastDate) / 86400000);
    if (daysSince > 7) {
      return {
        kind: 'info',
        title: `Tu última copia tiene ${daysSince} días`,
        text: `Es del ${formatLongDate(lastBackupAt.slice(0, 10))}. Guarda una nueva en Ajustes > Guardar copia.`
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
    text: 'Son compras que cierran después del último sueldo proyectado. Se asignarán solas cuando ese sueldo exista.'
  };
}

/* ===== Resumen de Inicio: "cuanto me queda" en lenguaje simple ===== */

function isConsumption(item) {
  return item.type === 'expense' || item.type === 'card_charge';
}

function consumptionDate(item) {
  return item.type === 'card_charge' ? item.purchaseDate || item.date : item.date;
}

function amountInBase(item) {
  const snapshot = getSnapshot();
  if (item.type === 'card_charge') return item.amount;
  const accountId = item.type === 'income' ? item.toAccountId : item.fromAccountId;
  const currency = snapshot.accounts.find((account) => account.id === accountId)?.currency || 'PEN';
  return FinanceDB.convertToBase(item.amount, currency, snapshot.settings);
}

function isWithin(date, startDate, endDate) {
  return FinanceDB.compareDate(date, startDate) >= 0 && FinanceDB.compareDate(date, endDate) <= 0;
}

export function sumConsumption(startDate, endDate) {
  return FinanceDB.roundAmount(
    getSnapshot().transactions
      .filter((item) => isConsumption(item) && isWithin(consumptionDate(item), startDate, endDate))
      .reduce((sum, item) => sum + amountInBase(item), 0)
  );
}

function sumIncome(startDate, endDate) {
  return FinanceDB.roundAmount(
    getSnapshot().transactions
      .filter((item) => item.type === 'income' && isWithin(item.date, startDate, endDate))
      .reduce((sum, item) => sum + amountInBase(item), 0)
  );
}

function daysBetween(startDate, endDate) {
  const start = new Date(`${startDate}T12:00:00`);
  const end = new Date(`${endDate}T12:00:00`);
  return Math.round((end - start) / 86400000);
}

/** Ciclo de sueldo vigente; si no hay sueldo configurado, el mes calendario. */
export function getCurrentPeriod() {
  const snapshot = getSnapshot();
  const today = FinanceDB.getToday();
  const cycle = snapshot.currentCycle;
  if (cycle && isWithin(today, cycle.startDate, cycle.endDate)) {
    const index = snapshot.cycles.findIndex((item) => item.id === cycle.id);
    const previous = index >= 0 ? snapshot.cycles[index + 1] || null : null;
    return {
      kind: 'cycle',
      startDate: cycle.startDate,
      endDate: cycle.endDate,
      cycle,
      previousStart: previous?.startDate || '',
      previousEnd: previous?.endDate || ''
    };
  }
  const startDate = `${today.slice(0, 7)}-01`;
  const endDate = FinanceDB.addDays(FinanceDB.addMonths(startDate, 1, 1), -1);
  const previousStart = FinanceDB.addMonths(startDate, -1, 1);
  return {
    kind: 'month',
    startDate,
    endDate,
    cycle: null,
    previousStart,
    previousEnd: FinanceDB.addDays(startDate, -1)
  };
}

export function getHomeSummary() {
  const snapshot = getSnapshot();
  const today = FinanceDB.getToday();
  const period = getCurrentPeriod();
  const spentSoFar = sumConsumption(period.startDate, today);
  const spentToday = sumConsumption(today, today);
  const incomeSoFar = sumIncome(period.startDate, today);
  const daysLeft = Math.max(1, daysBetween(today, period.endDate) + 1);
  const elapsedDays = Math.max(0, daysBetween(period.startDate, today));
  const totalDays = Math.max(1, daysBetween(period.startDate, period.endDate) + 1);

  // Que numero grande mostrar: tope > sobrante del ciclo > balance del mes.
  const monthlyBudget = FinanceDB.roundAmount(snapshot.settings.monthlyBudget || 0);
  let headline;
  if (monthlyBudget > 0) {
    const engineBudget = period.kind === 'cycle' && snapshot.budget?.enabled && snapshot.budget.limit > 0 ? snapshot.budget : null;
    const spent = engineBudget ? engineBudget.spent : spentSoFar;
    const remaining = FinanceDB.roundAmount(monthlyBudget - spent);
    headline = {
      kind: 'budget',
      label: remaining >= 0 ? 'Te queda de tu tope' : 'Te pasaste de tu tope',
      amount: Math.abs(remaining),
      tone: remaining < 0 ? 'danger' : remaining < monthlyBudget * 0.15 ? 'warning' : 'positive',
      ratio: Math.min(1, spent / monthlyBudget),
      detail: `${formatCurrency(spent)} de ${formatCurrency(monthlyBudget)}`,
      available: Math.max(0, remaining)
    };
  } else if (period.kind === 'cycle') {
    const free = FinanceDB.roundAmount(period.cycle.freeNetAmount ?? 0);
    headline = {
      kind: 'cycle',
      label: free >= 0 ? 'Te queda libre hasta tu sueldo' : 'Te falta para cubrir tus pagos',
      amount: Math.abs(free),
      tone: free < 0 ? 'danger' : 'positive',
      ratio: null,
      detail: 'Ya descontando tarjetas y deudas de este sueldo',
      available: Math.max(0, free)
    };
  } else if (incomeSoFar > 0) {
    const balance = FinanceDB.roundAmount(incomeSoFar - spentSoFar);
    headline = {
      kind: 'balance',
      label: balance >= 0 ? 'Te sobra este mes' : 'Gastaste más de lo que entró',
      amount: Math.abs(balance),
      tone: balance < 0 ? 'danger' : 'positive',
      ratio: null,
      detail: `Entraron ${formatCurrency(incomeSoFar)} y salieron ${formatCurrency(spentSoFar)}`,
      available: Math.max(0, balance)
    };
  } else {
    headline = {
      kind: 'spent',
      label: 'Gastaste este mes',
      amount: spentSoFar,
      tone: 'neutral',
      ratio: null,
      detail: spentSoFar > 0 ? 'Anota tus ingresos para ver cuánto te sobra' : 'Anota tu primer gasto con el botón +',
      available: 0
    };
  }

  let comparison = null;
  if (period.previousStart) {
    const previousCut = FinanceDB.addDays(period.previousStart, elapsedDays);
    const previousSpent = sumConsumption(period.previousStart, previousCut);
    if (previousSpent > 0) {
      comparison = {
        diff: FinanceDB.roundAmount(spentSoFar - previousSpent),
        previousSpent
      };
    }
  }

  const byCategory = new Map();
  snapshot.transactions
    .filter((item) => isConsumption(item) && isWithin(consumptionDate(item), period.startDate, today))
    .forEach((item) => {
      const key = item.categoryId || 'other-expense';
      byCategory.set(key, FinanceDB.roundAmount((byCategory.get(key) || 0) + amountInBase(item)));
    });
  const topCategories = Array.from(byCategory.entries())
    .map(([categoryId, amount]) => ({ categoryId, amount, ratio: spentSoFar > 0 ? amount / spentSoFar : 0 }))
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 5);

  return {
    period,
    today,
    spentSoFar,
    spentToday,
    incomeSoFar,
    daysLeft,
    elapsedDays,
    totalDays,
    perDay: headline.available > 0 ? FinanceDB.roundAmount(headline.available / daysLeft) : 0,
    headline,
    comparison,
    topCategories,
    // Los recurrentes se proyectan a futuro: "ultimos" son solo los que ya pasaron.
    recent: snapshot.transactions.filter((item) => FinanceDB.compareDate(item.date, today) <= 0).slice(0, 5)
  };
}
