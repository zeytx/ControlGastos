/* Formateo de montos, fechas y etiquetas legibles. */
import { MONTH_NAMES, SHORT_MONTH_NAMES } from './constants.js';
import { $ } from './dom.js';
import { getAccountMap, getCardMap, getCategoryMap, getSnapshot } from './state.js';

export function formatCurrency(value) {
  return `S/ ${FinanceDB.roundAmount(value).toFixed(2)}`;
}

export function formatDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'Sin fecha';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const date = FinanceDB.normalizeDate(raw);
  const year = parseInt(date.slice(0, 4), 10);
  const month = parseInt(date.slice(5, 7), 10);
  const day = parseInt(date.slice(8, 10), 10);
  return `${day} ${SHORT_MONTH_NAMES[month - 1]} ${year}`;
}

export function formatShortDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'Sin fecha';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const date = FinanceDB.normalizeDate(raw);
  const month = parseInt(date.slice(5, 7), 10);
  const day = parseInt(date.slice(8, 10), 10);
  return `${day} ${SHORT_MONTH_NAMES[month - 1]}`;
}

export function formatLongDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'Sin fecha';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const date = FinanceDB.normalizeDate(raw);
  const year = parseInt(date.slice(0, 4), 10);
  const month = parseInt(date.slice(5, 7), 10);
  const day = parseInt(date.slice(8, 10), 10);
  return `${day} de ${MONTH_NAMES[month - 1]} de ${year}`;
}

export function formatMonthLabel(value) {
  const raw = String(value || '').trim();
  const normalized = /^\d{4}-\d{2}$/.test(raw) ? `${raw}-01` : raw;
  const date = FinanceDB.normalizeDate(normalized);
  const year = parseInt(date.slice(0, 4), 10);
  const month = parseInt(date.slice(5, 7), 10);
  return `${MONTH_NAMES[month - 1]} ${year}`;
}

export function formatCycleMonthLabel(cycle) {
  if (!cycle) return 'Sin ciclo';
  const date = FinanceDB.normalizeDate(cycle.startDate);
  const year = parseInt(date.slice(0, 4), 10);
  const month = parseInt(date.slice(5, 7), 10);
  return `${MONTH_NAMES[month - 1]} ${year}`;
}

export function formatCycleRange(cycle) {
  if (!cycle) return '';
  return `Del ${formatShortDate(cycle.startDate)} al ${formatShortDate(cycle.endDate)}`;
}

export function formatCycleOptionLabel(cycle) {
  if (!cycle) return 'Sin ciclo';
  return `${formatCycleMonthLabel(cycle)} / ${formatShortDate(cycle.startDate)} - ${formatShortDate(cycle.endDate)}`;
}

export function formatCycleLabel(cycle) {
  if (!cycle) return 'Ciclo sin configurar';
  return `Ciclo ${formatCycleMonthLabel(cycle)}`;
}

export function getAccountLabel(accountId) {
  if (!accountId) return '';
  return getAccountMap().get(accountId)?.name || 'Cuenta no encontrada';
}

export function getCategoryLabel(categoryId) {
  return getCategoryMap().get(categoryId)?.name || '';
}

export function getCardDisplayLabel(card) {
  if (!card) return '';
  return `${card.bankName} • ${card.last4}`;
}

export function getCardLabel(cardId) {
  const card = getCardMap().get(cardId);
  return card ? `${card.bankName} • ${card.last4}` : '';
}

export function formatStatementLabel(statement) {
  if (!statement) return 'Sin estado';
  if (statement.kind === 'opening-debt') return 'Deuda inicial';
  if (statement.periodStart && statement.periodEnd) {
    return `${formatShortDate(statement.periodStart)} - ${formatShortDate(statement.periodEnd)}`;
  }
  return String(statement.label || '');
}

export function formatInstallmentLabel(transaction) {
  if (!transaction || transaction.type !== 'card_charge') return '';
  const count = Math.max(1, parseInt(transaction.installmentCount || 1, 10) || 1);
  const index = Math.max(1, parseInt(transaction.installmentIndex || 1, 10) || 1);
  if (count <= 1) return '1 cuota';
  return `Cuota ${index}/${count}`;
}

export function isInstallmentGroupTransaction(transaction) {
  return !!(transaction?.type === 'card_charge' && transaction.installmentGroupId && transaction.installmentCount > 1);
}

export function getCycleLabelById(cycleId) {
  const cycle = getSnapshot().cycles.find((item) => item.id === cycleId);
  return formatCycleLabel(cycle);
}

export function describeTransactionAccounts(transaction) {
  if (transaction.type === 'expense') return `Sale de ${getAccountLabel(transaction.fromAccountId)}`;
  if (transaction.type === 'income') return `Entra a ${getAccountLabel(transaction.toAccountId)}`;
  if (transaction.type === 'transfer') return `${getAccountLabel(transaction.fromAccountId)} -> ${getAccountLabel(transaction.toAccountId)}`;
  if (transaction.type === 'card_charge') return getCardLabel(transaction.cardId);
  if (transaction.type === 'card_payment') return `Paga ${getCardLabel(transaction.cardId)} desde ${getAccountLabel(transaction.fromAccountId)}`;
  if (transaction.type === 'debt_payment') return `Pagado desde ${getAccountLabel(transaction.fromAccountId)}`;
  if (transaction.type === 'goal_contribution') return `${getAccountLabel(transaction.fromAccountId)} -> ${getAccountLabel(transaction.toAccountId)}`;
  return '';
}

export function formatSourceLabel(sourceType) {
  const map = {
    manual: 'Manual',
    legacy: 'Migrado',
    recurring: 'Recurrente',
    system: 'Automatico',
    'image-upload': 'IA imagen',
    'text-email': 'IA correo',
    'pdf-text': 'IA PDF',
    'text-file': 'IA texto'
  };
  return map[sourceType] || sourceType || 'Manual';
}

export function agendaKindLabel(kind) {
  const labels = { salary: 'Sueldo', 'card-close': 'Corte', 'card-due': 'Vencimiento', 'card-charge': 'Cargo tarjeta', income: 'Ingreso', expense: 'Gasto', statement: 'Estado de cuenta', transaction: 'Transacción' };
  return labels[kind] || kind;
}

export function agendaKindIcon(kind) {
  const icons = { salary: '$', 'card-close': '||', 'card-due': '!', 'card-charge': 'TC', income: '^', expense: 'v', statement: 'EC' };
  return icons[kind] || '·';
}
