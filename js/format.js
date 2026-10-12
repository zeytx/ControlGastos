/* Formateo de montos, fechas y etiquetas legibles. */
import { MONTH_NAMES, SHORT_MONTH_NAMES } from './constants.js';
import { getCategoryEmoji } from './icons.js';
import { getAccountMap, getCardMap, getCategoryMap, getSnapshot } from './state.js';

const amountFormatter = new Intl.NumberFormat('es-PE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function formatCurrency(value) {
  const amount = FinanceDB.roundAmount(value);
  return `${amount < 0 ? '-' : ''}S/ ${amountFormatter.format(Math.abs(amount))}`;
}

const WEEKDAY_NAMES = ['Domingo', 'Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado'];

/** "Hoy", "Ayer" o "Lunes 6 Oct": como se lee una fecha en una lista. */
export function formatRelativeDay(value) {
  const date = FinanceDB.normalizeDate(value);
  const today = FinanceDB.getToday();
  if (date === today) return 'Hoy';
  if (date === FinanceDB.addDays(today, -1)) return 'Ayer';
  if (date === FinanceDB.addDays(today, 1)) return 'Manana';
  const [year, month, day] = date.split('-').map((part) => parseInt(part, 10));
  const weekday = WEEKDAY_NAMES[new Date(year, month - 1, day, 12).getDay()];
  const sameYear = date.slice(0, 4) === today.slice(0, 4);
  return `${weekday} ${day} ${SHORT_MONTH_NAMES[month - 1]}${sameYear ? '' : ` ${year}`}`;
}

export function getCategoryIcon(categoryId, transactionType = 'expense') {
  return getCategoryEmoji(getCategoryMap().get(categoryId), transactionType);
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
    'text-file': 'IA texto',
    'statement-import': 'Estado de cuenta'
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
