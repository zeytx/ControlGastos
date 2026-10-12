/* Lista de movimientos y estado de almacenamiento. */
import { TYPE_LABELS } from '../constants.js';
import { $, $$, escapeHtml } from '../dom.js';
import { describeTransactionAccounts, formatCurrency, getAccountLabel, getCardLabel, formatDate, formatInstallmentLabel, formatLongDate, formatRelativeDay, formatSourceLabel, getCategoryIcon, getCategoryLabel, getCycleLabelById } from '../format.js';
import { getFilteredTransactions } from '../metrics.js';
import { getSnapshot, isSimpleMode, state } from '../state.js';

export function renderQuickFilters() {
  $$('#quick-filter-row [data-quick-filter]').forEach((button) => {
    button.classList.toggle('active', button.dataset.quickFilter === state.filters.type);
    if (state.filters.type === 'all' && button.dataset.quickFilter === 'all') {
      button.classList.add('active');
    }
  });
}

const OUTFLOW_TYPES = new Set(['expense', 'card_charge', 'debt_payment']);
const NEUTRAL_TYPES = new Set(['transfer', 'card_payment', 'goal_contribution']);

function amountSign(item) {
  if (item.type === 'income') return '+';
  if (OUTFLOW_TYPES.has(item.type)) return '-';
  return '';
}

function amountTone(item) {
  if (item.type === 'income') return 'income';
  if (NEUTRAL_TYPES.has(item.type)) return 'neutral';
  return 'expense';
}

/** Fila compacta de movimiento: emoji, que fue, de donde salio y cuanto. */
/** De donde salio o a donde entro, en pocas palabras (la fila es angosta). */
function shortAccounts(item) {
  if (item.type === 'expense' || item.type === 'debt_payment') return getAccountLabel(item.fromAccountId);
  if (item.type === 'income') return getAccountLabel(item.toAccountId);
  if (item.type === 'card_charge' || item.type === 'card_payment') return getCardLabel(item.cardId);
  if (item.type === 'transfer' || item.type === 'goal_contribution') {
    return [getAccountLabel(item.fromAccountId), getAccountLabel(item.toAccountId)].filter(Boolean).join(' → ');
  }
  return describeTransactionAccounts(item);
}

export function renderTransactionRow(item, { showDate = false } = {}) {
  const categoryLabel = getCategoryLabel(item.categoryId) || TYPE_LABELS[item.type] || '';
  const accountSummary = shortAccounts(item);
  const installmentLabel = item.installmentCount > 1 ? formatInstallmentLabel(item) : '';
  const simple = isSimpleMode();
  const subtitle = [showDate ? formatRelativeDay(item.date) : '', categoryLabel, accountSummary]
    .filter(Boolean)
    .join(' · ');
  const tags = [
    installmentLabel,
    item.manualOverride && !simple ? 'Editado a mano' : '',
    item.sourceType && item.sourceType !== 'manual' && !simple ? formatSourceLabel(item.sourceType) : '',
    item.statementCycleKey && item.type === 'card_charge' && !simple ? `Cierra ${formatDate(item.statementCycleKey)}` : ''
  ].filter(Boolean);

  return `
    <article class="tx-row" data-transaction-id="${escapeHtml(item.id)}" tabindex="0" role="button">
      <span class="tx-emoji" aria-hidden="true">${escapeHtml(getCategoryIcon(item.categoryId, item.type))}</span>
      <div class="tx-main">
        <strong class="tx-title">${escapeHtml(item.description)}</strong>
        <span class="tx-sub">${escapeHtml(subtitle)}</span>
        ${tags.length ? `<span class="tx-tags">${tags.map((tag) => `<span class="tx-tag">${escapeHtml(tag)}</span>`).join('')}</span>` : ''}
      </div>
      <strong class="tx-amount ${amountTone(item)}">${amountSign(item)}${escapeHtml(formatCurrency(item.amount))}</strong>
    </article>
  `;
}

export function renderTransactions() {
  const list = $('#transactions-list');
  const empty = $('#transactions-empty');
  // Lo mas reciente arriba, como en cualquier app de banco.
  const filtered = getFilteredTransactions().slice().reverse();
  $('#transactions-count-pill').textContent = `${filtered.length} movimiento${filtered.length === 1 ? '' : 's'}`;

  const totals = filtered.reduce(
    (acc, item) => {
      if (item.type === 'income') acc.in += item.amount;
      else if (OUTFLOW_TYPES.has(item.type)) acc.out += item.amount;
      return acc;
    },
    { in: 0, out: 0 }
  );
  $('#transactions-totals').innerHTML = filtered.length
    ? `<span class="total-in">Entró <strong>${escapeHtml(formatCurrency(totals.in))}</strong></span><span class="total-out">Salió <strong>${escapeHtml(formatCurrency(totals.out))}</strong></span>`
    : '';

  if (!filtered.length) {
    list.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }

  empty.classList.add('hidden');
  const groups = new Map();
  filtered.forEach((item) => {
    if (!groups.has(item.date)) groups.set(item.date, []);
    groups.get(item.date).push(item);
  });

  list.innerHTML = Array.from(groups.entries())
    .map(([date, items]) => {
      const dayOut = items.filter((item) => OUTFLOW_TYPES.has(item.type)).reduce((sum, item) => sum + item.amount, 0);
      return `
        <section class="day-group">
          <header class="day-head">
            <span>${escapeHtml(formatRelativeDay(date))}</span>
            ${dayOut > 0 ? `<span class="day-total">-${escapeHtml(formatCurrency(dayOut))}</span>` : ''}
          </header>
          <div class="day-items">${items.map((item) => renderTransactionRow(item)).join('')}</div>
        </section>
      `;
    })
    .join('');
}

export function renderStorageStatus() {
  const status = $('#storage-status');
  if (state.storage.persisted === null && !state.storage.estimate) {
    status.textContent = 'Tu navegador no expone detalles de persistencia. Igual el backup JSON sigue siendo recomendado.';
    return;
  }

  const used = state.storage.estimate?.usage
    ? `${(state.storage.estimate.usage / 1024 / 1024).toFixed(2)} MB usados`
    : 'uso no disponible';
  const quota = state.storage.estimate?.quota
    ? `${(state.storage.estimate.quota / 1024 / 1024).toFixed(0)} MB de cuota`
    : 'cuota no disponible';
  const persistedText =
    state.storage.persisted === true
      ? 'Almacenamiento persistente activo.'
      : 'Persistencia no garantizada por el navegador.';
  status.textContent = `${persistedText} ${used}, ${quota}.`;
}

export function renderBackupStatus() {
  const status = $('#backup-status');
  const lastBackupAt = getSnapshot().settings.lastBackupAt;
  status.textContent = lastBackupAt
    ? `Tu última copia es del ${formatLongDate(lastBackupAt.slice(0, 10))}.`
    : 'Aún no guardaste ninguna copia.';
}
