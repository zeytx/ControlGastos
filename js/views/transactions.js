/* Lista de movimientos y estado de almacenamiento. */
import { TYPE_LABELS } from '../constants.js';
import { $, $$, escapeHtml } from '../dom.js';
import { describeTransactionAccounts, formatCurrency, formatDate, formatInstallmentLabel, formatLongDate, formatSourceLabel, getCategoryLabel, getCycleLabelById } from '../format.js';
import { getFilteredTransactions } from '../metrics.js';
import { getSnapshot, state } from '../state.js';

export function renderQuickFilters() {
  $$('#quick-filter-row [data-quick-filter]').forEach((button) => {
    button.classList.toggle('active', button.dataset.quickFilter === state.filters.type);
    if (state.filters.type === 'all' && button.dataset.quickFilter === 'all') {
      button.classList.add('active');
    }
  });
}

export function renderTransactions() {
  const list = $('#transactions-list');
  const empty = $('#transactions-empty');
  const filtered = getFilteredTransactions();
  $('#transactions-count-pill').textContent = `${filtered.length} resultado${filtered.length === 1 ? '' : 's'}`;

  if (!filtered.length) {
    list.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }

  empty.classList.add('hidden');
  list.innerHTML = filtered
    .map((item) => {
      const accountSummary = describeTransactionAccounts(item);
      const categoryLabel = getCategoryLabel(item.categoryId);
      const cycleLabel = item.budgetCycleId ? getCycleLabelById(item.budgetCycleId) : '';
      const installmentLabel = formatInstallmentLabel(item);
      const amountClass =
        item.type === 'income'
          ? 'income'
          : item.type === 'transfer'
            ? 'transfer'
            : item.type;
      const typeBadge = (TYPE_LABELS[item.type] || item.type).slice(0, 3).toUpperCase();
      return `
        <article class="transaction-item" data-transaction-id="${escapeHtml(item.id)}">
          <div class="transaction-top">
            <div>
              <div class="transaction-heading">
                <span class="type-badge type-${escapeHtml(item.type)}">${escapeHtml(typeBadge)}</span>
                <h4 class="transaction-title">${escapeHtml(item.description)}</h4>
              </div>
              <div class="transaction-meta">${escapeHtml(TYPE_LABELS[item.type] || item.type)} / ${escapeHtml(formatDate(item.purchaseDate || item.date))}</div>
            </div>
            <div class="transaction-amount ${escapeHtml(amountClass)}">${formatCurrency(item.amount)}</div>
          </div>
          <div class="chips-row">
            ${categoryLabel ? `<span class="chip">${escapeHtml(categoryLabel)}</span>` : ''}
            ${accountSummary ? `<span class="chip">${escapeHtml(accountSummary)}</span>` : ''}
            ${cycleLabel ? `<span class="chip">${escapeHtml(cycleLabel)}</span>` : ''}
            ${item.statementCycleKey && item.type === 'card_charge' ? `<span class="chip">Cierra ${escapeHtml(formatDate(item.statementCycleKey))}</span>` : ''}
            ${installmentLabel ? `<span class="chip">${escapeHtml(installmentLabel)}</span>` : ''}
            ${item.manualOverride ? '<span class="chip warning">Editado a mano</span>' : ''}
            <span class="chip">${escapeHtml(formatSourceLabel(item.sourceType))}</span>
          </div>
          ${item.notes ? `<div class="transaction-foot"><span>${escapeHtml(item.notes)}</span></div>` : ''}
        </article>
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
    ? `Ultimo backup completo: ${formatLongDate(lastBackupAt.slice(0, 10))}.`
    : 'Aun no has exportado un backup completo.';
}
