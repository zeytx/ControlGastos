/* Cuentas y patrimonio. */
import { ACCOUNT_KIND_LABELS } from '../constants.js';
import { $, escapeHtml, renderEmptyPanel } from '../dom.js';
import { formatCurrency } from '../format.js';
import { getAccountProjectionView } from '../metrics.js';
import { getSnapshot } from '../state.js';

export function renderAccounts() {
  const container = $('#accounts-grid');
  const { accounts, balances } = getSnapshot();
  const projection = getAccountProjectionView();
  if (!accounts.length) {
    container.innerHTML = renderEmptyPanel('Aun no tienes cuentas', 'Crea una cuenta para empezar a distribuir tu dinero localmente.');
    return;
  }

  const netWorth = getSnapshot().netWorth || { liquid: 0, cardDebt: 0, otherDebt: 0, total: 0 };
  const visibleNetWorth = netWorth.liquid;
  const projectedVisibleNetWorth = projection.showProjection ? projection.visibleNetWorth || 0 : 0;
  const hasDebt = netWorth.cardDebt > 0 || netWorth.otherDebt > 0;

  const summaryCard = `
    <article class="panel-card">
      <div class="panel-card-header">
        <div>
          <p class="eyebrow">Vista global</p>
          <h4 class="panel-card-title">Patrimonio visible hoy</h4>
        </div>
        <span class="chip positive">${accounts.filter((account) => !account.archived).length} activas</span>
      </div>
      <div class="balance-amount">${formatCurrency(visibleNetWorth)}</div>
      <p class="panel-card-subtitle">Saldo real al dia de hoy en tus cuentas visibles.</p>
      ${hasDebt ? `
        <div class="networth-breakdown">
          ${netWorth.cardDebt > 0 ? `<span class="networth-line">Deuda de tarjetas <strong>-${escapeHtml(formatCurrency(netWorth.cardDebt))}</strong></span>` : ''}
          ${netWorth.otherDebt > 0 ? `<span class="networth-line">Otras deudas <strong>-${escapeHtml(formatCurrency(netWorth.otherDebt))}</strong></span>` : ''}
        </div>
        <div class="balance-amount ${netWorth.total < 0 ? 'negative' : ''}">${formatCurrency(netWorth.total)}</div>
        <p class="panel-card-subtitle">Patrimonio neto real, ya descontando lo que debes.</p>
      ` : ''}
      ${projection.showProjection ? `
        <div class="projection-box">
          <span class="projection-label">${escapeHtml(projection.label)}</span>
          <strong class="projection-amount">${escapeHtml(formatCurrency(projectedVisibleNetWorth))}</strong>
        </div>
      ` : ''}
    </article>
  `;

  const cards = accounts
    .map(
      (account) => {
        const currentBalance = balances[account.id] || 0;
        const projectedBalance = projection.showProjection ? (projection.balances[account.id] || 0) : currentBalance;
        return `
        <article class="panel-card" data-account-id="${escapeHtml(account.id)}">
          <div class="panel-card-header">
            <div>
              <h4 class="panel-card-title">${escapeHtml(account.name)}</h4>
              <span class="panel-card-subtitle">${escapeHtml(ACCOUNT_KIND_LABELS[account.kind] || account.kind)}</span>
            </div>
            <span class="chip ${account.archived ? 'warning' : 'positive'}">${account.archived ? 'Archivada' : 'Activa'}</span>
          </div>
          <div class="balance-amount">${formatCurrency(currentBalance)}</div>
          <p class="panel-card-subtitle">Disponible hoy</p>
          <div class="chips-row">
            <span class="chip">${account.includeInNetWorth ? 'Cuenta visible' : 'No suma patrimonio'}</span>
            <span class="chip">Inicial ${formatCurrency(account.openingBalance)}</span>
          </div>
          ${projection.showProjection ? `
            <div class="projection-box">
              <span class="projection-label">${escapeHtml(projection.label)}</span>
              <strong class="projection-amount">${escapeHtml(formatCurrency(projectedBalance))}</strong>
            </div>
          ` : ''}
          <div class="panel-actions">
            <button class="btn-secondary" data-account-action="edit" data-account-id="${escapeHtml(account.id)}">Editar</button>
          </div>
        </article>
      `;
      }
    )
    .join('');

  container.innerHTML = summaryCard + cards;
}
