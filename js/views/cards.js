/* Tarjetas por fecha de corte. */
import { $, escapeHtml, renderEmptyPanel } from '../dom.js';
import { formatCurrency, formatDate, formatStatementLabel, getCardLabel, getCycleLabelById } from '../format.js';
import { getSnapshot } from '../state.js';

export function renderCards() {
  const container = $('#cards-summary-grid');
  const cards = getSnapshot().cards;
  if (!cards.length) {
    container.innerHTML = renderEmptyPanel('Sin tarjetas aún', 'Registra banco, últimos 4, corte y pago para asignar compras al sueldo correcto.');
    return;
  }

  container.innerHTML = cards
    .map((card) => {
      const statement = card.currentStatement;
      const assignedCycle = statement?.budgetCycleId ? getCycleLabelById(statement.budgetCycleId) : 'Sin sueldo asignado aún';
      const progress = statement?.chargedAmount ? Math.min(100, Math.round((statement.paidAmount / statement.chargedAmount) * 100)) : 0;
      const utilizationWidth = Math.max(0, Math.min(100, card.utilizationPct || 0));
      const statementLabel = statement ? formatStatementLabel(statement) : 'Aún no hay estado abierto para esta tarjeta.';
      return `
        <article class="panel-card" data-card-id="${escapeHtml(card.id)}">
          <div class="panel-card-header">
            <div>
              <h4 class="panel-card-title">${escapeHtml(getCardLabel(card.id))}</h4>
              <span class="panel-card-subtitle">Corte día ${escapeHtml(card.closingDay)} / Pago día ${escapeHtml(card.dueDay)}</span>
            </div>
            <span class="chip ${card.archived ? 'warning' : card.currentDebt > 0 ? 'danger' : 'positive'}">${card.archived ? 'Archivada' : card.currentDebt > 0 ? 'Con deuda' : 'Sin deuda'}</span>
          </div>
          <div class="balance-amount">${formatCurrency(card.currentDebt || 0)}</div>
          <p class="panel-card-subtitle">Deuda actual</p>
          <div class="card-metrics-grid">
            <div class="metric-tile">
              <span class="metric-kicker">Disponible</span>
              <strong>${formatCurrency(card.availableCredit || 0)}</strong>
            </div>
            <div class="metric-tile">
              <span class="metric-kicker">Línea total</span>
              <strong>${formatCurrency(card.creditLimit || 0)}</strong>
            </div>
          </div>
          <div class="progress-track compact-track"><div class="progress-fill" style="width: ${utilizationWidth}%"></div></div>
          <div class="utilization-copy">
            <span>Uso ${escapeHtml(`${FinanceDB.roundAmount(card.utilizationPct || 0).toFixed(1)}%`)}</span>
            <span>${escapeHtml(card.creditLimit > 0 ? `Deuda ${formatCurrency(card.currentDebt || 0)}` : 'Agrega la línea total')}</span>
          </div>
          <div class="chips-row">
            <span class="chip">Próximo corte ${escapeHtml(formatDate(card.nextClosingDate))}</span>
            <span class="chip">Próximo pago ${escapeHtml(formatDate(card.nextDueDate))}</span>
            <span class="chip">${escapeHtml(statement ? assignedCycle : 'Sin estado abierto')}</span>
            ${card.openingDebtPending > 0 ? `<span class="chip warning">Deuda inicial ${escapeHtml(formatCurrency(card.openingDebtPending))}</span>` : ''}
            ${card.creditBalance > 0 ? `<span class="chip positive">Saldo a favor ${escapeHtml(formatCurrency(card.creditBalance))}</span>` : ''}
            ${card.needsReview ? '<span class="chip warning">Revisar migracion</span>' : ''}
          </div>
          <div class="panel-card-subtitle">${escapeHtml(statement ? `${statementLabel} / cubierto ${progress}%` : statementLabel)}</div>
          <div class="panel-actions">
            ${card.archived ? '' : `<button class="btn-primary" data-card-action="charge" data-card-id="${escapeHtml(card.id)}">Compra</button>`}
            ${card.archived ? '' : `<button class="btn-primary" data-card-action="pay" data-card-id="${escapeHtml(card.id)}">Pagar</button>`}
            <button class="btn-secondary" data-card-action="statements" data-card-id="${escapeHtml(card.id)}">Estados</button>
            <button class="btn-secondary" data-card-action="edit" data-card-id="${escapeHtml(card.id)}">Editar</button>
          </div>
        </article>
      `;
    })
    .join('');
}
