/* Metas, deudas, recurrentes y agenda. */
import { DEBT_KIND_LABELS } from '../constants.js';
import { $, escapeHtml, renderEmptyPanel } from '../dom.js';
import { agendaKindIcon, agendaKindLabel, formatCurrency, formatDate, formatLongDate, formatMonthLabel, formatShortDate, getAccountLabel, getCategoryLabel } from '../format.js';
import { getSnapshot } from '../state.js';

export function renderGoals() {
  const container = $('#goals-grid');
  const goals = getSnapshot().goals;
  if (!goals.length) {
    container.innerHTML = renderEmptyPanel('Sin metas aun', 'Define una meta de ahorro y luego registra aportes desde tus cuentas.');
    return;
  }

  container.innerHTML = goals
    .map((goal) => {
      const progress = goal.targetAmount > 0 ? Math.min(100, Math.round((goal.currentAmount / goal.targetAmount) * 100)) : 0;
      return `
        <article class="panel-card" data-goal-id="${escapeHtml(goal.id)}">
          <div class="panel-card-header">
            <div>
              <h4 class="panel-card-title">${escapeHtml(goal.name)}</h4>
              <span class="panel-card-subtitle">${goal.targetDate ? `Meta para ${escapeHtml(formatLongDate(goal.targetDate))}` : 'Sin fecha objetivo'}</span>
            </div>
            <span class="chip ${goal.archived ? 'warning' : 'positive'}">${goal.archived ? 'Archivada' : `${progress}%`}</span>
          </div>
          <div class="balance-amount">${formatCurrency(goal.currentAmount)}</div>
          <div class="progress-track"><div class="progress-fill" style="width: ${progress}%"></div></div>
          <div class="chips-row">
            <span class="chip">Objetivo ${formatCurrency(goal.targetAmount)}</span>
            <span class="chip">${escapeHtml(getAccountLabel(goal.accountId) || 'Sin cuenta destino')}</span>
            ${goal.contributedAmount > 0 ? `<span class="chip positive">Aportado ${escapeHtml(formatCurrency(goal.contributedAmount))}</span>` : ''}
            ${goal.completed ? '<span class="chip positive">Meta cumplida</span>' : ''}
          </div>
          <div class="panel-actions">
            <button class="btn-primary" data-goal-action="contribute" data-goal-id="${escapeHtml(goal.id)}">Aportar</button>
            <button class="btn-secondary" data-goal-action="edit" data-goal-id="${escapeHtml(goal.id)}">Editar</button>
          </div>
        </article>
      `;
    })
    .join('');
}

export function renderDebts() {
  const container = $('#debts-grid');
  const debts = getSnapshot().debts;
  if (!debts.length) {
    container.innerHTML = renderEmptyPanel('Sin deudas registradas', 'Si tienes prestamos o cuotas, registralos aqui para no olvidarlos.');
    return;
  }

  container.innerHTML = debts
    .map((debt) => {
      const progress = debt.totalAmount > 0
        ? Math.min(100, Math.round(((debt.totalAmount - debt.outstandingAmount) / debt.totalAmount) * 100))
        : 0;
      const quotaLabel = debt.installmentCount ? `${debt.installmentsPaid}/${debt.installmentCount} cuotas` : 'Sin cuota definida';
      return `
        <article class="panel-card" data-debt-id="${escapeHtml(debt.id)}">
          <div class="panel-card-header">
            <div>
              <h4 class="panel-card-title">${escapeHtml(debt.name)}</h4>
              <span class="panel-card-subtitle">${escapeHtml(DEBT_KIND_LABELS[debt.kind] || debt.kind)} / Pago dia ${escapeHtml(debt.dueDay)}</span>
            </div>
            <span class="chip ${debt.archived ? 'warning' : debt.outstandingAmount > 0 ? 'danger' : 'positive'}">${debt.archived ? 'Archivada' : debt.outstandingAmount > 0 ? 'Pendiente' : 'Pagada'}</span>
          </div>
          <div class="balance-amount">${formatCurrency(debt.outstandingAmount)}</div>
          <div class="progress-track"><div class="progress-fill" style="width: ${progress}%"></div></div>
          <div class="chips-row">
            <span class="chip">Original ${formatCurrency(debt.totalAmount)}</span>
            <span class="chip">${escapeHtml(quotaLabel)}</span>
            <span class="chip">Minimo ${formatCurrency(debt.minimumPayment)}</span>
            ${debt.paidAmount > 0 ? `<span class="chip positive">Pagado ${escapeHtml(formatCurrency(debt.paidAmount))}</span>` : ''}
            ${debt.settled ? '<span class="chip positive">Saldada</span>' : ''}
          </div>
          <div class="panel-actions">
            ${debt.settled ? '' : `<button class="btn-primary" data-debt-action="pay" data-debt-id="${escapeHtml(debt.id)}">Registrar pago</button>`}
            <button class="btn-secondary" data-debt-action="edit" data-debt-id="${escapeHtml(debt.id)}">Editar</button>
          </div>
        </article>
      `;
    })
    .join('');
}

export function renderRecurring() {
  const container = $('#recurring-grid');
  const recurring = getSnapshot().recurring;
  if (!recurring.length) {
    container.innerHTML = renderEmptyPanel('Nada automatico aun', 'Crea ingresos o gastos que deban caer solos cada mes.');
    return;
  }

  container.innerHTML = recurring
    .map((item) => `
      <article class="panel-card" data-recurring-id="${escapeHtml(item.id)}">
        <div class="panel-card-header">
          <div>
            <h4 class="panel-card-title">${escapeHtml(item.description)}</h4>
            <span class="panel-card-subtitle">${item.type === 'income' ? 'Ingreso' : 'Gasto'} / Dia ${escapeHtml(item.dayOfMonth)}</span>
          </div>
          <span class="chip ${item.active ? 'positive' : 'warning'}">${item.active ? 'Activo' : 'Pausado'}</span>
        </div>
        <div class="balance-amount">${formatCurrency(item.amount)}</div>
        <div class="chips-row">
          <span class="chip">${escapeHtml(getCategoryLabel(item.categoryId) || 'Sin categoria')}</span>
          <span class="chip">${escapeHtml(getAccountLabel(item.accountId) || 'Sin cuenta')}</span>
          ${item.startDate ? `<span class="chip">Empieza ${escapeHtml(formatShortDate(item.startDate))}</span>` : ''}
          ${item.endMonth ? `<span class="chip warning">Hasta ${escapeHtml(formatMonthLabel(item.endMonth))}</span>` : ''}
          ${item.isPrimarySalary ? '<span class="chip positive">Sueldo principal</span>' : ''}
        </div>
        <div class="panel-actions">
          <button class="btn-secondary" data-recurring-action="edit" data-recurring-id="${escapeHtml(item.id)}">Editar</button>
          <button class="btn-danger" data-recurring-action="delete" data-recurring-id="${escapeHtml(item.id)}">Eliminar</button>
        </div>
      </article>
    `)
    .join('');
}

export function renderAgenda() {
  const container = $('#agenda-list');
  const items = getSnapshot().agenda || [];
  if (!items.length) {
    container.innerHTML = renderEmptyPanel('Sin eventos cercanos', 'Cuando existan sueldos, cortes, pagos o recurrentes proximos apareceran aqui.');
    return;
  }

  container.innerHTML = items
    .map(
      (item) => `
        <article class="transaction-item agenda-item agenda-${escapeHtml(item.kind)}">
          <div class="transaction-top">
            <div class="transaction-heading">
              <span class="agenda-icon agenda-icon-${escapeHtml(item.kind)}">${escapeHtml(agendaKindIcon(item.kind))}</span>
              <div>
                <h4 class="transaction-title">${escapeHtml(item.title)}</h4>
                <div class="transaction-meta">${escapeHtml(formatDate(item.date))} · ${escapeHtml(agendaKindLabel(item.kind))}</div>
              </div>
            </div>
            <span class="chip">${escapeHtml(item.subtitle)}</span>
          </div>
        </article>
      `
    )
    .join('');
}
