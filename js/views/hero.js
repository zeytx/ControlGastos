/* Cabecera del ciclo, tope, sobrante y alertas. */
import { $, escapeHtml, fillSelect } from '../dom.js';
import { formatCurrency, formatCycleLabel, formatCycleOptionLabel, formatCycleRange, getAccountLabel } from '../format.js';
import { buildBackupAlert, buildBudgetAlert, buildUnassignedChargesAlert } from '../metrics.js';
import { getRecurringMap, getSelectedCycle, getSelectedCycleId, getSnapshot, getTodayDate, state } from '../state.js';

export function renderInstallCard() {
  const card = $('#install-card');
  const copy = $('#install-copy');
  const settings = getSnapshot().settings;
  const dismissed = !!settings.installPromptDismissedAt;
  const hasData =
    getSnapshot().transactions.length +
    getSnapshot().goals.length +
    getSnapshot().debts.length +
    getSnapshot().recurring.length > 0;

  if (state.platform.standalone || dismissed) {
    card.classList.add('hidden');
    return;
  }

  const installGuide = state.platform.isIOS
    ? 'En iPhone usa Safari > Compartir > "Anadir a pantalla de inicio".'
    : 'Instalala desde el navegador para que se sienta como app y quede mejor separada del resto de la navegacion.';
  const extra = hasData
    ? ' Si ya cargaste datos aqui en Safari, exporta JSON e importalo luego en la version instalada para no dividir tu informacion.'
    : ' Te conviene instalarla antes de empezar a registrar mucho movimiento.';

  copy.textContent = `${installGuide}${extra}`;
  card.classList.remove('hidden');
}

export function renderHeader() {
  $('#header-subtitle').textContent = state.platform.standalone
    ? 'Modo app activa. Tus datos viven aqui en este dispositivo.'
    : 'Modo navegador. Instalala si quieres que se sienta como app local.';
}

export function renderHero() {
  const cycle = getSelectedCycle();
  $('#cycle-label').textContent = formatCycleLabel(cycle);
  $('#cycle-subtitle').textContent = cycle
    ? formatCycleRange(cycle)
    : 'Configura un recurrente de ingreso como sueldo principal para activar los ciclos.';

  const summary = cycle || {};
  $('#summary-income').textContent = formatCurrency(summary.income || 0);
  $('#summary-cash-spend').textContent = formatCurrency(summary.cashSpend || 0);
  $('#summary-card-assigned').textContent = formatCurrency(summary.cardAssigned || 0);
  $('#summary-free').textContent = formatCurrency(summary.freeNetAmount ?? summary.freeLiquidNow ?? 0);

  const progressRow = $('#cycle-progress-row');
  if (cycle) {
    const today = getTodayDate();
    const startMs = new Date(cycle.startDate).getTime();
    const endMs = new Date(cycle.endDate).getTime();
    const todayMs = new Date(today).getTime();
    const totalDays = Math.max(1, Math.round((endMs - startMs) / 86400000));
    const elapsed = Math.max(0, Math.round((todayMs - startMs) / 86400000));
    const pct = Math.min(100, Math.round((elapsed / totalDays) * 100));
    const remaining = Math.max(0, totalDays - elapsed);
    $('#cycle-time-fill').style.width = `${pct}%`;
    $('#cycle-progress-label').textContent = `Dia ${elapsed} de ${totalDays} (${remaining}d restantes)`;
    progressRow.style.display = '';
  } else {
    progressRow.style.display = 'none';
  }

  const config = getSnapshot().settings.financialCycleConfig || {};
  const meta = $('#cycle-meta');
  const note = $('#cycle-note');
  const chips = $('#cycle-chips');

  if (!config.primarySalaryRecurringId) {
    meta.textContent = 'Sin sueldo principal configurado';
    note.textContent = 'Elige el recurrente de ingreso que abre tu ciclo financiero.';
    chips.innerHTML = '<span class="chip warning">Sin configuracion de ciclo</span>';
    return;
  }

  const recurring = getRecurringMap().get(config.primarySalaryRecurringId);
  meta.textContent = recurring
    ? `Sueldo principal: ${recurring.description} el dia ${recurring.dayOfMonth}`
    : 'Sueldo principal configurado';
  note.textContent = cycle?.sweepTransferId
    ? `Este ciclo ya movio ${formatCurrency(cycle.sweptAmount || 0)} a ahorro al llegar el siguiente sueldo.`
    : 'Cuando llegue el siguiente sueldo, el sobrante libre neto de este ciclo podra moverse a ahorro.';
  chips.innerHTML = [
    config.savingsAccountId ? `<span class="chip">Ahorro destino: ${escapeHtml(getAccountLabel(config.savingsAccountId))}</span>` : '',
    config.sweepSourceAccountId ? `<span class="chip">Barrer desde: ${escapeHtml(getAccountLabel(config.sweepSourceAccountId))}</span>` : '',
    `<span class="chip ${config.savingsSweepEnabled ? 'positive' : 'warning'}">${
      config.savingsSweepEnabled
        ? (config.savingsSweepMode === 'auto' ? 'Ahorro automatico' : 'Ahorro propuesto')
        : 'Ahorro pausado'
    }</span>`
  ].join('');
}

export function renderBudgetProgress() {
  const card = $('#budget-progress-card');
  const budget = getSnapshot().budget;
  const isCurrentCycle = getSelectedCycleId() === getSnapshot().currentCycleId;

  if (!budget?.enabled || !isCurrentCycle) {
    card.classList.add('hidden');
    return;
  }

  card.classList.remove('hidden');

  const globalRow = $('#budget-global-row');
  if (budget.limit > 0) {
    globalRow.classList.remove('hidden');
    $('#budget-progress-meta').textContent = `${formatCurrency(budget.spent)} de ${formatCurrency(budget.limit)}`;
    $('#budget-progress-fill').style.width = `${Math.round(budget.ratio * 100)}%`;
    $('#budget-progress-fill').classList.toggle('over', budget.overspent);
    $('#budget-progress-note').textContent = budget.overspent
      ? `Te pasaste ${formatCurrency(Math.abs(budget.remaining))} del tope que te pusiste para este ciclo.`
      : `Te quedan ${formatCurrency(budget.remaining)} antes de tocar el tope de este ciclo.`;
  } else {
    globalRow.classList.add('hidden');
    $('#budget-progress-meta').textContent = 'Topes por categoria';
    $('#budget-progress-note').textContent = '';
  }

  const categories = budget.byCategory || [];
  $('#budget-category-list').innerHTML = categories
    .map(
      (item) => `
        <div class="breakdown-row">
          <div class="breakdown-meta">
            <strong>${escapeHtml(item.name)}</strong>
            <span class="${item.overspent ? 'over-budget' : ''}">${escapeHtml(formatCurrency(item.spent))} / ${escapeHtml(formatCurrency(item.limit))}</span>
          </div>
          <div class="breakdown-bar">
            <div class="breakdown-fill ${item.overspent ? 'over' : ''}" style="width: ${(item.ratio * 100).toFixed(1)}%"></div>
          </div>
        </div>
      `
    )
    .join('');
}

export function renderSweepSuggestion() {
  const card = $('#sweep-suggestion-card');
  const cycle = getSelectedCycle();
  const config = getSnapshot().settings.financialCycleConfig || {};
  const confirmButton = $('#btn-confirm-sweep');
  const undoButton = $('#btn-undo-sweep');

  const hasConfirmedSweep = !!cycle?.sweepTransferId;
  const suggested = FinanceDB.roundAmount(cycle?.suggestedSweepAmount || 0);

  if (!cycle || !config.savingsSweepEnabled || (!hasConfirmedSweep && suggested <= 0)) {
    card.classList.add('hidden');
    return;
  }

  card.classList.remove('hidden');

  if (hasConfirmedSweep) {
    $('#sweep-suggestion-meta').textContent = formatCurrency(cycle.sweptAmount || 0);
    $('#sweep-suggestion-note').textContent = config.savingsSweepMode === 'auto'
      ? 'La app registro este traslado automaticamente al cerrar el ciclo.'
      : `Ya registraste el traslado a ${getAccountLabel(config.savingsAccountId) || 'tu cuenta de ahorro'}.`;
    confirmButton.classList.add('hidden');
    undoButton.classList.toggle('hidden', config.savingsSweepMode === 'auto');
    return;
  }

  $('#sweep-suggestion-meta').textContent = formatCurrency(suggested);
  $('#sweep-suggestion-note').textContent = `Esto es lo que te sobro libre despues de cubrir tarjetas y deudas del ciclo. Si ya lo moviste a ${getAccountLabel(config.savingsAccountId) || 'tu cuenta de ahorro'}, registralo aqui.`;
  confirmButton.classList.remove('hidden');
  undoButton.classList.add('hidden');
}

export function renderCycleHistory() {
  const select = $('#cycle-history-select');
  const prevButton = $('#btn-prev-cycle');
  const nextButton = $('#btn-next-cycle');
  const cycles = getSnapshot().cycles || [];
  const currentIndex = cycles.findIndex((cycle) => cycle.id === state.selectedCycleId);

  if (!cycles.length) {
    fillSelect(select, [{ value: '', label: 'Sin ciclos aun' }], '');
    select.disabled = true;
    prevButton.disabled = true;
    nextButton.disabled = true;
    return;
  }

  fillSelect(
    select,
    cycles.map((cycle) => ({
      value: cycle.id,
      label: formatCycleOptionLabel(cycle)
    })),
    state.selectedCycleId
  );

  select.disabled = false;
  prevButton.disabled = currentIndex === -1 || currentIndex >= cycles.length - 1;
  nextButton.disabled = currentIndex <= 0;
}

export function renderAlerts() {
  const container = $('#alerts-list');
  const alerts = [...(getSnapshot().alerts || [])];
  const budgetAlert = buildBudgetAlert();
  if (budgetAlert) alerts.push(budgetAlert);
  const unassignedAlert = buildUnassignedChargesAlert();
  if (unassignedAlert) alerts.push(unassignedAlert);
  const backupAlert = buildBackupAlert();
  if (backupAlert) alerts.push(backupAlert);

  if (!alerts.length) {
    container.innerHTML = `
      <div class="alert-item info">
        <strong>Todo en orden</strong>
        <span>No hay alertas urgentes y tu backup esta al dia.</span>
      </div>
    `;
    return;
  }

  container.innerHTML = alerts
    .map(
      (alert) => `
        <div class="alert-item ${escapeHtml(alert.kind || 'info')}">
          <strong>${escapeHtml(alert.title)}</strong>
          <span>${escapeHtml(alert.text)}</span>
        </div>
      `
    )
    .join('');
}

export function renderCycleConfigPreview() {
  const config = getSnapshot().settings.financialCycleConfig || {};
  const recurring = getRecurringMap().get(config.primarySalaryRecurringId);
  const liquidAccounts = (config.liquidAccountIds || []).map((id) => getAccountLabel(id)).filter(Boolean);
  const body = $('#cycle-config-preview');

  if (!recurring) {
    body.innerHTML = `
      <div class="detail-card">
        <strong>Ciclo aun no configurado</strong>
        <p>Necesitas elegir un recurrente de ingreso como sueldo principal para que la app se mueva por ciclos y no por meses calendario.</p>
      </div>
    `;
    return;
  }

  body.innerHTML = `
    <div class="detail-card">
      <strong>Sueldo que abre el ciclo</strong>
      <p>${escapeHtml(recurring.description)} el dia ${escapeHtml(recurring.dayOfMonth)}</p>
    </div>
    <div class="detail-card">
      <strong>Sobrante a ahorro</strong>
      <p>${config.savingsSweepEnabled
        ? `${config.savingsSweepMode === 'auto' ? 'Se registra solo' : 'Se propone y tu lo confirmas'} hacia ${escapeHtml(getAccountLabel(config.savingsAccountId) || 'sin cuenta definida')}`
        : 'Pausado'}</p>
    </div>
    <div class="detail-card">
      <strong>Cuentas liquidas consideradas</strong>
      <p>${liquidAccounts.length ? escapeHtml(liquidAccounts.join(', ')) : 'Ninguna configurada'}</p>
    </div>
    <div class="detail-card">
      <strong>Cuenta desde la que se mueve</strong>
      <p>${escapeHtml(getAccountLabel(config.sweepSourceAccountId) || 'Sin definir')}</p>
    </div>
  `;
}
