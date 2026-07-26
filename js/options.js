/* Poblado y sincronizacion de los selects de los formularios. */
import { $, escapeHtml, fillSelect } from './dom.js';
import { formatCurrency, formatDate, formatStatementLabel, getCardDisplayLabel, getCycleLabelById } from './format.js';
import { getCardMap, getOpenStatementsForCard, getSnapshot, getTodayDate, state } from './state.js';

export function buildAccountOptions({ includeArchived = false, kinds = null, emptyLabel = '' } = {}) {
  const list = getSnapshot().accounts.filter((account) => {
    if (!includeArchived && account.archived) return false;
    if (Array.isArray(kinds) && !kinds.includes(account.kind)) return false;
    return true;
  });
  const options = list.map((account) => ({
    value: account.id,
    label: `${account.name}${account.archived ? ' (archivada)' : ''}`
  }));
  if (emptyLabel) {
    return [{ value: '', label: emptyLabel }].concat(options);
  }
  return options;
}

export function buildCardOptions({ emptyLabel = '', includeArchived = false, selectedCardId = '' } = {}) {
  const options = getSnapshot().cards
    .filter((card) => includeArchived || !card.archived || card.id === selectedCardId)
    .map((card) => ({
      value: card.id,
      label: `${card.bankName} • ${card.last4}`
    }));
  if (emptyLabel) {
    return [{ value: '', label: emptyLabel }].concat(options);
  }
  return options;
}

export function buildVisibleCardOptions({ emptyLabel = '', includeArchived = false, selectedCardId = '' } = {}) {
  const options = getSnapshot().cards
    .filter((card) => includeArchived || !card.archived || card.id === selectedCardId)
    .map((card) => ({
      value: card.id,
      label: `${getCardDisplayLabel(card)}${card.archived ? ' (archivada)' : ''}`
    }));
  if (emptyLabel) {
    return [{ value: '', label: emptyLabel }].concat(options);
  }
  return options;
}

export function setDefaultFormDates() {
  const today = FinanceDB.getToday();
  if ($('#tx-date')) $('#tx-date').value = today;
}

export function loadSettingsIntoInputs() {
  const settings = getSnapshot().settings;
  $('#settings-budget-input').value = settings.monthlyBudget ? FinanceDB.roundAmount(settings.monthlyBudget) : '';
  $('#settings-api-key').value = localStorage.getItem('openai_api_key') || '';
}

export function populateSelects() {
  populateFilterOptions();
  populateTransactionFormOptions($('#tx-card-id')?.value || '');
  populateInstallmentCountOptions($('#tx-installment-count')?.value || '1');
  populateGoalAccountOptions();
  populateDebtAccountOptions();
  populateRecurringAccountOptions();
  populateCardPaymentOptions();
  populateSettingsOptions();
}

export function populateFilterOptions() {
  fillSelect(
    $('#filter-account'),
    [{ value: 'all', label: 'Todas' }].concat(buildAccountOptions({ includeArchived: true })),
    state.filters.accountId
  );
  state.filters.accountId = $('#filter-account').value;

  fillSelect(
    $('#filter-card'),
    [{ value: 'all', label: 'Todas' }].concat(buildCardOptions()),
    state.filters.cardId
  );
  state.filters.cardId = $('#filter-card').value;

  fillSelect(
    $('#filter-category'),
    [{ value: 'all', label: 'Todas' }].concat(
      getSnapshot().categories.map((category) => ({ value: category.id, label: category.name }))
    ),
    state.filters.categoryId
  );
  state.filters.categoryId = $('#filter-category').value;
}

export function populateTransactionFormOptions(selectedCardId = '') {
  fillSelect($('#tx-from-account'), buildAccountOptions({ kinds: ['cash', 'bank', 'savings'], emptyLabel: 'Selecciona una cuenta' }), $('#tx-from-account').value);
  fillSelect($('#tx-to-account'), buildAccountOptions({ kinds: ['cash', 'bank', 'savings'], emptyLabel: 'Sin cuenta destino' }), $('#tx-to-account').value);
  fillSelect(
    $('#tx-card-id'),
    buildVisibleCardOptions({
      emptyLabel: 'Selecciona una tarjeta',
      selectedCardId: selectedCardId || $('#tx-card-id').value
    }),
    selectedCardId || $('#tx-card-id').value
  );
  fillSelect(
    $('#tx-linked-debt'),
    [{ value: '', label: 'Selecciona una deuda' }].concat(
      getSnapshot().debts.map((item) => ({ value: item.id, label: item.name }))
    ),
    $('#tx-linked-debt').value
  );
  fillSelect(
    $('#tx-linked-goal'),
    [{ value: '', label: 'Selecciona una meta' }].concat(
      getSnapshot().goals.map((item) => ({ value: item.id, label: item.name }))
    ),
    $('#tx-linked-goal').value
  );
  updateTransactionCategoryOptions();
  updateCardStatementOptions();
}

export function populateGoalAccountOptions() {
  fillSelect(
    $('#goal-account-id'),
    buildAccountOptions({ kinds: ['savings', 'bank', 'cash'], emptyLabel: 'Sin cuenta asociada' }),
    $('#goal-account-id').value
  );
}

export function populateDebtAccountOptions() {
  fillSelect(
    $('#debt-account-id'),
    buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Sin cuenta asociada' }),
    $('#debt-account-id').value
  );
}

export function populateRecurringAccountOptions() {
  fillSelect(
    $('#recurring-account-id'),
    buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Selecciona una cuenta' }),
    $('#recurring-account-id').value
  );
}

export function populateRecurringDayOptions(selectedDay = '1') {
  const options = Array.from({ length: 28 }, (_, index) => {
    const day = String(index + 1);
    return { value: day, label: `Dia ${day}` };
  });
  fillSelect($('#recurring-day'), options, String(selectedDay || '1'));
}

export function populateCardPaymentOptions() {
  fillSelect(
    $('#card-payment-account-id'),
    buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Selecciona una cuenta' }),
    $('#card-payment-account-id').value
  );
}

export function populateSettingsOptions() {
  fillSelect(
    $('#settings-primary-salary'),
    [{ value: '', label: 'Selecciona un recurrente' }].concat(
      getSnapshot().recurring
        .filter((item) => item.type === 'income')
        .map((item) => ({ value: item.id, label: `${item.description} / dia ${item.dayOfMonth}` }))
    ),
    getSnapshot().settings.financialCycleConfig.primarySalaryRecurringId
  );

  fillSelect(
    $('#settings-savings-account'),
    buildAccountOptions({ kinds: ['savings'], emptyLabel: 'Selecciona una cuenta' }),
    getSnapshot().settings.financialCycleConfig.savingsAccountId
  );

  fillSelect(
    $('#settings-sweep-source-account'),
    buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Selecciona una cuenta' }),
    getSnapshot().settings.financialCycleConfig.sweepSourceAccountId
  );

  $('#settings-savings-sweep-enabled').checked = getSnapshot().settings.financialCycleConfig.savingsSweepEnabled !== false;
  $('#settings-sweep-mode').value = getSnapshot().settings.financialCycleConfig.savingsSweepMode || 'suggest';
  renderLiquidAccountsCheckboxes();
}

export function renderLiquidAccountsCheckboxes() {
  const container = $('#settings-liquid-accounts');
  const config = getSnapshot().settings.financialCycleConfig;
  const liquidIds = new Set(config.liquidAccountIds || []);
  const accounts = getSnapshot().accounts.filter((account) => ['cash', 'bank'].includes(account.kind) && !account.archived);

  if (!accounts.length) {
    container.innerHTML = '<p class="soft-note">Primero crea cuentas de tipo banco o efectivo.</p>';
    return;
  }

  container.innerHTML = accounts
    .map(
      (account) => `
        <label class="checkbox-chip">
          <input type="checkbox" value="${escapeHtml(account.id)}" ${liquidIds.has(account.id) ? 'checked' : ''}>
          <span>${escapeHtml(account.name)}</span>
        </label>
      `
    )
    .join('');
}

export function updateTransactionCategoryOptions() {
  const type = $('#tx-type').value || 'expense';
  let options = [];

  if (type === 'expense' || type === 'income' || type === 'card_charge') {
    const desiredType = type === 'income' ? 'income' : 'expense';
    options = getSnapshot().categories
      .filter((item) => item.type === desiredType)
      .map((item) => ({ value: item.id, label: item.name }));
  } else if (type === 'debt_payment' || type === 'card_payment') {
    options = [{ value: 'debt-payment', label: 'Pago de deuda' }];
  } else if (type === 'goal_contribution') {
    options = [{ value: 'goal-contribution', label: 'Aporte a meta' }];
  } else {
    options = [{ value: '', label: 'Sin categoria' }];
  }

  fillSelect($('#tx-category'), options, $('#tx-category').value || options[0]?.value || '');
}

export function updateCardStatementOptions() {
  const cardId = $('#tx-card-id').value;
  const openStatements = cardId
    ? getOpenStatementsForCard(cardId)
        .sort((a, b) => FinanceDB.compareDate(a.dueDate || '9999-12-31', b.dueDate || '9999-12-31'))
    : [];
  const options = [{ value: '', label: 'Aplicar al saldo pendiente mas antiguo' }].concat(
    openStatements.map((statement) => ({
      value: statement.statementCycleKey,
      label: `${formatStatementLabel(statement)} / ${formatCurrency(statement.pendingAmount)}`
    }))
  );
  fillSelect($('#tx-card-statement'), options, $('#tx-card-statement').value);
}

export function populateInstallmentCountOptions(selectedValue = '1') {
  const options = Array.from({ length: 24 }, (_, index) => {
    const value = String(index + 1);
    return {
      value,
      label: index === 0 ? '1 cuota' : `${value} cuotas`
    };
  });
  fillSelect($('#tx-installment-count'), options, String(selectedValue || '1'));
}

export function updateInstallmentPreview() {
  const preview = $('#tx-installment-preview');
  if (!preview) return;
  const type = $('#tx-type').value || 'expense';
  if (type !== 'card_charge') {
    preview.textContent = 'Las cuotas solo aplican a compras con tarjeta.';
    return;
  }

  const card = getCardMap().get($('#tx-card-id').value || '');
  const rawDate = $('#tx-date').value || getTodayDate();
  const amount = parseFloat($('#tx-amount').value || '0') || 0;
  const installmentCount = Math.max(1, parseInt($('#tx-installment-count').value || '1', 10) || 1);
  if (!card) {
    preview.textContent = 'Elige una tarjeta para calcular el primer cierre y el sueldo al que ira la compra.';
    return;
  }

  const firstClosing = FinanceDB.getStatementClosingDate(rawDate, card.closingDay);
  const lastClosing = FinanceDB.addMonths(firstClosing, installmentCount - 1, card.closingDay);
  const projectedCycle = [...getSnapshot().cycles]
    .sort((a, b) => FinanceDB.compareDate(a.startDate, b.startDate))
    .find((cycle) => FinanceDB.compareDate(cycle.startDate, firstClosing) >= 0) || null;
  const perInstallment = installmentCount > 0 ? FinanceDB.roundAmount(amount / installmentCount) : 0;
  preview.textContent = installmentCount === 1
    ? `Ira al cierre del ${formatDate(firstClosing)} y se cubrira con ${projectedCycle ? getCycleLabelById(projectedCycle.id) : 'el siguiente sueldo disponible'}.`
    : `Se crearan ${installmentCount} cuotas. La primera cierra el ${formatDate(firstClosing)}, la ultima el ${formatDate(lastClosing)}. Monto aproximado por cuota: ${formatCurrency(perInstallment)}.`;
}

export function updateTransactionFields() {
  const type = $('#tx-type').value || 'expense';
  const fromField = $('#field-tx-from-account');
  const toField = $('#field-tx-to-account');
  const categoryField = $('#field-tx-category');
  const linkedRow = $('#transaction-linked-row');
  const installmentsRow = $('#tx-installments-row');
  const cardField = $('#field-tx-card');
  const statementField = $('#field-tx-card-statement');
  const debtField = $('#field-tx-linked-debt');
  const goalField = $('#field-tx-linked-goal');

  [fromField, toField, categoryField].forEach((field) => field.classList.remove('hidden'));
  linkedRow.classList.add('hidden');
  installmentsRow.classList.add('hidden');
  [cardField, statementField, debtField, goalField].forEach((field) => field.classList.add('hidden'));

  if (type === 'expense') {
    toField.classList.add('hidden');
  } else if (type === 'income') {
    fromField.classList.add('hidden');
  } else if (type === 'transfer') {
    categoryField.classList.add('hidden');
  } else if (type === 'card_charge') {
    fromField.classList.add('hidden');
    toField.classList.add('hidden');
    linkedRow.classList.remove('hidden');
    installmentsRow.classList.remove('hidden');
    cardField.classList.remove('hidden');
  } else if (type === 'card_payment') {
    toField.classList.add('hidden');
    categoryField.classList.add('hidden');
    linkedRow.classList.remove('hidden');
    cardField.classList.remove('hidden');
    statementField.classList.remove('hidden');
  } else if (type === 'debt_payment') {
    toField.classList.add('hidden');
    categoryField.classList.add('hidden');
    linkedRow.classList.remove('hidden');
    debtField.classList.remove('hidden');
  } else if (type === 'goal_contribution') {
    categoryField.classList.add('hidden');
    linkedRow.classList.remove('hidden');
    goalField.classList.remove('hidden');
  }

  updateTransactionCategoryOptions();
  updateCardStatementOptions();
  updateInstallmentPreview();
}

export function updateRecurringFields() {
  const isPrimarySalary = $('#recurring-is-primary-salary').checked;
  const type = $('#recurring-type').value;
  const categoryField = $('#field-recurring-category');
  const endMonthLabel = $('#recurring-end-month-label');

  if (type !== 'income' && isPrimarySalary) {
    $('#recurring-is-primary-salary').checked = false;
  }

  if ($('#recurring-is-primary-salary').checked) {
    categoryField.classList.add('hidden');
    $('#recurring-type').value = 'income';
  } else {
    categoryField.classList.remove('hidden');
  }

  const desiredType = $('#recurring-type').value === 'income' ? 'income' : 'expense';
  endMonthLabel.textContent = desiredType === 'income' ? 'Hasta que mes llega' : 'Hasta que mes se paga';
  const options = getSnapshot().categories
    .filter((item) => item.type === desiredType)
    .map((item) => ({ value: item.id, label: item.name }));
  fillSelect($('#recurring-category-id'), options, $('#recurring-category-id').value || (desiredType === 'income' ? 'other-income' : 'other-expense'));
}
