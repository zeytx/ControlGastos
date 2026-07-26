/* Apertura y precarga de cada modal. */
import { TYPE_LABELS } from './constants.js';
import { $, closeModal, escapeHtml, openModal, renderEmptyPanel, toggleDeleteButton } from './dom.js';
import { describeTransactionAccounts, formatCurrency, formatDate, formatInstallmentLabel, formatLongDate, formatSourceLabel, formatStatementLabel, getCardLabel, getCategoryLabel, getCycleLabelById, isInstallmentGroupTransaction } from './format.js';
import { populateCardPaymentOptions, populateDebtAccountOptions, populateGoalAccountOptions, populateInstallmentCountOptions, populateRecurringAccountOptions, populateRecurringDayOptions, populateTransactionFormOptions, setDefaultFormDates, updateCardStatementOptions, updateRecurringFields, updateTransactionFields } from './options.js';
import { getCardMap, getCardStatements, getSnapshot } from './state.js';

export function openTransactionModal({ transaction = null, preset = null } = {}) {
  closeModal('modal-actions');
  const form = $('#transaction-form');
  form.reset();
  setDefaultFormDates();
  form.dataset.editId = transaction?.id || '';
  form.dataset.sourceType = transaction?.sourceType || preset?.sourceType || 'manual';

  const sourceBadge = $('#tx-source-badge');
  if (form.dataset.sourceType && form.dataset.sourceType !== 'manual') {
    sourceBadge.textContent = formatSourceLabel(form.dataset.sourceType);
    sourceBadge.classList.remove('hidden');
  } else {
    sourceBadge.textContent = '';
    sourceBadge.classList.add('hidden');
  }

  const base = transaction || preset || {};
  $('#transaction-modal-title').textContent = transaction ? 'Editar movimiento' : 'Nuevo movimiento';
  $('#tx-type').value = base.type || 'expense';
  $('#tx-amount').value = base.amount ? FinanceDB.roundAmount(base.amount) : '';
  $('#tx-date').value = base.date || FinanceDB.getToday();
  $('#tx-description').value = base.description || '';
  $('#tx-notes').value = base.notes || '';
  populateTransactionFormOptions();
  populateInstallmentCountOptions(base.installmentCount || 1);
  $('#tx-category').value = base.categoryId || $('#tx-category').value;
  $('#tx-from-account').value = base.fromAccountId || $('#tx-from-account').value;
  $('#tx-to-account').value = base.toAccountId || $('#tx-to-account').value;
  $('#tx-card-id').value = base.cardId || '';
  updateCardStatementOptions();
  $('#tx-card-statement').value = base.statementCycleKey || '';
  $('#tx-installment-count').value = String(base.installmentCount || 1);
  $('#tx-linked-debt').value = base.linkedEntityType === 'debt' ? base.linkedEntityId || '' : '';
  $('#tx-linked-goal').value = base.linkedEntityType === 'goal' ? base.linkedEntityId || '' : '';
  updateTransactionFields();
  openModal('modal-transaction');
}

export function openAIModal() {
  closeModal('modal-actions');
  $('#ai-form').reset();
  openModal('modal-ai');
}

export function openAccountModal(account = null) {
  closeModal('modal-actions');
  const form = $('#account-form');
  form.reset();
  form.dataset.editId = account?.id || '';
  toggleDeleteButton('#btn-delete-account', account);
  $('#account-modal-title').textContent = account ? 'Editar cuenta' : 'Nueva cuenta';
  $('#account-name').value = account?.name || '';
  $('#account-kind').value = account?.kind || 'bank';
  $('#account-opening-balance').value = account ? FinanceDB.roundAmount(account.openingBalance) : '';
  $('#account-include-networth').checked = account ? account.includeInNetWorth !== false : true;
  $('#account-archived').checked = account?.archived || false;
  openModal('modal-account');
}

export function openCardModal(card = null) {
  closeModal('modal-actions');
  const form = $('#card-form');
  form.reset();
  form.dataset.editId = card?.id || '';
  toggleDeleteButton('#btn-delete-card', card ? { id: card.id, name: getCardLabel(card.id) } : null);
  $('#card-modal-title').textContent = card ? 'Editar tarjeta' : 'Nueva tarjeta';
  $('#card-bank-name').value = card?.bankName || '';
  $('#card-last4').value = card?.last4 || '';
  $('#card-closing-day').value = card?.closingDay || 10;
  $('#card-due-day').value = card?.dueDay || 25;
  populateCardPaymentOptions();
  $('#card-payment-account-id').value = card?.paymentAccountId || '';
  $('#card-credit-limit').value = card ? FinanceDB.roundAmount(card.creditLimit || 0) : '';
  $('#card-opening-debt').value = card ? FinanceDB.roundAmount(card.openingDebtAmount || 0) : 0;
  $('#card-archived').checked = card?.archived || false;
  openModal('modal-card');
}

export function openGoalModal(goal = null) {
  closeModal('modal-actions');
  const form = $('#goal-form');
  form.reset();
  form.dataset.editId = goal?.id || '';
  toggleDeleteButton('#btn-delete-goal', goal);
  $('#goal-modal-title').textContent = goal ? 'Editar meta' : 'Nueva meta';
  $('#goal-name').value = goal?.name || '';
  $('#goal-target-amount').value = goal ? FinanceDB.roundAmount(goal.targetAmount) : '';
  $('#goal-current-amount').value = goal ? FinanceDB.roundAmount(goal.currentAmount) : 0;
  $('#goal-target-date').value = goal?.targetDate || '';
  populateGoalAccountOptions();
  $('#goal-account-id').value = goal?.accountId || '';
  $('#goal-archived').checked = goal?.archived || false;
  openModal('modal-goal');
}

export function openDebtModal(debt = null) {
  closeModal('modal-actions');
  const form = $('#debt-form');
  form.reset();
  form.dataset.editId = debt?.id || '';
  toggleDeleteButton('#btn-delete-debt', debt);
  $('#debt-modal-title').textContent = debt ? 'Editar deuda' : 'Nueva deuda';
  $('#debt-name').value = debt?.name || '';
  $('#debt-kind').value = debt?.kind || 'loan';
  $('#debt-due-day').value = debt?.dueDay || 1;
  $('#debt-total-amount').value = debt ? FinanceDB.roundAmount(debt.totalAmount) : '';
  $('#debt-outstanding-amount').value = debt ? FinanceDB.roundAmount(debt.outstandingAmount) : '';
  $('#debt-minimum-payment').value = debt ? FinanceDB.roundAmount(debt.minimumPayment) : 0;
  populateDebtAccountOptions();
  $('#debt-account-id').value = debt?.accountId || '';
  $('#debt-installment-count').value = debt?.installmentCount || 0;
  $('#debt-installments-paid').value = debt?.installmentsPaid || 0;
  $('#debt-archived').checked = debt?.archived || false;
  openModal('modal-debt');
}

export function openRecurringModal(recurring = null) {
  closeModal('modal-actions');
  const form = $('#recurring-form');
  form.reset();
  form.dataset.editId = recurring?.id || '';
  $('#recurring-modal-title').textContent = recurring ? 'Editar recurrente' : 'Nuevo recurrente';
  $('#recurring-type').value = recurring?.type || 'expense';
  populateRecurringDayOptions(recurring?.dayOfMonth || 1);
  $('#recurring-start-date').value = recurring?.startDate || '';
  $('#recurring-end-month').value = recurring?.endMonth || '';
  $('#recurring-amount').value = recurring ? FinanceDB.roundAmount(recurring.amount) : '';
  $('#recurring-description').value = recurring?.description || '';
  populateRecurringAccountOptions();
  $('#recurring-account-id').value = recurring?.accountId || '';
  updateRecurringFields();
  $('#recurring-category-id').value = recurring?.categoryId || $('#recurring-category-id').value;
  $('#recurring-is-primary-salary').checked = recurring?.isPrimarySalary || false;
  $('#recurring-active').checked = recurring ? recurring.active !== false : true;
  updateRecurringFields();
  openModal('modal-recurring');
}

export function openCardStatementsModal(cardId) {
  const card = getCardMap().get(cardId);
  if (!card) return;
  $('#card-statements-title').textContent = `Estados de ${getCardLabel(cardId)}`;
  const statements = getCardStatements(cardId);
  $('#card-statements-body').innerHTML = statements.length
    ? statements
        .map((statement) => `
          <div class="detail-card">
            <strong>${escapeHtml(formatStatementLabel(statement))}</strong>
            <p>${statement.closingDate ? `Cierra ${escapeHtml(formatDate(statement.closingDate))}` : 'Deuda previa al uso de la app'}</p>
            <p>${statement.dueDate ? `Vence ${escapeHtml(formatDate(statement.dueDate))}` : 'Sin fecha de pago registrada'}</p>
            <p>Cargado ${escapeHtml(formatCurrency(statement.chargedAmount))} / Pagado ${escapeHtml(formatCurrency(statement.paidAmount))} / Pendiente ${escapeHtml(formatCurrency(statement.pendingAmount))}</p>
            <p>${escapeHtml(statement.budgetCycleId ? getCycleLabelById(statement.budgetCycleId) : 'Sin sueldo asignado aun')}</p>
            ${statement.purchases?.length ? `
              <div class="detail-list">
                ${statement.purchases
                  .map((purchase) => `
                    <div class="detail-list-row">
                      <strong>${escapeHtml(purchase.description)}</strong>
                      <span>${escapeHtml(formatCurrency(purchase.amount))} ${escapeHtml(formatInstallmentLabel(purchase) || '')}</span>
                    </div>
                  `)
                  .join('')}
              </div>
            ` : ''}
          </div>
        `)
        .join('')
    : renderEmptyPanel('Sin estados aun', 'Registra compras con tarjeta para empezar a ver estados de cuenta.');
  openModal('modal-card-statements');
}

export function renderTransactionDetail(transaction) {
  const cycleLabel = transaction.budgetCycleId ? getCycleLabelById(transaction.budgetCycleId) : 'Sin ciclo';
  const body = $('#detail-body');
  const purchaseDate = transaction.purchaseDate || transaction.date;
  const groupedInstallment = isInstallmentGroupTransaction(transaction);
  const statement = transaction.statementCycleKey
    ? getSnapshot().statements.find(
        (item) => item.cardId === transaction.cardId && item.statementCycleKey === transaction.statementCycleKey
      ) || null
    : null;
  $('#detail-title').textContent = transaction.description;
  body.innerHTML = `
    <div class="detail-card">
      <strong>Tipo y monto</strong>
      <p>${escapeHtml(TYPE_LABELS[transaction.type] || transaction.type)} / ${escapeHtml(formatCurrency(transaction.amount))}</p>
    </div>
    <div class="detail-card">
      <strong>Fecha real</strong>
      <p>${escapeHtml(formatLongDate(purchaseDate))}</p>
    </div>
    <div class="detail-card">
      <strong>Ciclo al que pertenece</strong>
      <p>${escapeHtml(cycleLabel)}</p>
    </div>
    <div class="detail-card">
      <strong>Cuentas o tarjeta</strong>
      <p>${escapeHtml(describeTransactionAccounts(transaction) || 'Sin detalle')}</p>
    </div>
    <div class="detail-card">
      <strong>Categoria</strong>
      <p>${escapeHtml(getCategoryLabel(transaction.categoryId) || 'Sin categoria')}</p>
    </div>
    ${transaction.statementCycleKey ? `
      <div class="detail-card">
        <strong>Estado de cuenta</strong>
        <p>${escapeHtml(statement ? formatStatementLabel(statement) : formatDate(transaction.statementCycleKey))}</p>
      </div>
    ` : ''}
    ${transaction.type === 'card_charge' ? `
      <div class="detail-card">
        <strong>Cuotas</strong>
        <p>${escapeHtml(formatInstallmentLabel(transaction))} / compra total ${escapeHtml(formatCurrency(transaction.originalPurchaseAmount || transaction.amount))}</p>
      </div>
    ` : ''}
    <div class="detail-card">
      <strong>Origen</strong>
      <p>${escapeHtml(formatSourceLabel(transaction.sourceType))}</p>
    </div>
    ${transaction.notes ? `
      <div class="detail-card">
        <strong>Notas</strong>
        <p>${escapeHtml(transaction.notes)}</p>
      </div>
    ` : ''}
  `;
  $('#btn-detail-edit').disabled = groupedInstallment;
  $('#btn-detail-edit').title = groupedInstallment
    ? 'Las compras en cuotas se vuelven a registrar completas si necesitas corregirlas.'
    : '';
}
