/* Arranque de la app y cableado de eventos. */
import { $, $$, bindSwipeToDismiss, closeModal, openConfirm, openModal, showToast } from './dom.js';
import { getCardLabel, isInstallmentGroupTransaction } from './format.js';
import { handleAISubmit, handleAccountSubmit, handleCardSubmit, handleDebtSubmit, handleExportCsv, handleExportJson, handleGoalSubmit, handleImportJson, handleRecurringSubmit, handleSaveCycleConfig, handleTransactionSubmit, saveTheme } from './forms.js';
import { initNavigation } from './navigation.js';
import { openAIModal, openAccountModal, openCardModal, openCardStatementsModal, openDebtModal, openGoalModal, openRecurringModal, openTransactionModal, renderTransactionDetail } from './modals.js';
import { loadSettingsIntoInputs, populateSettingsOptions, setDefaultFormDates, updateCardStatementOptions, updateInstallmentPreview, updateRecurringFields, updateTransactionFields } from './options.js';
import { refreshData, renderAll } from './render.js';
import { getSelectedCycle, getSnapshot, getTransactionById, state } from './state.js';
import { renderQuickFilters, renderTransactions } from './views/transactions.js';

export function syncPlatformState() {
  const userAgent = navigator.userAgent || '';
  const touchMac = navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1;
  state.platform.isIOS = /iPad|iPhone|iPod/.test(userAgent) || touchMac;
  state.platform.standalone =
    window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
}

export async function requestPersistentStorage() {
  if (!navigator.storage || typeof navigator.storage.estimate !== 'function') {
    state.storage = { persisted: null, estimate: null };
    return;
  }

  const estimate = await navigator.storage.estimate().catch(() => null);
  let persisted = null;
  if (typeof navigator.storage.persisted === 'function') {
    persisted = await navigator.storage.persisted().catch(() => null);
  }
  if (persisted === false && typeof navigator.storage.persist === 'function') {
    persisted = await navigator.storage.persist().catch(() => false);
  }

  state.storage = { persisted, estimate };
}

export async function init() {
  syncPlatformState();
  initNavigation();
  bindEvents();
  setDefaultFormDates();
  await FinanceDB.initDB();
  await requestPersistentStorage();
  await refreshData({ preserveSelectedCycle: false });
  registerServiceWorker();
}

export function changeCycle(direction) {
  const cycles = getSnapshot().cycles;
  const index = cycles.findIndex((cycle) => cycle.id === state.selectedCycleId);
  if (index === -1) return;
  const nextIndex = index + direction;
  if (nextIndex < 0 || nextIndex >= cycles.length) return;
  state.selectedCycleId = cycles[nextIndex].id;
  renderAll();
}

export function handleAccountsGridClick(event) {
  const button = event.target.closest('[data-account-action]');
  if (!button) return;
  const account = getSnapshot().accounts.find((item) => item.id === button.dataset.accountId);
  if (!account) return;
  openAccountModal(account);
}

export function handleCardsGridClick(event) {
  const button = event.target.closest('[data-card-action]');
  if (!button) return;
  const card = getSnapshot().cards.find((item) => item.id === button.dataset.cardId);
  if (!card) return;

  if (button.dataset.cardAction === 'edit') {
    openCardModal(card);
    return;
  }
  if (button.dataset.cardAction === 'statements') {
    openCardStatementsModal(card.id);
    return;
  }
  if (button.dataset.cardAction === 'charge') {
    openTransactionModal({
      preset: {
        type: 'card_charge',
        cardId: card.id,
        categoryId: 'shopping',
        description: `Compra ${getCardLabel(card.id)}`,
        date: FinanceDB.getToday(),
        installmentCount: 1,
        sourceType: 'manual'
      }
    });
    return;
  }
  if (button.dataset.cardAction === 'pay') {
    openTransactionModal({
      preset: {
        type: 'card_payment',
        cardId: card.id,
        fromAccountId: card.paymentAccountId || getSnapshot().settings.financialCycleConfig.sweepSourceAccountId || '',
        categoryId: 'debt-payment',
        description: `Pago ${getCardLabel(card.id)}`,
        date: FinanceDB.getToday(),
        sourceType: 'manual'
      }
    });
  }
}

export function handleGoalsGridClick(event) {
  const button = event.target.closest('[data-goal-action]');
  if (!button) return;
  const goal = getSnapshot().goals.find((item) => item.id === button.dataset.goalId);
  if (!goal) return;
  if (button.dataset.goalAction === 'edit') {
    openGoalModal(goal);
    return;
  }
  if (button.dataset.goalAction === 'contribute') {
    openTransactionModal({
      preset: {
        type: 'goal_contribution',
        description: `Aporte a ${goal.name}`,
        linkedEntityId: goal.id,
        linkedEntityType: 'goal',
        toAccountId: goal.accountId || '',
        fromAccountId: getSnapshot().settings.financialCycleConfig.sweepSourceAccountId || '',
        categoryId: 'goal-contribution'
      }
    });
  }
}

export function handleDebtsGridClick(event) {
  const button = event.target.closest('[data-debt-action]');
  if (!button) return;
  const debt = getSnapshot().debts.find((item) => item.id === button.dataset.debtId);
  if (!debt) return;
  if (button.dataset.debtAction === 'edit') {
    openDebtModal(debt);
    return;
  }
  if (button.dataset.debtAction === 'pay') {
    openTransactionModal({
      preset: {
        type: 'debt_payment',
        description: `Pago ${debt.name}`,
        amount: FinanceDB.roundAmount(
          Math.min(debt.minimumPayment || debt.outstandingAmount, debt.outstandingAmount)
        ),
        linkedEntityId: debt.id,
        linkedEntityType: 'debt',
        fromAccountId: debt.accountId || getSnapshot().settings.financialCycleConfig.sweepSourceAccountId || '',
        categoryId: 'debt-payment'
      }
    });
  }
}

export function handleRecurringGridClick(event) {
  const button = event.target.closest('[data-recurring-action]');
  if (!button) return;
  const recurring = getSnapshot().recurring.find((item) => item.id === button.dataset.recurringId);
  if (!recurring) return;
  if (button.dataset.recurringAction === 'edit') {
    openRecurringModal(recurring);
    return;
  }
  if (button.dataset.recurringAction === 'delete') {
    openConfirm({
      title: 'Eliminar recurrente',
      text: `Se eliminara "${recurring.description}".`,
      onAccept: async () => {
        await FinanceDB.deleteRecurring(recurring.id);
        await refreshData();
        showToast('Recurrente eliminado', 'success');
      }
    });
  }
}

export function handleTransactionsListClick(event) {
  const card = event.target.closest('[data-transaction-id]');
  if (!card) return;
  const transaction = getTransactionById(card.dataset.transactionId);
  if (!transaction) return;
  state.ui.detailTransactionId = transaction.id;
  renderTransactionDetail(transaction);
  openModal('modal-detail');
}

export function bindDeleteButton(selector, { modalId, label, remove, doneMessage }) {
  $(selector).addEventListener('click', (event) => {
    const button = event.currentTarget;
    const id = button.dataset.entityId;
    if (!id) return;
    const name = button.dataset.entityName || label;
    openConfirm({
      title: `Eliminar ${label}`,
      text: `Se eliminara "${name}". Esta accion no se puede deshacer.`,
      onAccept: async () => {
        try {
          await remove(id);
          closeModal(modalId);
          await refreshData();
          showToast(doneMessage, 'success');
        } catch (error) {
          showToast(error.message, 'error');
        }
      }
    });
  });
}

export function bindEvents() {
  bindSwipeToDismiss();

  bindDeleteButton('#btn-delete-account', {
    modalId: 'modal-account',
    label: 'cuenta',
    remove: (id) => FinanceDB.deleteAccount(id),
    doneMessage: 'Cuenta eliminada'
  });
  bindDeleteButton('#btn-delete-card', {
    modalId: 'modal-card',
    label: 'tarjeta',
    remove: (id) => FinanceDB.deleteCard(id),
    doneMessage: 'Tarjeta eliminada'
  });
  bindDeleteButton('#btn-delete-goal', {
    modalId: 'modal-goal',
    label: 'meta',
    remove: (id) => FinanceDB.deleteGoal(id),
    doneMessage: 'Meta eliminada'
  });
  bindDeleteButton('#btn-delete-debt', {
    modalId: 'modal-debt',
    label: 'deuda',
    remove: (id) => FinanceDB.deleteDebt(id),
    doneMessage: 'Deuda eliminada'
  });

  $('#btn-confirm-sweep').addEventListener('click', async () => {
    const cycle = getSelectedCycle();
    if (!cycle) return;
    try {
      await FinanceDB.confirmCycleSweep(cycle.id);
      await refreshData();
      showToast('Traslado a ahorro registrado', 'success');
    } catch (error) {
      showToast(error.message, 'error');
    }
  });

  $('#btn-undo-sweep').addEventListener('click', () => {
    const cycle = getSelectedCycle();
    if (!cycle) return;
    openConfirm({
      title: 'Deshacer traslado',
      text: 'Se eliminara la transferencia a ahorro de este ciclo.',
      onAccept: async () => {
        try {
          await FinanceDB.undoCycleSweep(cycle.id);
          await refreshData();
          showToast('Traslado deshecho', 'success');
        } catch (error) {
          showToast(error.message, 'error');
        }
      }
    });
  });

  $('#fab-add').addEventListener('click', () => openModal('modal-actions'));
  $('#btn-open-actions').addEventListener('click', () => openModal('modal-actions'));
  $('#btn-open-settings').addEventListener('click', () => {
    loadSettingsIntoInputs();
    populateSettingsOptions();
    openModal('modal-settings');
  });
  $('#btn-open-cycle-settings').addEventListener('click', () => {
    loadSettingsIntoInputs();
    populateSettingsOptions();
    openModal('modal-settings');
  });

  $('#btn-prev-cycle').addEventListener('click', () => changeCycle(1));
  $('#btn-next-cycle').addEventListener('click', () => changeCycle(-1));
  $('#cycle-history-select').addEventListener('change', (event) => {
    state.selectedCycleId = event.target.value;
    renderAll();
  });

  $('#filter-search').addEventListener('input', (event) => {
    state.filters.search = event.target.value;
    renderTransactions();
  });
  $('#filter-type').addEventListener('change', (event) => {
    state.filters.type = event.target.value;
    renderTransactions();
    renderQuickFilters();
  });
  $('#filter-account').addEventListener('change', (event) => {
    state.filters.accountId = event.target.value;
    renderTransactions();
  });
  $('#filter-card').addEventListener('change', (event) => {
    state.filters.cardId = event.target.value;
    renderTransactions();
  });
  $('#filter-category').addEventListener('change', (event) => {
    state.filters.categoryId = event.target.value;
    renderTransactions();
  });

  $('#quick-filter-row').addEventListener('click', (event) => {
    const button = event.target.closest('[data-quick-filter]');
    if (!button) return;
    state.filters.type = button.dataset.quickFilter || 'all';
    $('#filter-type').value = state.filters.type;
    renderTransactions();
    renderQuickFilters();
  });

  $('#btn-add-account').addEventListener('click', () => openAccountModal());
  $('#btn-add-card').addEventListener('click', () => openCardModal());
  $('#btn-add-goal').addEventListener('click', () => openGoalModal());
  $('#btn-add-debt').addEventListener('click', () => openDebtModal());
  $('#btn-add-recurring').addEventListener('click', () => openRecurringModal());

  $('#action-add-transaction').addEventListener('click', () => openTransactionModal());
  $('#action-add-ai').addEventListener('click', () => openAIModal());
  $('#action-add-card').addEventListener('click', () => openCardModal());
  $('#action-add-goal').addEventListener('click', () => openGoalModal());
  $('#action-add-debt').addEventListener('click', () => openDebtModal());
  $('#action-add-recurring').addEventListener('click', () => openRecurringModal());
  $('#action-add-account').addEventListener('click', () => openAccountModal());

  $('#tx-type').addEventListener('change', updateTransactionFields);
  $('#tx-card-id').addEventListener('change', () => {
    updateCardStatementOptions();
    updateInstallmentPreview();
  });
  $('#tx-date').addEventListener('change', updateInstallmentPreview);
  $('#tx-amount').addEventListener('input', updateInstallmentPreview);
  $('#tx-installment-count').addEventListener('change', updateInstallmentPreview);
  $('#recurring-type').addEventListener('change', updateRecurringFields);
  $('#recurring-is-primary-salary').addEventListener('change', updateRecurringFields);
  $('#recurring-start-date').addEventListener('change', (event) => {
    const value = event.target.value;
    if (!value) return;
    const day = Math.max(1, Math.min(28, parseInt(value.slice(8, 10), 10) || 1));
    $('#recurring-day').value = String(day);
  });

  $('#transaction-form').addEventListener('submit', handleTransactionSubmit);
  $('#ai-form').addEventListener('submit', handleAISubmit);
  $('#account-form').addEventListener('submit', handleAccountSubmit);
  $('#card-form').addEventListener('submit', handleCardSubmit);
  $('#goal-form').addEventListener('submit', handleGoalSubmit);
  $('#debt-form').addEventListener('submit', handleDebtSubmit);
  $('#recurring-form').addEventListener('submit', handleRecurringSubmit);

  $('#accounts-grid').addEventListener('click', handleAccountsGridClick);
  $('#cards-summary-grid').addEventListener('click', handleCardsGridClick);
  $('#goals-grid').addEventListener('click', handleGoalsGridClick);
  $('#debts-grid').addEventListener('click', handleDebtsGridClick);
  $('#recurring-grid').addEventListener('click', handleRecurringGridClick);
  $('#transactions-list').addEventListener('click', handleTransactionsListClick);

  $('#btn-detail-edit').addEventListener('click', () => {
    const transaction = getTransactionById(state.ui.detailTransactionId);
    if (!transaction) return;
    if (isInstallmentGroupTransaction(transaction)) {
      showToast('Las compras en cuotas se corrigen eliminando la compra completa y registrandola de nuevo.', 'error');
      return;
    }
    closeModal('modal-detail');
    openTransactionModal({ transaction });
  });

  $('#btn-detail-delete').addEventListener('click', () => {
    const transaction = getTransactionById(state.ui.detailTransactionId);
    if (!transaction) return;
    const isInstallmentGroup = isInstallmentGroupTransaction(transaction);
    openConfirm({
      title: 'Eliminar movimiento',
      text: isInstallmentGroup
        ? `Se eliminara la compra completa "${transaction.description}" con todas sus cuotas. Esta accion no se puede deshacer.`
        : `Se eliminara "${transaction.description}". Esta accion no se puede deshacer.`,
      onAccept: async () => {
        await FinanceDB.deleteTransaction(transaction.id);
        closeModal('modal-detail');
        await refreshData();
        showToast('Movimiento eliminado', 'success');
      }
    });
  });

  $('#btn-confirm-cancel').addEventListener('click', () => closeModal('modal-confirm'));
  $('#btn-confirm-accept').addEventListener('click', async () => {
    const action = state.ui.pendingConfirm;
    state.ui.pendingConfirm = null;
    closeModal('modal-confirm');
    if (typeof action === 'function') {
      await action();
    }
  });

  $('#btn-install-dismiss').addEventListener('click', async () => {
    await FinanceDB.saveSettings({ installPromptDismissedAt: new Date().toISOString() });
    await refreshData();
  });

  $('#btn-save-cycle-config').addEventListener('click', handleSaveCycleConfig);
  $('#btn-save-budget').addEventListener('click', async () => {
    try {
      await FinanceDB.saveSettings({ monthlyBudget: $('#settings-budget-input').value });
      await refreshData();
      showToast('Tope actualizado', 'success');
    } catch (error) {
      showToast(error.message, 'error');
    }
  });

  $('#btn-save-api-key').addEventListener('click', (event) => {
    event.preventDefault();
    const value = $('#settings-api-key').value.trim();
    if (value && !value.startsWith('sk-')) {
      showToast('La API key debe empezar con "sk-"', 'error');
      return;
    }
    if (value) localStorage.setItem('openai_api_key', value);
    else localStorage.removeItem('openai_api_key');
    showToast(value ? 'API key guardada localmente' : 'API key eliminada', 'success');
  });

  $('#btn-toggle-api-key').addEventListener('click', (event) => {
    event.preventDefault();
    const input = $('#settings-api-key');
    input.type = input.type === 'password' ? 'text' : 'password';
  });

  $('#btn-theme-dark').addEventListener('click', async () => saveTheme('dark'));
  $('#btn-theme-light').addEventListener('click', async () => saveTheme('light'));

  $('#btn-export-json').addEventListener('click', handleExportJson);
  $('#btn-import-json').addEventListener('click', () => $('#backup-file-input').click());
  $('#backup-file-input').addEventListener('change', handleImportJson);
  $('#btn-export-csv').addEventListener('click', handleExportCsv);
  $('#btn-clear-data').addEventListener('click', () => {
    openConfirm({
      title: 'Borrar todo',
      text: 'Se eliminaran movimientos, cuentas, tarjetas, metas, deudas, recurrentes y ajustes financieros locales.',
      onAccept: async () => {
        await FinanceDB.clearAllData();
        await refreshData({ preserveSelectedCycle: false });
        showToast('Datos financieros borrados', 'success');
      }
    });
  });

  $$('.modal-overlay').forEach((overlay) => {
    overlay.addEventListener('click', (event) => {
      if (event.target === overlay) closeModal(overlay.id);
    });
  });

  $$('[data-close-modal]').forEach((button) => {
    button.addEventListener('click', () => closeModal(button.dataset.closeModal));
  });

  const scrollTopBtn = $('#scroll-top');
  if (scrollTopBtn) {
    let scrollTicking = false;
    window.addEventListener('scroll', () => {
      if (!scrollTicking) {
        requestAnimationFrame(() => {
          scrollTopBtn.classList.toggle('hidden', window.scrollY < 400);
          scrollTicking = false;
        });
        scrollTicking = true;
      }
    }, { passive: true });
    scrollTopBtn.addEventListener('click', () => window.scrollTo({ top: 0, behavior: 'smooth' }));
  }
}

export function registerServiceWorker() {
  if (!('serviceWorker' in navigator)) return;

  window.addEventListener('load', async () => {
    try {
      const registration = await navigator.serviceWorker.register('sw.js');

      registration.addEventListener('updatefound', () => {
        const installing = registration.installing;
        if (!installing) return;
        installing.addEventListener('statechange', () => {
          // Solo avisamos si ya habia una version corriendo: en la primera
          // instalacion no hay nada que actualizar.
          if (installing.state === 'installed' && navigator.serviceWorker.controller) {
            showToast('Hay una version nueva. Cierra y vuelve a abrir la app.', 'success');
          }
        });
      });
    } catch (error) {
      // Sin service worker la app sigue funcionando, solo pierde el modo offline.
    }
  });
}

window.addEventListener('DOMContentLoaded', () => {
  init().catch((error) => {
    console.error(error);
  });
});
