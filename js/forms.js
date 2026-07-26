/* Envio de formularios, backups y exportaciones. */
import { $, $$, closeModal, openConfirm, showToast, withFormLock } from './dom.js';
import { formatInstallmentLabel, formatSourceLabel, getAccountLabel, getCardLabel, getCategoryLabel, getCycleLabelById } from './format.js';
import { openTransactionModal } from './modals.js';
import { refreshData } from './render.js';
import { getSnapshot } from './state.js';
import { applyTheme } from './theme.js';

export async function saveTheme(theme) {
  await FinanceDB.saveSettings({ theme });
  applyTheme(theme);
  showToast(`Tema ${theme === 'light' ? 'claro' : 'oscuro'} activo`, 'success');
  await refreshData();
}

export function resolveAccountIdFromHint(hint) {
  const needle = String(hint || '').trim().toLowerCase();
  if (!needle) return '';
  const account = getSnapshot().accounts.find((item) => item.name.toLowerCase().includes(needle));
  return account?.id || '';
}

export function draftToPreset(draft) {
  const fallbackAccountId = getSnapshot().accounts.find((a) => a.kind === 'bank' && !a.archived)?.id || getSnapshot().accounts[0]?.id || '';
  return {
    type: draft.suggestedType,
    amount: draft.amount,
    date: draft.date,
    description: draft.description,
    categoryId: draft.categoryId,
    fromAccountId: draft.suggestedType === 'expense' ? resolveAccountIdFromHint(draft.accountHint) || fallbackAccountId : '',
    toAccountId: draft.suggestedType === 'income' ? resolveAccountIdFromHint(draft.accountHint) || fallbackAccountId : '',
    notes: [draft.notes, draft.debtHint ? `Posible deuda: ${draft.debtHint}` : '', `Confianza IA: ${draft.confidence}`]
      .filter(Boolean)
      .join(' / '),
    sourceType: draft.sourceType
  };
}

export async function handleTransactionSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;

  await withFormLock(form, async () => {
    try {
    const type = $('#tx-type').value;
    const payload = {
      id: form.dataset.editId || '',
      type,
      amount: $('#tx-amount').value,
      date: $('#tx-date').value,
      description: $('#tx-description').value.trim(),
      categoryId: $('#tx-category').value,
      fromAccountId: $('#tx-from-account').value,
      toAccountId: $('#tx-to-account').value,
      cardId: $('#tx-card-id').value,
      statementCycleKey: $('#tx-card-statement').value,
      installmentCount: $('#tx-installment-count').value,
      linkedEntityId:
        type === 'debt_payment'
          ? $('#tx-linked-debt').value
          : type === 'goal_contribution'
            ? $('#tx-linked-goal').value
            : '',
      linkedEntityType:
        type === 'debt_payment'
          ? 'debt'
          : type === 'goal_contribution'
            ? 'goal'
            : '',
      notes: $('#tx-notes').value.trim(),
      sourceType: form.dataset.sourceType || 'manual'
    };

    if (type === 'card_charge') {
      payload.fromAccountId = '';
      payload.toAccountId = '';
      payload.purchaseDate = payload.date;
    }
    if (type === 'card_payment') {
      payload.linkedEntityId = '';
      payload.linkedEntityType = '';
    }

    await FinanceDB.saveTransaction(payload);
    closeModal('modal-transaction');
    form.reset();
    form.dataset.editId = '';
    form.dataset.sourceType = 'manual';
    await refreshData();
    showToast('Movimiento guardado', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleAISubmit(event) {
  event.preventDefault();
  const button = $('#btn-ai-analyze');
  const file = $('#ai-file-input').files?.[0];
  const text = $('#ai-text-input').value.trim();
  const apiKey = localStorage.getItem('openai_api_key') || '';

  if (!file && !text) {
    showToast('Adjunta un archivo o pega texto para analizar.', 'error');
    return;
  }

  try {
    button.disabled = true;
    button.textContent = 'Analizando...';
    let source;
    if (file) {
      source = await FinanceAI.prepareSourceFromFile(file);
    } else {
      source = { kind: 'text', text, sourceType: 'text-email' };
    }

    const draft = await FinanceAI.analyzeSource(source, apiKey, {
      categories: getSnapshot().categories,
      accounts: getSnapshot().accounts,
      debts: getSnapshot().debts
    });
    closeModal('modal-ai');
    openTransactionModal({ preset: draftToPreset(draft) });
    showToast('Borrador creado. Revisa todo antes de guardar.', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  } finally {
    button.disabled = false;
    button.textContent = 'Analizar';
  }
}

export async function handleAccountSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  await withFormLock(form, async () => { try {
    await FinanceDB.saveAccount({
      id: form.dataset.editId || '',
      name: $('#account-name').value.trim(),
      kind: $('#account-kind').value,
      openingBalance: $('#account-opening-balance').value,
      includeInNetWorth: $('#account-include-networth').checked,
      archived: $('#account-archived').checked
    });
    closeModal('modal-account');
    await refreshData();
    showToast('Cuenta guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleCardSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  await withFormLock(form, async () => { try {
    await FinanceDB.saveCard({
      id: form.dataset.editId || '',
      bankName: $('#card-bank-name').value.trim(),
      last4: $('#card-last4').value.trim(),
      closingDay: $('#card-closing-day').value,
      dueDay: $('#card-due-day').value,
      paymentAccountId: $('#card-payment-account-id').value,
      creditLimit: $('#card-credit-limit').value,
      openingDebtAmount: $('#card-opening-debt').value,
      archived: $('#card-archived').checked
    });
    closeModal('modal-card');
    await refreshData();
    showToast('Tarjeta guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleGoalSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  await withFormLock(form, async () => { try {
    await FinanceDB.saveGoal({
      id: form.dataset.editId || '',
      name: $('#goal-name').value.trim(),
      targetAmount: $('#goal-target-amount').value,
      currentAmount: $('#goal-current-amount').value,
      targetDate: $('#goal-target-date').value,
      accountId: $('#goal-account-id').value,
      archived: $('#goal-archived').checked
    });
    closeModal('modal-goal');
    await refreshData();
    showToast('Meta guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleDebtSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  await withFormLock(form, async () => { try {
    await FinanceDB.saveDebt({
      id: form.dataset.editId || '',
      name: $('#debt-name').value.trim(),
      kind: $('#debt-kind').value,
      dueDay: $('#debt-due-day').value,
      totalAmount: $('#debt-total-amount').value,
      outstandingAmount: $('#debt-outstanding-amount').value,
      minimumPayment: $('#debt-minimum-payment').value,
      accountId: $('#debt-account-id').value,
      installmentCount: $('#debt-installment-count').value,
      installmentsPaid: $('#debt-installments-paid').value,
      archived: $('#debt-archived').checked
    });
    closeModal('modal-debt');
    await refreshData();
    showToast('Deuda guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleRecurringSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  await withFormLock(form, async () => { try {
    const isPrimarySalary = $('#recurring-is-primary-salary').checked;
    const recurringType = $('#recurring-type').value;
    const accountId = $('#recurring-account-id').value;
    const startDate = $('#recurring-start-date').value;
    const endMonth = $('#recurring-end-month').value;
    if (isPrimarySalary && recurringType !== 'income') {
      throw new Error('El sueldo principal debe ser un recurrente de ingreso.');
    }
    if (!accountId) {
      throw new Error('Elige la cuenta donde cae o se paga este recurrente.');
    }
    if (startDate && endMonth && startDate.slice(0, 7) > endMonth) {
      throw new Error('El mes final no puede ser anterior al inicio.');
    }

    await FinanceDB.saveRecurring({
      id: form.dataset.editId || '',
      type: recurringType,
      dayOfMonth: $('#recurring-day').value,
      startDate,
      endMonth,
      amount: $('#recurring-amount').value,
      description: $('#recurring-description').value.trim(),
      categoryId: isPrimarySalary ? 'salary' : $('#recurring-category-id').value,
      accountId,
      active: $('#recurring-active').checked,
      isPrimarySalary,
      opensFinancialCycle: isPrimarySalary,
      savingsSweepEnabled: true
    });
    closeModal('modal-recurring');
    await refreshData();
    showToast('Recurrente guardado', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
  });
}

export async function handleSaveCycleConfig(event) {
  event.preventDefault();
  const selectedRecurringId = $('#settings-primary-salary').value;
  const recurring = getSnapshot().recurring;
  const currentPrimary = recurring.find((item) => item.isPrimarySalary);
  const selectedRecurring = recurring.find((item) => item.id === selectedRecurringId) || null;

  try {
    if (selectedRecurringId && !selectedRecurring) {
      throw new Error('Elige un recurrente valido para el sueldo principal.');
    }
    if (selectedRecurring && selectedRecurring.type !== 'income') {
      throw new Error('El sueldo principal debe ser un recurrente de ingreso.');
    }

    if (currentPrimary && currentPrimary.id !== selectedRecurringId) {
      await FinanceDB.saveRecurring({ ...currentPrimary, isPrimarySalary: false, opensFinancialCycle: false });
    }
    if (selectedRecurring && !selectedRecurring.isPrimarySalary) {
      await FinanceDB.saveRecurring({ ...selectedRecurring, isPrimarySalary: true, opensFinancialCycle: true });
    }

    const liquidAccountIds = $$('#settings-liquid-accounts input[type="checkbox"]:checked').map((input) => input.value);
    await FinanceDB.configureFinancialCycle({
      primarySalaryRecurringId: selectedRecurringId,
      savingsAccountId: $('#settings-savings-account').value,
      sweepSourceAccountId: $('#settings-sweep-source-account').value,
      liquidAccountIds,
      onboardingCompleted: !!selectedRecurringId,
      savingsSweepEnabled: $('#settings-savings-sweep-enabled').checked,
      savingsSweepMode: $('#settings-sweep-mode').value
    });
    await refreshData();
    showToast('Configuracion del ciclo guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
}

export async function handleExportJson() {
  try {
    const payload = await FinanceDB.exportBackup();
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `finanzas-locales-backup-${FinanceDB.getToday()}.json`;
    link.click();
    URL.revokeObjectURL(url);
    await FinanceDB.saveSettings({ lastBackupAt: new Date().toISOString() });
    await refreshData();
    showToast('Backup JSON exportado', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
}

export async function handleImportJson(event) {
  const file = event.target.files?.[0];
  if (!file) return;

  try {
    const text = await file.text();
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== 'object') {
      throw new Error('El archivo no contiene un JSON valido.');
    }
    const hasExpectedData = parsed.transactions || parsed.accounts || parsed.settings || parsed.categories;
    if (!hasExpectedData) {
      throw new Error('El archivo no parece ser un backup de Finanzas Locales. Debe contener al menos transactions, accounts, settings o categories.');
    }

    const incomingCount = (parsed.transactions || []).length;
    const currentCount = getSnapshot().transactions.length;
    event.target.value = '';

    openConfirm({
      title: 'Reemplazar todos tus datos',
      text: `Importar este backup borra lo que tienes ahora (${currentCount} movimientos) y lo reemplaza por el contenido del archivo (${incomingCount} movimientos). Si algo falla, se restauran tus datos actuales.`,
      onAccept: async () => {
        try {
          await FinanceDB.importBackup(parsed);
          closeModal('modal-settings');
          await refreshData({ preserveSelectedCycle: false });
          showToast('Backup importado', 'success');
        } catch (error) {
          await refreshData({ preserveSelectedCycle: false });
          showToast(error.message, 'error');
        }
      }
    });
  } catch (error) {
    event.target.value = '';
    showToast(error.message, 'error');
  }
}

export function buildCsvContent() {
  const rows = [
    ['id', 'fecha', 'fecha_compra', 'tipo', 'monto', 'descripcion', 'categoria', 'cuenta_origen', 'cuenta_destino', 'tarjeta', 'estado_cuenta', 'cuota', 'ciclo', 'origen']
  ];
  getSnapshot().transactions.forEach((transaction) => {
    rows.push([
      transaction.id,
      transaction.date,
      transaction.purchaseDate || transaction.date,
      transaction.type,
      FinanceDB.roundAmount(transaction.amount).toFixed(2),
      transaction.description,
      getCategoryLabel(transaction.categoryId),
      getAccountLabel(transaction.fromAccountId),
      getAccountLabel(transaction.toAccountId),
      getCardLabel(transaction.cardId),
      transaction.statementCycleKey,
      formatInstallmentLabel(transaction),
      getCycleLabelById(transaction.budgetCycleId),
      formatSourceLabel(transaction.sourceType)
    ]);
  });
  return rows
    .map((row) => row.map((cell) => `"${String(cell ?? '').replace(/"/g, '""')}"`).join(','))
    .join('\n');
}

export function handleExportCsv() {
  try {
    const csv = buildCsvContent();
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `finanzas-locales-${FinanceDB.getToday()}.csv`;
    link.click();
    URL.revokeObjectURL(url);
    showToast('CSV exportado', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
}
