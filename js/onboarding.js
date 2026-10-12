/* Bienvenida guiada: deja la app lista en un minuto, sin palabras tecnicas. */
import { $, $$, closeModal, openModal, showToast } from './dom.js';
import { canPromptInstall, isInstalled, openInstallGuide, promptInstall } from './install.js';
import { parseAmount } from './quickadd.js';
import { refreshData } from './render.js';
import { getSnapshot } from './state.js';

let step = 0;

function steps() {
  return $$('#modal-onboarding [data-ob-step]').filter(
    (element) => !(element.dataset.obStep === 'install' && isInstalled())
  );
}

export function shouldShowOnboarding() {
  const snapshot = getSnapshot();
  return (
    !snapshot.settings.onboardingDone &&
    !snapshot.transactions.length &&
    !snapshot.recurring.length &&
    !snapshot.cards.length
  );
}

function render() {
  const list = steps();
  step = Math.max(0, Math.min(step, list.length - 1));
  $$('#modal-onboarding [data-ob-step]').forEach((element) => element.classList.add('hidden'));
  list[step].classList.remove('hidden');
  $('#ob-dots').innerHTML = list
    .map((_, index) => `<span class="ob-dot ${index === step ? 'active' : index < step ? 'done' : ''}"></span>`)
    .join('');
  $('#ob-back').classList.toggle('hidden', step === 0);
  $('.ob-nav').classList.toggle('is-first', step === 0);
  const isLast = step === list.length - 1;
  $('#ob-next').textContent = step === 0 ? 'Empezar' : isLast ? 'Terminar' : 'Siguiente';
  $('#ob-salary-fields').classList.toggle('hidden', !$('#ob-has-salary').checked);
  const installButton = $('#ob-install');
  installButton.textContent = canPromptInstall() ? 'Instalar ahora' : 'Ver cómo instalarla';
}

export function openOnboarding() {
  step = 0;
  const days = Array.from({ length: 28 }, (_, index) => index + 1);
  // La app maneja hasta el dia 28 para que exista en todos los meses.
  $('#ob-salary-day').innerHTML = days
    .map((day) => `<option value="${day}">${day === 28 ? 'Dia 28 o fin de mes' : `Dia ${day}`}</option>`)
    .join('');
  $('#ob-salary-day').value = '28';
  render();
  openModal('modal-onboarding');
}

function primaryAccountId(kind) {
  const accounts = getSnapshot().accounts.filter((account) => !account.archived);
  return (
    accounts.find((account) => account.id === `${kind}-main`)?.id ||
    accounts.find((account) => account.kind === kind)?.id ||
    accounts[0]?.id ||
    ''
  );
}

async function finish() {
  const button = $('#ob-next');
  button.disabled = true;
  try {
    const hasSalary = $('#ob-has-salary').checked;
    const salaryAmount = parseAmount($('#ob-salary-amount').value);
    if (hasSalary && salaryAmount > 0) {
      await FinanceDB.saveRecurring({
        type: 'income',
        amount: salaryAmount,
        dayOfMonth: $('#ob-salary-day').value,
        description: 'Sueldo',
        categoryId: 'salary',
        accountId: primaryAccountId('bank'),
        isPrimarySalary: true,
        active: true
      });
    }

    // "Cuanto tienes hoy" manda: el saldo inicial se ajusta para que, despues
    // del sueldo que la app acaba de registrar, la cuenta muestre justo eso.
    const desired = [
      [primaryAccountId('cash'), $('#ob-cash').value],
      [primaryAccountId('bank'), $('#ob-bank').value]
    ].filter(([id, raw]) => id && String(raw).trim() !== '');
    if (desired.length) {
      const snapshot = await FinanceDB.getFinanceSnapshot();
      for (const [id, raw] of desired) {
        const account = snapshot.accounts.find((item) => item.id === id);
        if (!account) continue;
        const movements = FinanceDB.roundAmount((snapshot.balances[id] || 0) - account.openingBalance);
        await FinanceDB.saveAccount({ ...account, openingBalance: FinanceDB.roundAmount(parseAmount(raw) - movements) });
      }
    }

    const budget = parseAmount($('#ob-budget').value);
    await FinanceDB.saveSettings({
      onboardingDone: true,
      uiMode: getSnapshot().settings.uiMode || 'simple',
      ...(budget > 0 ? { monthlyBudget: budget } : {})
    });
    closeModal('modal-onboarding');
    await refreshData({ preserveSelectedCycle: false });
    showToast('¡Todo listo! Anota tu primer gasto con el botón +', 'success', { duration: 4500 });
  } catch (error) {
    showToast(error.message, 'error');
  } finally {
    button.disabled = false;
  }
}

async function skip() {
  await FinanceDB.saveSettings({ onboardingDone: true, uiMode: getSnapshot().settings.uiMode || 'simple' });
  closeModal('modal-onboarding');
  await refreshData();
}

export function bindOnboardingEvents() {
  $('#ob-next').addEventListener('click', () => {
    if (step >= steps().length - 1) {
      finish();
      return;
    }
    step += 1;
    render();
  });
  $('#ob-back').addEventListener('click', () => {
    step -= 1;
    render();
  });
  $('#ob-skip').addEventListener('click', skip);
  $('#ob-has-salary').addEventListener('change', render);
  $('#ob-install').addEventListener('click', async () => {
    if (canPromptInstall()) {
      await promptInstall();
      render();
    } else {
      openInstallGuide();
    }
  });
  $('#ob-restore').addEventListener('click', () => {
    closeModal('modal-onboarding');
    $('#backup-file-input').click();
  });
}
