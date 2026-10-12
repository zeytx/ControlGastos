/* Orquestador de render y recarga de datos. */
import { $$ } from './dom.js';
import { renderNotificationStatus, runCardDueCheck } from './notifications.js';
import { loadSettingsIntoInputs, populateSelects, updateRecurringFields, updateTransactionFields } from './options.js';
import { isSimpleMode, state } from './state.js';
import { applyTheme } from './theme.js';
import { renderAccounts } from './views/accounts.js';
import { renderCards } from './views/cards.js';
import { renderDashboard } from './views/dashboard.js';
import { renderHome } from './views/home.js';
import { renderAlerts, renderBudgetProgress, renderCycleConfigPreview, renderCycleHistory, renderHeader, renderHero, renderInstallCard, renderSweepSuggestion } from './views/hero.js';
import { renderAgenda, renderDebts, renderGoals, renderRecurring } from './views/plan.js';
import { renderBackupStatus, renderQuickFilters, renderStorageStatus, renderTransactions } from './views/transactions.js';

/** Modo sencillo y letra grande se aplican con clases en <body>: el CSS hace el resto. */
export function applyUiPreferences() {
  const settings = state.snapshot?.settings || {};
  const simple = isSimpleMode();
  document.body.classList.toggle('mode-simple', simple);
  document.body.classList.toggle('mode-full', !simple);
  document.documentElement.classList.toggle('text-large', settings.textSize === 'large');
  document.body.classList.toggle('has-cards', !!state.snapshot?.cards?.some((card) => !card.archived));
  document.body.classList.toggle('has-cycles', !!state.snapshot?.cycles?.length);
}

export function renderAll() {
  applyUiPreferences();
  renderHome();
  renderInstallCard();
  renderHeader();
  renderHero();
  renderBudgetProgress();
  renderSweepSuggestion();
  renderCycleHistory();
  renderDashboard();
  renderAlerts();
  renderCycleConfigPreview();
  renderAccounts();
  renderCards();
  renderGoals();
  renderDebts();
  renderRecurring();
  renderAgenda();
  renderTransactions();
  renderQuickFilters();
  renderStorageStatus();
  renderBackupStatus();
  updateTransactionFields();
  updateRecurringFields();
  renderNotificationStatus();
  animateSections();
}

// La animacion de entrada corre una sola vez: repetirla en cada render
// hacia parpadear la app entera al cambiar de ciclo o guardar un movimiento.
export let hasAnimatedOnce = false;

export function animateSections() {
  if (hasAnimatedOnce) return;
  hasAnimatedOnce = true;

  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  const elements = $$('.home-card, .section-block, .hero-card, .alerts-card, .dashboard-card, .panel-card');
  elements.forEach((el, i) => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(16px)';
    setTimeout(() => {
      el.style.transition = 'opacity 0.45s var(--ease-out-expo), transform 0.5s var(--ease-out-expo)';
      el.style.opacity = '1';
      el.style.transform = 'translateY(0)';
    }, 30 + i * 25);
  });
}

export async function refreshData({ preserveSelectedCycle = true } = {}) {
  const previousCycleId = state.selectedCycleId;
  state.snapshot = await FinanceDB.getFinanceSnapshot();

  if (preserveSelectedCycle && previousCycleId && state.snapshot.cycles.some((cycle) => cycle.id === previousCycleId)) {
    state.selectedCycleId = previousCycleId;
  } else {
    state.selectedCycleId = state.snapshot.currentCycleId || state.snapshot.cycles[0]?.id || '';
  }

  applyTheme(state.snapshot.settings.theme || localStorage.getItem('theme') || 'dark');
  loadSettingsIntoInputs();
  populateSelects();
  renderAll();
}
