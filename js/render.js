/* Orquestador de render y recarga de datos. */
import { $$ } from './dom.js';
import { loadSettingsIntoInputs, populateSelects, updateRecurringFields, updateTransactionFields } from './options.js';
import { state } from './state.js';
import { applyTheme } from './theme.js';
import { renderAccounts } from './views/accounts.js';
import { renderCards } from './views/cards.js';
import { renderDashboard } from './views/dashboard.js';
import { renderAlerts, renderBudgetProgress, renderCycleConfigPreview, renderCycleHistory, renderHeader, renderHero, renderInstallCard, renderSweepSuggestion } from './views/hero.js';
import { renderAgenda, renderDebts, renderGoals, renderRecurring } from './views/plan.js';
import { renderBackupStatus, renderQuickFilters, renderStorageStatus, renderTransactions } from './views/transactions.js';

export function renderAll() {
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
  animateSections();
}

// La animacion de entrada corre una sola vez: repetirla en cada render
// hacia parpadear la app entera al cambiar de ciclo o guardar un movimiento.

// La animacion de entrada corre una sola vez: repetirla en cada render
// hacia parpadear la app entera al cambiar de ciclo o guardar un movimiento.
export let hasAnimatedOnce = false;

export function animateSections() {
  if (hasAnimatedOnce) return;
  hasAnimatedOnce = true;

  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  const elements = $$('.section-block, .hero-card, .alerts-card, .dashboard-card, .panel-card');
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
