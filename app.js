/* ============================================
   FINANZAS LOCALES - Main App
   Salary cycles + cards by statement cutoff
   ============================================ */

const App = (() => {
  const MONTH_NAMES = [
    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
  ];

  const SHORT_MONTH_NAMES = [
    'Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
    'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'
  ];

  const TYPE_LABELS = {
    expense: 'Gasto',
    income: 'Ingreso',
    transfer: 'Transferencia',
    card_charge: 'Compra con tarjeta',
    card_payment: 'Pago de tarjeta',
    debt_payment: 'Pago de deuda',
    goal_contribution: 'Aporte a meta'
  };

  const TYPE_ICONS = {
    expense: 'v',
    income: '^',
    transfer: '<>',
    card_charge: 'TC',
    card_payment: 'Pago',
    debt_payment: 'Deuda',
    goal_contribution: 'Meta'
  };

  const ACCOUNT_KIND_LABELS = {
    cash: 'Efectivo',
    bank: 'Banco',
    savings: 'Ahorros'
  };

  const DEBT_KIND_LABELS = {
    loan: 'Prestamo',
    installment: 'Cuotas',
    personal: 'Personal'
  };

  const state = {
    snapshot: null,
    selectedCycleId: '',
    filters: {
      search: '',
      type: 'all',
      accountId: 'all',
      categoryId: 'all',
      cardId: 'all'
    },
    ui: {
      detailTransactionId: '',
      pendingConfirm: null
    },
    storage: {
      persisted: null,
      estimate: null
    },
    platform: {
      isIOS: false,
      standalone: false
    }
  };

  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));

  function getSnapshot() {
    return state.snapshot || {
      settings: { financialCycleConfig: {} },
      categories: [],
      accounts: [],
      balances: {},
      debts: [],
      goals: [],
      recurring: [],
      cards: [],
      transactions: [],
      cycles: [],
      currentCycleId: '',
      currentCycle: null,
      projectionByCycle: {},
      statements: [],
      agenda: [],
      alerts: []
    };
  }

  function getSelectedCycle() {
    const snapshot = getSnapshot();
    return snapshot.cycles.find((cycle) => cycle.id === state.selectedCycleId) || snapshot.currentCycle || null;
  }

  function getSelectedCycleId() {
    return getSelectedCycle()?.id || '';
  }

  function getAccountMap() {
    return new Map(getSnapshot().accounts.map((item) => [item.id, item]));
  }

  function getCategoryMap() {
    return new Map(getSnapshot().categories.map((item) => [item.id, item]));
  }

  function getCardMap() {
    return new Map(getSnapshot().cards.map((item) => [item.id, item]));
  }

  function getDebtMap() {
    return new Map(getSnapshot().debts.map((item) => [item.id, item]));
  }

  function getGoalMap() {
    return new Map(getSnapshot().goals.map((item) => [item.id, item]));
  }

  function getRecurringMap() {
    return new Map(getSnapshot().recurring.map((item) => [item.id, item]));
  }

  function getTransactionById(id) {
    return getSnapshot().transactions.find((item) => item.id === id) || null;
  }

  function getCardStatements(cardId) {
    return getSnapshot().statements
      .filter((item) => item.cardId === cardId)
      .sort((a, b) => String(b.closingDate || '').localeCompare(String(a.closingDate || '')));
  }

  function getOpenStatementsForCard(cardId) {
    return getCardStatements(cardId).filter((item) => item.pendingAmount > 0);
  }

  function getSelectedCycleTransactions() {
    const cycleId = getSelectedCycleId();
    const snapshot = getSnapshot();
    if (!cycleId) {
      return snapshot.transactions
        .sort((a, b) => FinanceDB.compareDate(a.date, b.date));
    }
    return snapshot.transactions
      .filter((item) => item.budgetCycleId === cycleId)
      .sort((a, b) => FinanceDB.compareDate(a.date, b.date));
  }

  function getTodayDate() {
    return FinanceDB.getToday();
  }

  function getAccountProjectionView() {
    const cycleId = getSelectedCycleId();
    const projection = getSnapshot().projectionByCycle?.[cycleId] || null;
    if (!projection?.showProjection) {
      return {
        showProjection: false,
        label: '',
        balances: {},
        visibleNetWorth: 0
      };
    }

    return {
      showProjection: true,
      label: `Proyeccion al ${formatShortDate(projection.cutoffDate)}`,
      balances: projection.balances || {},
      visibleNetWorth: projection.visibleNetWorth || 0
    };
  }

  function sumAmounts(items, predicate) {
    return FinanceDB.roundAmount(
      items.reduce((sum, item) => sum + (predicate(item) ? item.amount : 0), 0)
    );
  }

  function getDashboardMetrics() {
    const cycle = getSelectedCycle();
    const transactions = getSelectedCycleTransactions();
    const cashSpend = sumAmounts(transactions, (item) => ['expense', 'debt_payment'].includes(item.type));
    const savingsFlow = FinanceDB.roundAmount(
      sumAmounts(transactions, (item) => item.type === 'goal_contribution') + (cycle?.sweptAmount || 0)
    );
    const cardCommitted = FinanceDB.roundAmount(cycle?.cardAssigned || 0);
    const totalCommitted = FinanceDB.roundAmount(cashSpend + savingsFlow + cardCommitted);
    const freeNet = FinanceDB.roundAmount(cycle?.freeNetAmount ?? 0);

    return {
      cycle,
      transactions,
      cashSpend,
      savingsFlow,
      cardCommitted,
      totalCommitted,
      freeNet,
      transactionCount: transactions.length
    };
  }

  function getCategoryBreakdown() {
    const transactions = getSelectedCycleTransactions().filter((item) =>
      ['expense', 'card_charge', 'debt_payment', 'goal_contribution'].includes(item.type)
    );

    const totals = new Map();
    transactions.forEach((transaction) => {
      const fallbackLabel =
        transaction.type === 'debt_payment'
          ? 'Pago de deuda'
          : transaction.type === 'goal_contribution'
            ? 'Ahorro'
            : transaction.type === 'card_charge'
              ? 'Compras con tarjeta'
              : 'Otros gastos';
      const label = getCategoryLabel(transaction.categoryId) || fallbackLabel;
      totals.set(label, FinanceDB.roundAmount((totals.get(label) || 0) + transaction.amount));
    });

    const total = Array.from(totals.values()).reduce((sum, value) => sum + value, 0);
    return Array.from(totals.entries())
      .map(([label, amount]) => ({
        label,
        amount,
        ratio: total > 0 ? amount / total : 0
      }))
      .sort((a, b) => b.amount - a.amount)
      .slice(0, 6);
  }

  function getCycleTrendPoints() {
    const cycle = getSelectedCycle();
    if (!cycle) return [];

    const relevantTransactions = getSelectedCycleTransactions().filter(
      (item) =>
        FinanceDB.compareDate(item.date, cycle.startDate) >= 0 &&
        FinanceDB.compareDate(item.date, cycle.endDate) <= 0 &&
        ['income', 'expense', 'debt_payment', 'goal_contribution', 'card_payment', 'card_charge'].includes(item.type)
    );

    const deltas = new Map();
    relevantTransactions.forEach((item) => {
      const sign = item.type === 'income' ? 1 : -1;
      deltas.set(item.date, FinanceDB.roundAmount((deltas.get(item.date) || 0) + item.amount * sign));
    });

    const points = [];
    let cursor = cycle.startDate;
    let running = 0;
    while (FinanceDB.compareDate(cursor, cycle.endDate) <= 0) {
      running = FinanceDB.roundAmount(running + (deltas.get(cursor) || 0));
      points.push({ date: cursor, value: running });
      cursor = FinanceDB.addDays(cursor, 1);
    }
    return points;
  }

  function getCardCoverageSummary() {
    const cycleId = getSelectedCycleId();
    const statements = getSnapshot().statements.filter((item) => item.budgetCycleId === cycleId);
    const assignedAmount = FinanceDB.roundAmount(
      statements.reduce((sum, statement) => sum + statement.chargedAmount, 0)
    );
    const paidAmount = FinanceDB.roundAmount(
      statements.reduce((sum, statement) => sum + statement.paidAmount, 0)
    );
    const pendingAmount = FinanceDB.roundAmount(
      statements.reduce((sum, statement) => sum + statement.pendingAmount, 0)
    );
    const ratio = assignedAmount > 0 ? Math.min(1, paidAmount / assignedAmount) : 0;

    return {
      assignedAmount,
      paidAmount,
      pendingAmount,
      ratio,
      items: statements
        .filter((statement) => statement.pendingAmount > 0)
        .sort((a, b) => FinanceDB.compareDate(a.dueDate || '9999-12-31', b.dueDate || '9999-12-31'))
        .slice(0, 4)
    };
  }

  function getUpcomingTimelinePreview() {
    return (getSnapshot().agenda || []).slice(0, 4);
  }

  function polarToCartesian(cx, cy, radius, angleInDegrees) {
    const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180;
    return {
      x: cx + radius * Math.cos(angleInRadians),
      y: cy + radius * Math.sin(angleInRadians)
    };
  }

  function describeArc(cx, cy, radius, startAngle, endAngle) {
    const start = polarToCartesian(cx, cy, radius, endAngle);
    const end = polarToCartesian(cx, cy, radius, startAngle);
    const largeArcFlag = endAngle - startAngle <= 180 ? '0' : '1';
    return `M ${start.x.toFixed(3)} ${start.y.toFixed(3)} A ${radius} ${radius} 0 ${largeArcFlag} 0 ${end.x.toFixed(3)} ${end.y.toFixed(3)}`;
  }

  function renderDonutSvg(segments, total, centerTop, centerBottom) {
    if (!total) {
      return `
        <svg viewBox="0 0 220 220" class="donut-figure" role="img" aria-label="Sin datos del ciclo">
          <circle cx="110" cy="110" r="76" class="donut-track"></circle>
          <circle cx="110" cy="110" r="52" class="donut-core"></circle>
          <text x="110" y="106" text-anchor="middle" class="donut-number">S/ 0</text>
          <text x="110" y="126" text-anchor="middle" class="donut-label">Sin datos</text>
        </svg>
      `;
    }

    let angle = 0;
    const paths = segments
      .filter((segment) => segment.value > 0)
      .map((segment) => {
        const sweep = Math.max(4, (segment.value / total) * 360);
        const path = describeArc(110, 110, 76, angle, angle + sweep);
        angle += sweep;
        return `<path d="${path}" class="donut-segment ${segment.className}"></path>`;
      })
      .join('');

    return `
      <svg viewBox="0 0 220 220" class="donut-figure" role="img" aria-label="Balance visual del ciclo">
        <circle cx="110" cy="110" r="76" class="donut-track"></circle>
        ${paths}
        <circle cx="110" cy="110" r="52" class="donut-core"></circle>
        <text x="110" y="104" text-anchor="middle" class="donut-number">${escapeHtml(centerTop)}</text>
        <text x="110" y="126" text-anchor="middle" class="donut-label">${escapeHtml(centerBottom)}</text>
      </svg>
    `;
  }

  function renderTrendSvg(points) {
    if (!points.length) {
      return '<div class="chart-empty">Aun no hay ritmo suficiente para graficar este ciclo.</div>';
    }

    const width = 520;
    const height = 200;
    const padding = 18;
    const values = points.map((point) => point.value);
    const min = Math.min(...values, 0);
    const max = Math.max(...values, 0);
    const range = max - min || 1;

    const coords = points.map((point, index) => {
      const x = padding + (index / Math.max(points.length - 1, 1)) * (width - padding * 2);
      const y = height - padding - ((point.value - min) / range) * (height - padding * 2);
      return { x, y, date: point.date, value: point.value };
    });

    const polyline = coords.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(' ');
    const area = `${padding},${height - padding} ${polyline} ${width - padding},${height - padding}`;
    const lastPoint = coords[coords.length - 1];

    return `
      <svg viewBox="0 0 ${width} ${height}" class="trend-svg" role="img" aria-label="Tendencia del ciclo">
        <defs>
          <linearGradient id="trend-fill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="var(--chart-card-fill)"></stop>
            <stop offset="100%" stop-color="rgba(0, 0, 0, 0)"></stop>
          </linearGradient>
        </defs>
        <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="trend-axis"></line>
        <polygon points="${area}" fill="url(#trend-fill)" class="trend-area"></polygon>
        <polyline points="${polyline}" class="trend-line"></polyline>
        <circle cx="${lastPoint.x.toFixed(2)}" cy="${lastPoint.y.toFixed(2)}" r="5" class="trend-dot"></circle>
      </svg>
    `;
  }

  function formatCurrency(value) {
    return `S/ ${FinanceDB.roundAmount(value).toFixed(2)}`;
  }

  function formatDate(value) {
    const raw = String(value || '').trim();
    if (!raw) return 'Sin fecha';
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const date = FinanceDB.normalizeDate(raw);
    const year = parseInt(date.slice(0, 4), 10);
    const month = parseInt(date.slice(5, 7), 10);
    const day = parseInt(date.slice(8, 10), 10);
    return `${day} ${SHORT_MONTH_NAMES[month - 1]} ${year}`;
  }

  function formatShortDate(value) {
    const raw = String(value || '').trim();
    if (!raw) return 'Sin fecha';
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const date = FinanceDB.normalizeDate(raw);
    const month = parseInt(date.slice(5, 7), 10);
    const day = parseInt(date.slice(8, 10), 10);
    return `${day} ${SHORT_MONTH_NAMES[month - 1]}`;
  }

  function formatLongDate(value) {
    const raw = String(value || '').trim();
    if (!raw) return 'Sin fecha';
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const date = FinanceDB.normalizeDate(raw);
    const year = parseInt(date.slice(0, 4), 10);
    const month = parseInt(date.slice(5, 7), 10);
    const day = parseInt(date.slice(8, 10), 10);
    return `${day} de ${MONTH_NAMES[month - 1]} de ${year}`;
  }

  function formatMonthLabel(value) {
    const raw = String(value || '').trim();
    const normalized = /^\d{4}-\d{2}$/.test(raw) ? `${raw}-01` : raw;
    const date = FinanceDB.normalizeDate(normalized);
    const year = parseInt(date.slice(0, 4), 10);
    const month = parseInt(date.slice(5, 7), 10);
    return `${MONTH_NAMES[month - 1]} ${year}`;
  }

  function formatCycleMonthLabel(cycle) {
    if (!cycle) return 'Sin ciclo';
    const date = FinanceDB.normalizeDate(cycle.startDate);
    const year = parseInt(date.slice(0, 4), 10);
    const month = parseInt(date.slice(5, 7), 10);
    return `${MONTH_NAMES[month - 1]} ${year}`;
  }

  function formatCycleRange(cycle) {
    if (!cycle) return '';
    return `Del ${formatShortDate(cycle.startDate)} al ${formatShortDate(cycle.endDate)}`;
  }

  function formatCycleOptionLabel(cycle) {
    if (!cycle) return 'Sin ciclo';
    return `${formatCycleMonthLabel(cycle)} / ${formatShortDate(cycle.startDate)} - ${formatShortDate(cycle.endDate)}`;
  }

  function formatCycleLabel(cycle) {
    if (!cycle) return 'Ciclo sin configurar';
    return `Ciclo ${formatCycleMonthLabel(cycle)}`;
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function fillSelect(select, options, selectedValue) {
    if (!select) return;
    const desiredValue = selectedValue ?? select.value;
    select.innerHTML = options
      .map((option) => `<option value="${escapeHtml(option.value)}">${escapeHtml(option.label)}</option>`)
      .join('');
    if (options.some((option) => option.value === desiredValue)) {
      select.value = desiredValue;
    } else if (options.length) {
      select.value = options[0].value;
    }
  }

  function buildAccountOptions({ includeArchived = false, kinds = null, emptyLabel = '' } = {}) {
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

  function buildCardOptions({ emptyLabel = '', includeArchived = false, selectedCardId = '' } = {}) {
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

  function getAccountLabel(accountId) {
    if (!accountId) return '';
    return getAccountMap().get(accountId)?.name || 'Cuenta no encontrada';
  }

  function getCategoryLabel(categoryId) {
    return getCategoryMap().get(categoryId)?.name || '';
  }

  function getCardDisplayLabel(card) {
    if (!card) return '';
    return `${card.bankName} • ${card.last4}`;
  }

  function buildVisibleCardOptions({ emptyLabel = '', includeArchived = false, selectedCardId = '' } = {}) {
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

  function getCardLabel(cardId) {
    const card = getCardMap().get(cardId);
    return card ? `${card.bankName} • ${card.last4}` : '';
  }

  function formatStatementLabel(statement) {
    if (!statement) return 'Sin estado';
    if (statement.kind === 'opening-debt') return 'Deuda inicial';
    if (statement.periodStart && statement.periodEnd) {
      return `${formatShortDate(statement.periodStart)} - ${formatShortDate(statement.periodEnd)}`;
    }
    return String(statement.label || '').replace(/Â/g, '').replace(/â€¢/g, '•');
  }

  function formatInstallmentLabel(transaction) {
    if (!transaction || transaction.type !== 'card_charge') return '';
    const count = Math.max(1, parseInt(transaction.installmentCount || 1, 10) || 1);
    const index = Math.max(1, parseInt(transaction.installmentIndex || 1, 10) || 1);
    if (count <= 1) return '1 cuota';
    return `Cuota ${index}/${count}`;
  }

  function isInstallmentGroupTransaction(transaction) {
    return !!(transaction?.type === 'card_charge' && transaction.installmentGroupId && transaction.installmentCount > 1);
  }

  function getCycleLabelById(cycleId) {
    const cycle = getSnapshot().cycles.find((item) => item.id === cycleId);
    return formatCycleLabel(cycle);
  }

  function describeTransactionAccounts(transaction) {
    if (transaction.type === 'expense') return `Sale de ${getAccountLabel(transaction.fromAccountId)}`;
    if (transaction.type === 'income') return `Entra a ${getAccountLabel(transaction.toAccountId)}`;
    if (transaction.type === 'transfer') return `${getAccountLabel(transaction.fromAccountId)} -> ${getAccountLabel(transaction.toAccountId)}`;
    if (transaction.type === 'card_charge') return getCardLabel(transaction.cardId);
    if (transaction.type === 'card_payment') return `Paga ${getCardLabel(transaction.cardId)} desde ${getAccountLabel(transaction.fromAccountId)}`;
    if (transaction.type === 'debt_payment') return `Pagado desde ${getAccountLabel(transaction.fromAccountId)}`;
    if (transaction.type === 'goal_contribution') return `${getAccountLabel(transaction.fromAccountId)} -> ${getAccountLabel(transaction.toAccountId)}`;
    return '';
  }

  function formatSourceLabel(sourceType) {
    const map = {
      manual: 'Manual',
      legacy: 'Migrado',
      recurring: 'Recurrente',
      system: 'Automatico',
      'image-upload': 'IA imagen',
      'text-email': 'IA correo',
      'pdf-text': 'IA PDF',
      'text-file': 'IA texto'
    };
    return map[sourceType] || sourceType || 'Manual';
  }

  function renderEmptyPanel(title, text) {
    return `
      <div class="empty-state">
        <div class="empty-icon">[]</div>
        <h3>${escapeHtml(title)}</h3>
        <p>${escapeHtml(text)}</p>
      </div>
    `;
  }

  function syncPlatformState() {
    const userAgent = navigator.userAgent || '';
    const touchMac = navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1;
    state.platform.isIOS = /iPad|iPhone|iPod/.test(userAgent) || touchMac;
    state.platform.standalone =
      window.matchMedia('(display-mode: standalone)').matches || window.navigator.standalone === true;
  }

  async function requestPersistentStorage() {
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

  async function init() {
    syncPlatformState();
    bindEvents();
    setDefaultFormDates();
    await FinanceDB.initDB();
    await requestPersistentStorage();
    await refreshData({ preserveSelectedCycle: false });
    registerServiceWorker();
  }

  async function refreshData({ preserveSelectedCycle = true } = {}) {
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

  function renderAll() {
    renderInstallCard();
    renderHeader();
    renderHero();
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

  function animateSections() {
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

  function renderInstallCard() {
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

  function renderHeader() {
    $('#header-subtitle').textContent = state.platform.standalone
      ? 'Modo app activa. Tus datos viven aqui en este dispositivo.'
      : 'Modo navegador. Instalala si quieres que se sienta como app local.';
  }

  function renderHero() {
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
      `<span class="chip ${config.savingsSweepEnabled ? 'positive' : 'warning'}">${config.savingsSweepEnabled ? 'Barrido activo' : 'Barrido pausado'}</span>`
    ].join('');
  }

  function renderCycleHistory() {
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

  function renderDashboard() {
    const dashboardMetrics = getDashboardMetrics();
    const categoryBreakdown = getCategoryBreakdown();
    const cycleTrendPoints = getCycleTrendPoints();
    const cardCoverageSummary = getCardCoverageSummary();
    const upcomingTimelinePreview = getUpcomingTimelinePreview();
    const cycle = dashboardMetrics.cycle;

    $('#dashboard-pill').textContent = cycle
      ? `${dashboardMetrics.transactionCount} movs. / libre ${formatCurrency(dashboardMetrics.freeNet)}`
      : 'Sin ciclo activo';

    if (!cycle) {
      const emptyMsg = '<div class="chart-empty">Configura un recurrente de ingreso como sueldo principal para activar el dashboard por ciclos.</div>';
      $('#balance-donut').innerHTML = emptyMsg;
      $('#balance-legend').innerHTML = '';
      $('#insights-list').innerHTML = emptyMsg;
      $('#coverage-fill').style.width = '0%';
      $('#coverage-badge').textContent = 'Sin datos';
      $('#coverage-badge').className = 'chip';
      $('#coverage-note').textContent = 'El dashboard se activa cuando configures tu sueldo principal en Ajustes.';
      $('#category-breakdown').innerHTML = '<div class="chart-empty">Registra gastos para ver el desglose por categorias.</div>';
      $('#cycle-trend-chart').innerHTML = '';
      $('#trend-summary').innerHTML = '<span class="trend-chip">Sin ciclo</span>';
      return;
    }

    const segments = [
      { label: 'Cash y deudas', value: dashboardMetrics.cashSpend, className: 'segment-expense', colorClass: 'legend-expense' },
      { label: 'Tarjetas del ciclo', value: dashboardMetrics.cardCommitted, className: 'segment-card', colorClass: 'legend-card' },
      { label: 'Ahorro', value: dashboardMetrics.savingsFlow, className: 'segment-saving', colorClass: 'legend-saving' }
    ];
    $('#balance-donut').innerHTML = renderDonutSvg(
      segments,
      dashboardMetrics.totalCommitted,
      formatCurrency(dashboardMetrics.freeNet),
      'Libre neto'
    );
    $('#balance-legend').innerHTML = segments
      .map(
        (segment) => `
          <div class="legend-row">
            <span class="legend-dot ${segment.colorClass}"></span>
            <div class="legend-copy">
              <strong>${escapeHtml(segment.label)}</strong>
              <span>${escapeHtml(formatCurrency(segment.value))}</span>
            </div>
          </div>
        `
      )
      .join('');

    const primarySalary = getRecurringMap().get(getSnapshot().settings.financialCycleConfig.primarySalaryRecurringId || '');
    const nextSalaryDate = cycle ? FinanceDB.addDays(cycle.endDate, 1) : '';
    const nextPendingCard = cardCoverageSummary.items[0] || null;
    const nextClosingCard = getSnapshot().cards
      .filter((card) => !card.archived)
      .sort((a, b) => FinanceDB.compareDate(a.nextClosingDate, b.nextClosingDate))[0] || null;
    const insightItems = [
      {
        kicker: 'Proximo sueldo',
        title: nextSalaryDate ? formatDate(nextSalaryDate) : 'Sin fecha',
        detail: primarySalary ? `${primarySalary.description} / dia ${primarySalary.dayOfMonth}` : 'Configura tu sueldo principal'
      },
      {
        kicker: 'Proximo corte',
        title: nextClosingCard ? getCardLabel(nextClosingCard.id) : 'Sin tarjetas',
        detail: nextClosingCard ? formatDate(nextClosingCard.nextClosingDate) : 'No hay corte registrado'
      },
      {
        kicker: 'Proximo pago',
        title: nextPendingCard ? formatCurrency(nextPendingCard.pendingAmount) : 'Al dia',
        detail: nextPendingCard
          ? `${getCardLabel(nextPendingCard.cardId)} / ${nextPendingCard.dueDate ? formatDate(nextPendingCard.dueDate) : 'Sin vencimiento definido'}`
          : 'Sin estados abiertos'
      },
      {
        kicker: 'Barrido a ahorro',
        title: cycle?.sweepTransferId ? formatCurrency(cycle.sweptAmount || 0) : formatCurrency(Math.max(0, dashboardMetrics.freeNet)),
        detail: cycle?.sweepTransferId ? 'Ya ejecutado en el siguiente sueldo' : 'Estimado del cierre actual'
      }
    ];
    $('#insights-list').innerHTML = insightItems
      .concat(
        upcomingTimelinePreview.map((item) => ({
          kicker: 'Agenda',
          title: item.title,
          detail: `${formatDate(item.date)} / ${item.subtitle}`
        }))
      )
      .slice(0, 6)
      .map(
        (item) => `
          <div class="insight-item">
            <span class="insight-kicker">${escapeHtml(item.kicker)}</span>
            <strong>${escapeHtml(item.title)}</strong>
            <span>${escapeHtml(item.detail)}</span>
          </div>
        `
      )
      .join('');

    $('#coverage-fill').style.width = `${Math.round(cardCoverageSummary.ratio * 100)}%`;
    $('#coverage-badge').textContent = `${Math.round(cardCoverageSummary.ratio * 100)}% cubierto`;
    $('#coverage-badge').className = `chip ${cardCoverageSummary.pendingAmount > 0 ? 'warning' : 'positive'}`;
    $('#coverage-note').textContent = cardCoverageSummary.assignedAmount > 0
      ? `${formatCurrency(cardCoverageSummary.paidAmount)} pagados de ${formatCurrency(cardCoverageSummary.assignedAmount)} asignados a este sueldo.`
      : 'Cuando haya estados de cuenta asignados a este sueldo apareceran aqui.';

    $('#category-breakdown').innerHTML = categoryBreakdown.length
      ? categoryBreakdown
          .map(
            (item) => `
              <div class="breakdown-row">
                <div class="breakdown-meta">
                  <strong>${escapeHtml(item.label)}</strong>
                  <span>${escapeHtml(formatCurrency(item.amount))}</span>
                </div>
                <div class="breakdown-bar">
                  <div class="breakdown-fill" style="width: ${(item.ratio * 100).toFixed(1)}%"></div>
                </div>
              </div>
            `
          )
          .join('')
      : '<div class="chart-empty">Todavia no hay suficiente movimiento clasificado para mostrar categorias.</div>';

    $('#cycle-trend-chart').innerHTML = renderTrendSvg(cycleTrendPoints);
    $('#trend-summary').innerHTML = cycleTrendPoints.length
      ? `
          <span class="trend-chip">${escapeHtml(formatShortDate(cycleTrendPoints[0].date))}</span>
          <span class="trend-chip">${escapeHtml(formatCurrency(cycleTrendPoints[Math.floor(cycleTrendPoints.length / 2)].value))}</span>
          <span class="trend-chip">${escapeHtml(formatShortDate(cycleTrendPoints[cycleTrendPoints.length - 1].date))}</span>
        `
      : '<span class="trend-chip">Sin ritmo aun</span>';
  }

  function buildBackupAlert() {
    const settings = getSnapshot().settings;
    const lastBackupAt = settings.lastBackupAt || '';
    const txCount = getSnapshot().transactions.length;

    if (!lastBackupAt && txCount > 5) {
      return {
        kind: 'warning',
        title: 'Aun no has hecho tu primer backup',
        text: `Tienes ${txCount} movimientos que se perderan si borras los datos del navegador. Ve a Ajustes > Exportar JSON.`
      };
    }

    if (lastBackupAt) {
      const lastDate = new Date(lastBackupAt);
      const now = new Date();
      const daysSince = Math.floor((now - lastDate) / 86400000);
      if (daysSince > 7) {
        return {
          kind: 'info',
          title: `Han pasado ${daysSince} dias desde tu ultimo backup`,
          text: `Ultimo backup: ${formatLongDate(lastBackupAt.slice(0, 10))}. Exporta un JSON nuevo para proteger tus datos.`
        };
      }
    }

    return null;
  }

  function renderAlerts() {
    const container = $('#alerts-list');
    const alerts = [...(getSnapshot().alerts || [])];
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

  function renderCycleConfigPreview() {
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
        <strong>Ahorro automatico</strong>
        <p>${config.savingsSweepEnabled ? `Activa hacia ${escapeHtml(getAccountLabel(config.savingsAccountId))}` : 'Pausada'}</p>
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

  function renderAccounts() {
    const container = $('#accounts-grid');
    const { accounts, balances } = getSnapshot();
    const projection = getAccountProjectionView();
    if (!accounts.length) {
      container.innerHTML = renderEmptyPanel('Aun no tienes cuentas', 'Crea una cuenta para empezar a distribuir tu dinero localmente.');
      return;
    }

    const visibleNetWorth = accounts
      .filter((account) => account.includeInNetWorth && !account.archived)
      .reduce((sum, account) => sum + (balances[account.id] || 0), 0);
    const projectedVisibleNetWorth = projection.showProjection ? projection.visibleNetWorth || 0 : 0;

    const summaryCard = `
      <article class="panel-card">
        <div class="panel-card-header">
          <div>
            <p class="eyebrow">Vista global</p>
            <h4 class="panel-card-title">Patrimonio visible hoy</h4>
          </div>
          <span class="chip positive">${accounts.filter((account) => !account.archived).length} activas</span>
        </div>
        <div class="balance-amount">${formatCurrency(visibleNetWorth)}</div>
        <p class="panel-card-subtitle">Saldo real al dia de hoy en tus cuentas visibles.</p>
        ${projection.showProjection ? `
          <div class="projection-box">
            <span class="projection-label">${escapeHtml(projection.label)}</span>
            <strong class="projection-amount">${escapeHtml(formatCurrency(projectedVisibleNetWorth))}</strong>
          </div>
        ` : ''}
      </article>
    `;

    const cards = accounts
      .map(
        (account) => {
          const currentBalance = balances[account.id] || 0;
          const projectedBalance = projection.showProjection ? (projection.balances[account.id] || 0) : currentBalance;
          return `
          <article class="panel-card" data-account-id="${escapeHtml(account.id)}">
            <div class="panel-card-header">
              <div>
                <h4 class="panel-card-title">${escapeHtml(account.name)}</h4>
                <span class="panel-card-subtitle">${escapeHtml(ACCOUNT_KIND_LABELS[account.kind] || account.kind)}</span>
              </div>
              <span class="chip ${account.archived ? 'warning' : 'positive'}">${account.archived ? 'Archivada' : 'Activa'}</span>
            </div>
            <div class="balance-amount">${formatCurrency(currentBalance)}</div>
            <p class="panel-card-subtitle">Disponible hoy</p>
            <div class="chips-row">
              <span class="chip">${account.includeInNetWorth ? 'Cuenta visible' : 'No suma patrimonio'}</span>
              <span class="chip">Inicial ${formatCurrency(account.openingBalance)}</span>
            </div>
            ${projection.showProjection ? `
              <div class="projection-box">
                <span class="projection-label">${escapeHtml(projection.label)}</span>
                <strong class="projection-amount">${escapeHtml(formatCurrency(projectedBalance))}</strong>
              </div>
            ` : ''}
            <div class="panel-actions">
              <button class="btn-secondary" data-account-action="edit" data-account-id="${escapeHtml(account.id)}">Editar</button>
            </div>
          </article>
        `;
        }
      )
      .join('');

    container.innerHTML = summaryCard + cards;
  }

  function renderCards() {
    const container = $('#cards-summary-grid');
    const cards = getSnapshot().cards;
    if (!cards.length) {
      container.innerHTML = renderEmptyPanel('Sin tarjetas aun', 'Registra banco, ultimos 4, corte y pago para asignar compras al sueldo correcto.');
      return;
    }

    container.innerHTML = cards
      .map((card) => {
        const statement = card.currentStatement;
        const assignedCycle = statement?.budgetCycleId ? getCycleLabelById(statement.budgetCycleId) : 'Sin sueldo asignado aun';
        const progress = statement?.chargedAmount ? Math.min(100, Math.round((statement.paidAmount / statement.chargedAmount) * 100)) : 0;
        const utilizationWidth = Math.max(0, Math.min(100, card.utilizationPct || 0));
        const statementLabel = statement ? formatStatementLabel(statement) : 'Aun no hay estado abierto para esta tarjeta.';
        return `
          <article class="panel-card" data-card-id="${escapeHtml(card.id)}">
            <div class="panel-card-header">
              <div>
                <h4 class="panel-card-title">${escapeHtml(getCardLabel(card.id))}</h4>
                <span class="panel-card-subtitle">Corte dia ${escapeHtml(card.closingDay)} / Pago dia ${escapeHtml(card.dueDay)}</span>
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
                <span class="metric-kicker">Linea total</span>
                <strong>${formatCurrency(card.creditLimit || 0)}</strong>
              </div>
            </div>
            <div class="progress-track compact-track"><div class="progress-fill" style="width: ${utilizationWidth}%"></div></div>
            <div class="utilization-copy">
              <span>Uso ${escapeHtml(`${FinanceDB.roundAmount(card.utilizationPct || 0).toFixed(1)}%`)}</span>
              <span>${escapeHtml(card.creditLimit > 0 ? `Deuda ${formatCurrency(card.currentDebt || 0)}` : 'Agrega la linea total')}</span>
            </div>
            <div class="chips-row">
              <span class="chip">Proximo corte ${escapeHtml(formatDate(card.nextClosingDate))}</span>
              <span class="chip">Proximo pago ${escapeHtml(formatDate(card.nextDueDate))}</span>
              <span class="chip">${escapeHtml(statement ? assignedCycle : 'Sin estado abierto')}</span>
              ${card.openingDebtPending > 0 ? `<span class="chip warning">Deuda inicial ${escapeHtml(formatCurrency(card.openingDebtPending))}</span>` : ''}
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

  function renderGoals() {
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

  function renderDebts() {
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
            </div>
            <div class="panel-actions">
              <button class="btn-primary" data-debt-action="pay" data-debt-id="${escapeHtml(debt.id)}">Registrar pago</button>
              <button class="btn-secondary" data-debt-action="edit" data-debt-id="${escapeHtml(debt.id)}">Editar</button>
            </div>
          </article>
        `;
      })
      .join('');
  }

  function renderRecurring() {
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

  function agendaKindLabel(kind) {
    const labels = { salary: 'Sueldo', 'card-close': 'Corte', 'card-due': 'Vencimiento', 'card-charge': 'Cargo tarjeta', income: 'Ingreso', expense: 'Gasto', statement: 'Estado de cuenta', transaction: 'Transacción' };
    return labels[kind] || kind;
  }

  function agendaKindIcon(kind) {
    const icons = { salary: '$', 'card-close': '||', 'card-due': '!', 'card-charge': 'TC', income: '^', expense: 'v', statement: 'EC' };
    return icons[kind] || '·';
  }

  function renderAgenda() {
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

  function renderQuickFilters() {
    $$('#quick-filter-row [data-quick-filter]').forEach((button) => {
      button.classList.toggle('active', button.dataset.quickFilter === state.filters.type);
      if (state.filters.type === 'all' && button.dataset.quickFilter === 'all') {
        button.classList.add('active');
      }
    });
  }

  function getFilteredTransactions() {
    const snapshot = getSnapshot();
    const cycleId = getSelectedCycleId();
    const search = state.filters.search.trim().toLowerCase();

    return snapshot.transactions
      .filter((item) => {
        const matchesCycle = cycleId ? item.budgetCycleId === cycleId : true;
        const matchesType = state.filters.type === 'all' || item.type === state.filters.type;
        const matchesAccount =
          state.filters.accountId === 'all' ||
          item.fromAccountId === state.filters.accountId ||
          item.toAccountId === state.filters.accountId;
        const matchesCard = state.filters.cardId === 'all' || item.cardId === state.filters.cardId;
        const matchesCategory = state.filters.categoryId === 'all' || item.categoryId === state.filters.categoryId;
        const haystack = `${item.description} ${item.notes} ${item.sourceType} ${getCardLabel(item.cardId)}`.toLowerCase();
        const matchesSearch = !search || haystack.includes(search);
        return matchesCycle && matchesType && matchesAccount && matchesCard && matchesCategory && matchesSearch;
      })
      .sort((a, b) => FinanceDB.compareDate(a.date, b.date));
  }

  function renderTransactions() {
    const list = $('#transactions-list');
    const empty = $('#transactions-empty');
    const filtered = getFilteredTransactions();
    $('#transactions-count-pill').textContent = `${filtered.length} resultado${filtered.length === 1 ? '' : 's'}`;

    if (!filtered.length) {
      list.innerHTML = '';
      empty.classList.remove('hidden');
      return;
    }

    empty.classList.add('hidden');
    list.innerHTML = filtered
      .map((item) => {
        const accountSummary = describeTransactionAccounts(item);
        const categoryLabel = getCategoryLabel(item.categoryId);
        const cycleLabel = item.budgetCycleId ? getCycleLabelById(item.budgetCycleId) : '';
        const installmentLabel = formatInstallmentLabel(item);
        const amountClass =
          item.type === 'income'
            ? 'income'
            : item.type === 'transfer'
              ? 'transfer'
              : item.type;
        const typeBadge = (TYPE_LABELS[item.type] || item.type).slice(0, 3).toUpperCase();
        return `
          <article class="transaction-item" data-transaction-id="${escapeHtml(item.id)}">
            <div class="transaction-top">
              <div>
                <div class="transaction-heading">
                  <span class="type-badge type-${escapeHtml(item.type)}">${escapeHtml(typeBadge)}</span>
                  <h4 class="transaction-title">${escapeHtml(item.description)}</h4>
                </div>
                <div class="transaction-meta">${escapeHtml(TYPE_LABELS[item.type] || item.type)} / ${escapeHtml(formatDate(item.purchaseDate || item.date))}</div>
              </div>
              <div class="transaction-amount ${escapeHtml(amountClass)}">${formatCurrency(item.amount)}</div>
            </div>
            <div class="chips-row">
              ${categoryLabel ? `<span class="chip">${escapeHtml(categoryLabel)}</span>` : ''}
              ${accountSummary ? `<span class="chip">${escapeHtml(accountSummary)}</span>` : ''}
              ${cycleLabel ? `<span class="chip">${escapeHtml(cycleLabel)}</span>` : ''}
              ${item.statementCycleKey && item.type === 'card_charge' ? `<span class="chip">Cierra ${escapeHtml(formatDate(item.statementCycleKey))}</span>` : ''}
              ${installmentLabel ? `<span class="chip">${escapeHtml(installmentLabel)}</span>` : ''}
              <span class="chip">${escapeHtml(formatSourceLabel(item.sourceType))}</span>
            </div>
            ${item.notes ? `<div class="transaction-foot"><span>${escapeHtml(item.notes)}</span></div>` : ''}
          </article>
        `;
      })
      .join('');
  }

  function renderStorageStatus() {
    const status = $('#storage-status');
    if (state.storage.persisted === null && !state.storage.estimate) {
      status.textContent = 'Tu navegador no expone detalles de persistencia. Igual el backup JSON sigue siendo recomendado.';
      return;
    }

    const used = state.storage.estimate?.usage
      ? `${(state.storage.estimate.usage / 1024 / 1024).toFixed(2)} MB usados`
      : 'uso no disponible';
    const quota = state.storage.estimate?.quota
      ? `${(state.storage.estimate.quota / 1024 / 1024).toFixed(0)} MB de cuota`
      : 'cuota no disponible';
    const persistedText =
      state.storage.persisted === true
        ? 'Almacenamiento persistente activo.'
        : 'Persistencia no garantizada por el navegador.';
    status.textContent = `${persistedText} ${used}, ${quota}.`;
  }

  function renderBackupStatus() {
    const status = $('#backup-status');
    const lastBackupAt = getSnapshot().settings.lastBackupAt;
    status.textContent = lastBackupAt
      ? `Ultimo backup completo: ${formatLongDate(lastBackupAt.slice(0, 10))}.`
      : 'Aun no has exportado un backup completo.';
  }

  function setDefaultFormDates() {
    const today = new Date().toISOString().slice(0, 10);
    if ($('#tx-date')) $('#tx-date').value = today;
  }

  function loadSettingsIntoInputs() {
    const settings = getSnapshot().settings;
    $('#settings-budget-input').value = settings.monthlyBudget ? FinanceDB.roundAmount(settings.monthlyBudget) : '';
    $('#settings-api-key').value = localStorage.getItem('openai_api_key') || '';
  }

  function populateSelects() {
    populateFilterOptions();
    populateTransactionFormOptions($('#tx-card-id')?.value || '');
    populateInstallmentCountOptions($('#tx-installment-count')?.value || '1');
    populateGoalAccountOptions();
    populateDebtAccountOptions();
    populateRecurringAccountOptions();
    populateCardPaymentOptions();
    populateSettingsOptions();
  }

  function populateFilterOptions() {
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

  function populateTransactionFormOptions(selectedCardId = '') {
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

  function populateGoalAccountOptions() {
    fillSelect(
      $('#goal-account-id'),
      buildAccountOptions({ kinds: ['savings', 'bank', 'cash'], emptyLabel: 'Sin cuenta asociada' }),
      $('#goal-account-id').value
    );
  }

  function populateDebtAccountOptions() {
    fillSelect(
      $('#debt-account-id'),
      buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Sin cuenta asociada' }),
      $('#debt-account-id').value
    );
  }

  function populateRecurringAccountOptions() {
    fillSelect(
      $('#recurring-account-id'),
      buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Selecciona una cuenta' }),
      $('#recurring-account-id').value
    );
  }

  function populateRecurringDayOptions(selectedDay = '1') {
    const options = Array.from({ length: 28 }, (_, index) => {
      const day = String(index + 1);
      return { value: day, label: `Dia ${day}` };
    });
    fillSelect($('#recurring-day'), options, String(selectedDay || '1'));
  }

  function populateCardPaymentOptions() {
    fillSelect(
      $('#card-payment-account-id'),
      buildAccountOptions({ kinds: ['bank', 'cash'], emptyLabel: 'Selecciona una cuenta' }),
      $('#card-payment-account-id').value
    );
  }

  function populateSettingsOptions() {
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
    renderLiquidAccountsCheckboxes();
  }

  function renderLiquidAccountsCheckboxes() {
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

  function updateTransactionCategoryOptions() {
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

  function updateCardStatementOptions() {
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

  function populateInstallmentCountOptions(selectedValue = '1') {
    const options = Array.from({ length: 24 }, (_, index) => {
      const value = String(index + 1);
      return {
        value,
        label: index === 0 ? '1 cuota' : `${value} cuotas`
      };
    });
    fillSelect($('#tx-installment-count'), options, String(selectedValue || '1'));
  }

  function updateInstallmentPreview() {
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

  function updateTransactionFields() {
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

  function updateRecurringFields() {
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

  let previouslyFocusedElement = null;

  function openModal(id) {
    const modal = document.getElementById(id);
    if (!modal) return;
    previouslyFocusedElement = document.activeElement;
    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
    const firstFocusable = modal.querySelector('button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
    if (firstFocusable) {
      requestAnimationFrame(() => firstFocusable.focus());
    }
  }

  function closeModal(id) {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.classList.remove('active');
    document.body.style.overflow = '';
    if (previouslyFocusedElement && typeof previouslyFocusedElement.focus === 'function') {
      previouslyFocusedElement.focus();
      previouslyFocusedElement = null;
    }
  }

  function openConfirm({ title, text, onAccept }) {
    $('#confirm-title').textContent = title;
    $('#confirm-text').textContent = text;
    state.ui.pendingConfirm = onAccept;
    openModal('modal-confirm');
  }

  function showToast(message, kind = 'success') {
    const container = $('#toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${kind}`;
    toast.textContent = message;
    container.appendChild(toast);
    requestAnimationFrame(() => toast.classList.add('visible'));
    setTimeout(() => {
      toast.classList.remove('visible');
      setTimeout(() => toast.remove(), 220);
    }, 2600);
  }

  function openTransactionModal({ transaction = null, preset = null } = {}) {
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
    $('#tx-date').value = base.date || new Date().toISOString().slice(0, 10);
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

  function openAIModal() {
    closeModal('modal-actions');
    $('#ai-form').reset();
    openModal('modal-ai');
  }

  function openAccountModal(account = null) {
    closeModal('modal-actions');
    const form = $('#account-form');
    form.reset();
    form.dataset.editId = account?.id || '';
    $('#account-modal-title').textContent = account ? 'Editar cuenta' : 'Nueva cuenta';
    $('#account-name').value = account?.name || '';
    $('#account-kind').value = account?.kind || 'bank';
    $('#account-opening-balance').value = account ? FinanceDB.roundAmount(account.openingBalance) : '';
    $('#account-include-networth').checked = account ? account.includeInNetWorth !== false : true;
    $('#account-archived').checked = account?.archived || false;
    openModal('modal-account');
  }

  function openCardModal(card = null) {
    closeModal('modal-actions');
    const form = $('#card-form');
    form.reset();
    form.dataset.editId = card?.id || '';
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

  function openGoalModal(goal = null) {
    closeModal('modal-actions');
    const form = $('#goal-form');
    form.reset();
    form.dataset.editId = goal?.id || '';
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

  function openDebtModal(debt = null) {
    closeModal('modal-actions');
    const form = $('#debt-form');
    form.reset();
    form.dataset.editId = debt?.id || '';
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

  function openRecurringModal(recurring = null) {
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

  function openCardStatementsModal(cardId) {
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

  function renderTransactionDetail(transaction) {
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

  function syncThemeMeta(theme) {
    const themeColor = theme === 'light' ? '#F5F3F7' : '#000000';
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) {
      meta.setAttribute('content', themeColor);
    }
  }

  function applyTheme(theme) {
    const normalizedTheme = theme === 'light' ? 'light' : 'dark';
    document.documentElement.dataset.theme = normalizedTheme;
    document.documentElement.style.colorScheme = normalizedTheme;
    localStorage.setItem('theme', normalizedTheme);
    syncThemeMeta(normalizedTheme);
  }

  async function saveTheme(theme) {
    await FinanceDB.saveSettings({ theme });
    applyTheme(theme);
    showToast(`Tema ${theme === 'light' ? 'claro' : 'oscuro'} activo`, 'success');
    await refreshData();
  }

  function resolveAccountIdFromHint(hint) {
    const needle = String(hint || '').trim().toLowerCase();
    if (!needle) return '';
    const account = getSnapshot().accounts.find((item) => item.name.toLowerCase().includes(needle));
    return account?.id || '';
  }

  function draftToPreset(draft) {
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

  async function withFormLock(form, fn) {
    const submitButton = form.querySelector('[type="submit"], .btn-primary');
    if (submitButton?.disabled) return;
    const originalText = submitButton?.textContent || '';
    try {
      if (submitButton) {
        submitButton.disabled = true;
        submitButton.textContent = 'Guardando...';
      }
      await fn();
    } finally {
      if (submitButton) {
        submitButton.disabled = false;
        submitButton.textContent = originalText;
      }
    }
  }

  async function handleTransactionSubmit(event) {
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

  async function handleAISubmit(event) {
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

  async function handleAccountSubmit(event) {
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

  async function handleCardSubmit(event) {
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

  async function handleGoalSubmit(event) {
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

  async function handleDebtSubmit(event) {
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

  async function handleRecurringSubmit(event) {
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

  async function handleSaveCycleConfig(event) {
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
        savingsSweepEnabled: $('#settings-savings-sweep-enabled').checked
      });
      await refreshData();
      showToast('Configuracion del ciclo guardada', 'success');
    } catch (error) {
      showToast(error.message, 'error');
    }
  }

  async function handleExportJson() {
    try {
      const payload = await FinanceDB.exportBackup();
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `finanzas-locales-backup-${new Date().toISOString().slice(0, 10)}.json`;
      link.click();
      URL.revokeObjectURL(url);
      await FinanceDB.saveSettings({ lastBackupAt: new Date().toISOString() });
      await refreshData();
      showToast('Backup JSON exportado', 'success');
    } catch (error) {
      showToast(error.message, 'error');
    }
  }

  async function handleImportJson(event) {
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
      await FinanceDB.importBackup(parsed);
      event.target.value = '';
      closeModal('modal-settings');
      await refreshData({ preserveSelectedCycle: false });
      showToast('Backup importado', 'success');
    } catch (error) {
      event.target.value = '';
      showToast(error.message, 'error');
    }
  }

  function buildCsvContent() {
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

  function handleExportCsv() {
    try {
      const csv = buildCsvContent();
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = `finanzas-locales-${new Date().toISOString().slice(0, 10)}.csv`;
      link.click();
      URL.revokeObjectURL(url);
      showToast('CSV exportado', 'success');
    } catch (error) {
      showToast(error.message, 'error');
    }
  }

  function changeCycle(direction) {
    const cycles = getSnapshot().cycles;
    const index = cycles.findIndex((cycle) => cycle.id === state.selectedCycleId);
    if (index === -1) return;
    const nextIndex = index + direction;
    if (nextIndex < 0 || nextIndex >= cycles.length) return;
    state.selectedCycleId = cycles[nextIndex].id;
    renderAll();
  }

  function handleAccountsGridClick(event) {
    const button = event.target.closest('[data-account-action]');
    if (!button) return;
    const account = getSnapshot().accounts.find((item) => item.id === button.dataset.accountId);
    if (!account) return;
    openAccountModal(account);
  }

  function handleCardsGridClick(event) {
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
          date: new Date().toISOString().slice(0, 10),
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
          date: new Date().toISOString().slice(0, 10),
          sourceType: 'manual'
        }
      });
    }
  }

  function handleGoalsGridClick(event) {
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

  function handleDebtsGridClick(event) {
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
          linkedEntityId: debt.id,
          linkedEntityType: 'debt',
          fromAccountId: debt.accountId || getSnapshot().settings.financialCycleConfig.sweepSourceAccountId || '',
          categoryId: 'debt-payment'
        }
      });
    }
  }

  function handleRecurringGridClick(event) {
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

  function handleTransactionsListClick(event) {
    const card = event.target.closest('[data-transaction-id]');
    if (!card) return;
    const transaction = getTransactionById(card.dataset.transactionId);
    if (!transaction) return;
    state.ui.detailTransactionId = transaction.id;
    renderTransactionDetail(transaction);
    openModal('modal-detail');
  }

  function bindSwipeToDismiss() {
    $$('.modal-handle').forEach((handle) => {
      let startY = 0;
      let currentY = 0;
      let isDragging = false;
      const overlay = handle.closest('.modal-overlay');
      const sheet = handle.closest('.modal-sheet');
      if (!overlay || !sheet) return;

      handle.addEventListener('touchstart', (e) => {
        startY = e.touches[0].clientY;
        currentY = startY;
        isDragging = true;
        sheet.style.transition = 'none';
      }, { passive: true });

      handle.addEventListener('touchmove', (e) => {
        if (!isDragging) return;
        currentY = e.touches[0].clientY;
        const dy = Math.max(0, currentY - startY);
        sheet.style.transform = `translateY(${dy}px)`;
      }, { passive: true });

      handle.addEventListener('touchend', () => {
        if (!isDragging) return;
        isDragging = false;
        sheet.style.transition = '';
        const dy = currentY - startY;
        if (dy > 80) {
          closeModal(overlay.id);
        }
        sheet.style.transform = '';
      });
    });
  }

  function bindEvents() {
    bindSwipeToDismiss();
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

  function registerServiceWorker() {
    if (!('serviceWorker' in navigator)) return;
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('sw.js').catch(() => {});
    });
  }

  return { init };
})();

window.addEventListener('DOMContentLoaded', () => {
  App.init().catch((error) => {
    console.error(error);
  });
});
