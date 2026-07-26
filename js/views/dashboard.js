/* Panel del ciclo. */
import { renderDonutSvg, renderTrendSvg } from '../charts.js';
import { $, escapeHtml } from '../dom.js';
import { formatCurrency, formatDate, formatShortDate, getCardLabel } from '../format.js';
import { getCardCoverageSummary, getCategoryBreakdown, getCycleTrendPoints, getDashboardMetrics, getUpcomingTimelinePreview } from '../metrics.js';
import { getRecurringMap, getSnapshot } from '../state.js';

export function renderDashboard() {
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
