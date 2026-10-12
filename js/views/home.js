/* Inicio en lenguaje simple: cuanto queda, cuanto se gasto hoy y lo ultimo anotado. */
import { $, escapeHtml } from '../dom.js';
import { formatCurrency, formatShortDate, getCategoryIcon, getCategoryLabel } from '../format.js';
import { getHomeSummary } from '../metrics.js';
import { renderTransactionRow } from './transactions.js';

const WEEKDAYS = ['domingo', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado'];
const MONTHS = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'];

function greeting() {
  const hour = new Date().getHours();
  if (hour < 12) return 'Buenos días';
  if (hour < 19) return 'Buenas tardes';
  return 'Buenas noches';
}

function todayLabel() {
  const now = new Date();
  return `${WEEKDAYS[now.getDay()]} ${now.getDate()} de ${MONTHS[now.getMonth()]}`;
}

function periodCaption(summary) {
  const { period, daysLeft } = summary;
  if (period.kind === 'cycle') {
    return daysLeft <= 1
      ? 'Mañana llega tu sueldo'
      : `Faltan ${daysLeft} días para tu sueldo (${formatShortDate(FinanceDB.addDays(period.endDate, 1))})`;
  }
  return daysLeft <= 1 ? 'Hoy termina el mes' : `Faltan ${daysLeft} días para fin de mes`;
}

function renderComparison(summary) {
  const { comparison, period } = summary;
  if (!comparison) return '';
  const unit = period.kind === 'cycle' ? 'el sueldo anterior' : 'el mes pasado';
  if (Math.abs(comparison.diff) < 1) {
    return `<p class="home-compare neutral">Vas gastando igual que ${unit} a estas alturas.</p>`;
  }
  const better = comparison.diff < 0;
  return `
    <p class="home-compare ${better ? 'positive' : 'warning'}">
      ${better ? '👍' : '👀'} Vas ${escapeHtml(formatCurrency(Math.abs(comparison.diff)))} ${better ? 'por debajo' : 'por encima'} de ${unit} a estas alturas.
    </p>
  `;
}

export function renderHome() {
  const container = $('#home-summary');
  if (!container) return;
  const summary = getHomeSummary();
  const { headline } = summary;

  container.innerHTML = `
    <div class="home-greeting">
      <p class="home-hello">${greeting()} 👋</p>
      <p class="home-date">${escapeHtml(todayLabel())}</p>
    </div>

    <div class="home-headline tone-${escapeHtml(headline.tone)}">
      <span class="home-headline-label">${escapeHtml(headline.label)}</span>
      <strong class="home-headline-amount">${escapeHtml(formatCurrency(headline.amount))}</strong>
      ${headline.ratio !== null ? `
        <div class="home-meter" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${Math.round(headline.ratio * 100)}">
          <div class="home-meter-fill" style="width: ${(headline.ratio * 100).toFixed(1)}%"></div>
        </div>` : ''}
      <span class="home-headline-detail">${escapeHtml(headline.detail)}</span>
    </div>

    <div class="home-stats">
      <div class="home-stat">
        <span>Hoy gastaste</span>
        <strong>${escapeHtml(formatCurrency(summary.spentToday))}</strong>
      </div>
      ${summary.perDay > 0 ? `
        <div class="home-stat">
          <span>Puedes gastar al día</span>
          <strong>${escapeHtml(formatCurrency(summary.perDay))}</strong>
        </div>` : `
        <div class="home-stat">
          <span>${summary.period.kind === 'cycle' ? 'Gastado este sueldo' : 'Gastado este mes'}</span>
          <strong>${escapeHtml(formatCurrency(summary.spentSoFar))}</strong>
        </div>`}
    </div>
    <p class="home-caption">${escapeHtml(periodCaption(summary))}</p>
    ${renderComparison(summary)}

    <div class="home-actions">
      <button type="button" class="home-action expense" data-quick="expense">
        <span class="home-action-sign" aria-hidden="true">−</span>
        <span>Anotar gasto</span>
      </button>
      <button type="button" class="home-action income" data-quick="income">
        <span class="home-action-sign" aria-hidden="true">+</span>
        <span>Anotar ingreso</span>
      </button>
    </div>
  `;

  const recent = $('#home-recent');
  recent.innerHTML = summary.recent.length
    ? summary.recent.map((item) => renderTransactionRow(item, { showDate: true })).join('')
    : `
      <div class="home-empty">
        <span class="home-empty-emoji" aria-hidden="true">📝</span>
        <p>Aún no anotaste nada. Toca <strong>Anotar gasto</strong> la próxima vez que pagues algo.</p>
      </div>
    `;

  const categories = $('#home-categories');
  const categoriesBlock = $('#home-categories-block');
  categoriesBlock.classList.toggle('hidden', !summary.topCategories.length);
  categories.innerHTML = summary.topCategories
    .map(
      (item) => `
        <div class="cat-bar">
          <span class="cat-bar-emoji" aria-hidden="true">${escapeHtml(getCategoryIcon(item.categoryId))}</span>
          <div class="cat-bar-body">
            <div class="cat-bar-copy">
              <span>${escapeHtml(getCategoryLabel(item.categoryId) || 'Otros gastos')}</span>
              <strong>${escapeHtml(formatCurrency(item.amount))}</strong>
            </div>
            <div class="cat-bar-track"><div class="cat-bar-fill" style="width: ${(item.ratio * 100).toFixed(1)}%"></div></div>
          </div>
          <span class="cat-bar-pct">${Math.round(item.ratio * 100)}%</span>
        </div>
      `
    )
    .join('');
}
