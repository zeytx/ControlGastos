/* Preguntas sobre tus finanzas enviando solo agregados, nunca la lista de movimientos. */
import { $, closeModal, escapeHtml, openModal, showToast } from './dom.js';
import { getCategoryLabel } from './format.js';
import { getCategoryBreakdown, getDashboardMetrics } from './metrics.js';
import { getSelectedCycle, getSnapshot } from './state.js';

/** Lo unico que sale del dispositivo: totales, nunca transacciones. */
export function buildAggregates() {
  const snapshot = getSnapshot();
  const cycle = getSelectedCycle();
  const metrics = getDashboardMetrics();
  const budget = snapshot.budget || {};

  return {
    moneda: snapshot.baseCurrency || 'PEN',
    ciclo: cycle
      ? { desde: cycle.startDate, hasta: cycle.endDate, ingresos: cycle.income || 0 }
      : null,
    gastoEnEfectivo: metrics.cashSpend,
    comprasConTarjeta: metrics.cardCommitted,
    ahorroDelCiclo: metrics.savingsFlow,
    libreNeto: metrics.freeNet,
    gastoPorCategoria: getCategoryBreakdown().map((item) => ({
      categoria: item.label,
      monto: item.amount
    })),
    patrimonio: {
      liquido: snapshot.netWorth?.liquid || 0,
      deudaTarjetas: snapshot.netWorth?.cardDebt || 0,
      otrasDeudas: snapshot.netWorth?.otherDebt || 0,
      neto: snapshot.netWorth?.total || 0
    },
    tope: budget.enabled
      ? { limite: budget.limit, gastado: budget.spent, restante: budget.remaining }
      : null,
    topesPorCategoria: (budget.byCategory || []).map((item) => ({
      categoria: item.name,
      limite: item.limit,
      gastado: item.spent
    })),
    metas: (snapshot.goals || []).map((goal) => ({
      nombre: goal.name,
      objetivo: goal.targetAmount,
      actual: goal.currentAmount
    })),
    deudas: (snapshot.debts || [])
      .filter((debt) => !debt.archived)
      .map((debt) => ({ nombre: debt.name, pendiente: debt.outstandingAmount }))
  };
}

export function openAskModal() {
  closeModal('modal-actions');
  $('#ask-form').reset();
  $('#ask-answer').innerHTML = '';
  $('#ask-payload').textContent = JSON.stringify(buildAggregates(), null, 1);
  openModal('modal-ask');
}

export function bindAskEvents() {
  $('#ask-form').addEventListener('submit', handleAskSubmit);
}

async function handleAskSubmit(event) {
  event.preventDefault();
  const button = $('#btn-ask-send');
  const question = $('#ask-question').value.trim();
  const apiKey = localStorage.getItem('openai_api_key') || '';

  if (!question) {
    showToast('Escribe una pregunta.', 'error');
    return;
  }

  try {
    button.disabled = true;
    button.textContent = 'Pensando...';
    const answer = await FinanceAI.askAboutFinances(question, buildAggregates(), apiKey);
    $('#ask-answer').innerHTML = `
      <div class="detail-card">
        <strong>${escapeHtml(question)}</strong>
        <p>${escapeHtml(answer)}</p>
      </div>
    `;
  } catch (error) {
    showToast(error.message, 'error');
  } finally {
    button.disabled = false;
    button.textContent = 'Preguntar';
  }
}
