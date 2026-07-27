/* Conciliacion: compara el PDF del banco contra lo registrado en la app. */
import { $, closeModal, escapeHtml, fillSelect, openModal, showToast } from './dom.js';
import { formatCurrency, formatDate, formatStatementLabel, getCardLabel } from './format.js';
import { refreshData } from './render.js';
import { getCardStatements, getSnapshot } from './state.js';

const PASSWORD_KEY = 'statement_password';

let lastResult = null;

export function getStoredStatementPassword() {
  try {
    return localStorage.getItem(PASSWORD_KEY) || '';
  } catch (error) {
    return '';
  }
}

export function setStoredStatementPassword(value) {
  try {
    if (value) localStorage.setItem(PASSWORD_KEY, value);
    else localStorage.removeItem(PASSWORD_KEY);
  } catch (error) {
    // modo privado: la clave se pedira cada vez
  }
}

function populateStatementOptions() {
  const cardId = $('#reconcile-card').value;
  const statements = cardId ? getCardStatements(cardId).filter((item) => item.kind === 'statement') : [];
  const options = statements.map((statement) => ({
    value: statement.statementCycleKey,
    label: `${formatStatementLabel(statement)} · ${formatCurrency(statement.chargedAmount)}`
  }));

  fillSelect(
    $('#reconcile-statement'),
    options.length ? options : [{ value: '', label: 'Sin estados registrados' }],
    $('#reconcile-statement').value
  );
}

export function openReconcileModal() {
  closeModal('modal-actions');
  lastResult = null;
  $('#reconcile-form').reset();
  $('#reconcile-result').innerHTML = '';

  const cards = getSnapshot().cards.filter((card) => !card.archived);
  if (!cards.length) {
    showToast('Primero registra una tarjeta.', 'error');
    return;
  }

  fillSelect(
    $('#reconcile-card'),
    cards.map((card) => ({ value: card.id, label: getCardLabel(card.id) })),
    cards[0].id
  );
  populateStatementOptions();

  // Si no hay clave guardada, se pide en el momento.
  $('#field-reconcile-password').classList.toggle('hidden', !!getStoredStatementPassword());
  openModal('modal-reconcile');
}

export function bindReconcileEvents() {
  $('#reconcile-card').addEventListener('change', populateStatementOptions);
  $('#reconcile-form').addEventListener('submit', handleReconcileSubmit);
  $('#reconcile-result').addEventListener('click', handleResultClick);
}

async function handleReconcileSubmit(event) {
  event.preventDefault();
  const button = $('#btn-reconcile-analyze');
  const file = $('#reconcile-file').files?.[0];
  const cardId = $('#reconcile-card').value;
  const statementCycleKey = $('#reconcile-statement').value;
  const apiKey = localStorage.getItem('openai_api_key') || '';
  const password = $('#reconcile-password').value.trim() || getStoredStatementPassword();

  if (!file) {
    showToast('Adjunta el PDF del estado de cuenta.', 'error');
    return;
  }

  try {
    button.disabled = true;
    button.textContent = 'Leyendo PDF...';
    const text = await FinanceAI.extractPdfText(file, { password, maxPages: 20 });

    button.textContent = 'Ordenando con IA...';
    const statement = await FinanceAI.analyzeStatement(text, apiKey);

    button.textContent = 'Comparando...';
    const result = await FinanceDB.reconcileStatement({
      cardId,
      statementCycleKey,
      movements: statement.movements
    });

    lastResult = { ...result, statement, cardId };
    renderReconcileResult(lastResult);
    await refreshData();
    showToast('Estado de cuenta comparado', 'success');
  } catch (error) {
    if (/clave/i.test(error.message)) {
      $('#field-reconcile-password').classList.remove('hidden');
    }
    showToast(error.message, 'error');
  } finally {
    button.disabled = false;
    button.textContent = 'Leer y cuadrar';
  }
}

function renderReconcileResult(result) {
  const { statement, missing, extra, matched, difference, statementTotal, appTotal } = result;
  const cuadra = Math.abs(difference) < 0.02;

  $('#reconcile-result').innerHTML = `
    <div class="detail-card ${cuadra ? 'reconcile-ok' : 'reconcile-diff'}">
      <strong>${cuadra ? 'Todo cuadra' : `Diferencia de ${escapeHtml(formatCurrency(Math.abs(difference)))}`}</strong>
      <p>Estado del banco: ${escapeHtml(formatCurrency(statementTotal))} · En la app: ${escapeHtml(formatCurrency(appTotal))}</p>
      <p>${matched.length} movimiento${matched.length === 1 ? '' : 's'} coinciden${statement.unreadable ? ` · ${statement.unreadable} no se pudieron leer del PDF` : ''}</p>
    </div>

    ${missing.length ? `
      <div class="detail-card">
        <strong>Falta registrar (${missing.length})</strong>
        <p>Estan en el estado de cuenta pero no en la app.</p>
        <div class="detail-list">
          ${missing.map((item) => `
            <div class="detail-list-row">
              <strong>${escapeHtml(item.description)}</strong>
              <span>${escapeHtml(formatDate(item.date))} · ${escapeHtml(formatCurrency(item.amount))}</span>
            </div>
          `).join('')}
        </div>
        <div class="panel-actions">
          <button class="btn-primary" data-reconcile-action="import">Agregarlos todos</button>
        </div>
      </div>
    ` : ''}

    ${extra.length ? `
      <div class="detail-card">
        <strong>Registrado de mas (${extra.length})</strong>
        <p>Estan en la app pero el banco no los reporta. Revisa si los duplicaste.</p>
        <div class="detail-list">
          ${extra.map((item) => `
            <div class="detail-list-row">
              <strong>${escapeHtml(item.description)}</strong>
              <span>${escapeHtml(formatDate(item.purchaseDate || item.date))} · ${escapeHtml(formatCurrency(item.amount))}</span>
            </div>
          `).join('')}
        </div>
      </div>
    ` : ''}
  `;
}

async function handleResultClick(event) {
  const button = event.target.closest('[data-reconcile-action]');
  if (!button || !lastResult) return;

  if (button.dataset.reconcileAction === 'import') {
    try {
      button.disabled = true;
      button.textContent = 'Agregando...';
      const count = await FinanceDB.importMissingStatementMovements(lastResult.cardId, lastResult.missing);
      await refreshData();
      showToast(`${count} movimiento${count === 1 ? '' : 's'} agregado${count === 1 ? '' : 's'}`, 'success');
      closeModal('modal-reconcile');
    } catch (error) {
      showToast(error.message, 'error');
      button.disabled = false;
      button.textContent = 'Agregarlos todos';
    }
  }
}
