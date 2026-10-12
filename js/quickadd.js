/* Captura rapida: anotar un gasto o ingreso en tres toques (monto, categoria, guardar). */
import { $, $$, closeModal, escapeHtml, haptic, openModal, showToast } from './dom.js';
import { formatCurrency, getCardDisplayLabel } from './format.js';
import { EMOJI_CHOICES, getCategoryEmoji } from './icons.js';
import { openTransactionModal } from './modals.js';
import { refreshData } from './render.js';
import { getSnapshot } from './state.js';

const LAST_METHOD_KEY = 'quick-last-method';
const INSTALLMENT_CHOICES = [1, 2, 3, 4, 6, 9, 12, 18, 24, 36];
const ACCOUNT_EMOJI = { cash: '💵', bank: '🏦', savings: '🐷' };

const quick = {
  type: 'expense',
  categoryId: '',
  categoryTouched: false,
  method: '',
  date: '',
  installments: 1
};

function readStorage(key) {
  try {
    return localStorage.getItem(key) || '';
  } catch (error) {
    return '';
  }
}

function writeStorage(key, value) {
  try {
    localStorage.setItem(key, value);
  } catch (error) {
    // modo privado: solo se pierde el recuerdo del ultimo medio de pago
  }
}

/** Acepta "12,50", "1.234,50", "1,234.50" o "S/ 12": la gente escribe montos de muchas formas. */
export function parseAmount(raw) {
  const text = String(raw || '').replace(/[^\d.,]/g, '');
  if (!text) return 0;
  const decimals = text.match(/[.,](\d{1,2})$/);
  if (decimals) {
    const integer = text.slice(0, -decimals[0].length).replace(/[.,]/g, '') || '0';
    return FinanceDB.roundAmount(`${integer}.${decimals[1]}`);
  }
  return FinanceDB.roundAmount(text.replace(/[.,]/g, ''));
}

// Orden fijo, de lo mas usado en el dia a dia a lo menos: la posicion de cada
// boton no cambia y la mano aprende donde esta. Las propias van antes de "Otros".
const CATEGORY_ORDER = [
  'food', 'transport', 'home', 'services', 'shopping', 'health', 'entertainment', 'education',
  'salary', 'freelance'
];
const LAST_CATEGORIES = new Set(['other-expense', 'other-income']);

function categoryRank(category) {
  if (LAST_CATEGORIES.has(category.id)) return 1000;
  const index = CATEGORY_ORDER.indexOf(category.id);
  return index === -1 ? 500 : index;
}

export function sortCategories(categories) {
  return categories.slice().sort((a, b) => categoryRank(a) - categoryRank(b) || a.name.localeCompare(b.name, 'es'));
}

function getQuickCategories(type) {
  const desired = type === 'income' ? 'income' : 'expense';
  return sortCategories(
    getSnapshot().categories.filter((item) => item.type === desired && !item.archived && !item.system)
  );
}

function getMethods(type) {
  const snapshot = getSnapshot();
  const accounts = snapshot.accounts
    .filter((account) => !account.archived && ['cash', 'bank', 'savings'].includes(account.kind))
    .map((account) => ({
      value: `acc:${account.id}`,
      emoji: ACCOUNT_EMOJI[account.kind] || '🏦',
      label: `${account.name}${account.currency && account.currency !== 'PEN' ? ` (${account.currency})` : ''}`
    }));
  if (type === 'income') return accounts;
  const cards = snapshot.cards
    .filter((card) => !card.archived)
    .map((card) => ({ value: `card:${card.id}`, emoji: '💳', label: getCardDisplayLabel(card) }));
  return accounts.concat(cards);
}

function defaultMethod(type) {
  const methods = getMethods(type);
  const remembered = readStorage(`${LAST_METHOD_KEY}-${type}`);
  if (methods.some((item) => item.value === remembered)) return remembered;
  const preferred = type === 'income' ? 'acc:bank-main' : 'acc:cash-main';
  if (methods.some((item) => item.value === preferred)) return preferred;
  return methods[0]?.value || '';
}

function renderType() {
  $$('#quick-type [data-qtype]').forEach((button) => {
    const active = button.dataset.qtype === quick.type;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  const sheet = $('#modal-quick .quick-sheet');
  sheet.classList.toggle('is-income', quick.type === 'income');
  $('#quick-category-label').textContent = quick.type === 'income' ? '¿De dónde vino?' : '¿En qué gastaste?';
  $('#quick-method-label').textContent = quick.type === 'income' ? '¿A dónde entró?' : '¿Con qué pagaste?';
  $('#quick-save').textContent = quick.type === 'income' ? 'Guardar ingreso' : 'Guardar gasto';
  $('#quick-note').placeholder = quick.type === 'income' ? 'Ej: venta, pago de un cliente' : 'Ej: menú del día, taxi al trabajo';
}

function renderCategories() {
  const categories = getQuickCategories(quick.type);
  $('#quick-categories').innerHTML = categories
    .map(
      (category) => `
        <button type="button" class="cat-tile ${category.id === quick.categoryId ? 'active' : ''}" data-category-id="${escapeHtml(category.id)}" aria-pressed="${category.id === quick.categoryId}">
          <span class="cat-tile-emoji" aria-hidden="true">${escapeHtml(getCategoryEmoji(category, quick.type))}</span>
          <span class="cat-tile-name">${escapeHtml(category.name)}</span>
        </button>
      `
    )
    .concat(`
      <button type="button" class="cat-tile cat-tile-new" data-category-new="1">
        <span class="cat-tile-emoji" aria-hidden="true">＋</span>
        <span class="cat-tile-name">Nueva</span>
      </button>
    `)
    .join('');
}

function renderMethods() {
  const methods = getMethods(quick.type);
  if (!methods.some((item) => item.value === quick.method)) quick.method = defaultMethod(quick.type);
  $('#quick-methods').innerHTML = methods.length
    ? methods
      .map(
        (method) => `
          <button type="button" class="choice-chip ${method.value === quick.method ? 'active' : ''}" data-method="${escapeHtml(method.value)}" aria-pressed="${method.value === quick.method}">
            <span aria-hidden="true">${escapeHtml(method.emoji)}</span> ${escapeHtml(method.label)}
          </button>
        `
      )
      .join('')
    : '<p class="soft-note">Primero crea una cuenta en la sección Cuentas.</p>';

  const isCard = quick.method.startsWith('card:');
  $('#quick-installments-field').classList.toggle('hidden', !isCard || quick.type === 'income');
}

function renderDates() {
  const today = FinanceDB.getToday();
  const yesterday = FinanceDB.addDays(today, -1);
  const custom = quick.date !== today && quick.date !== yesterday;
  $$('#quick-dates [data-date-choice]').forEach((button) => {
    const choice = button.dataset.dateChoice;
    const active = (choice === 'today' && quick.date === today) || (choice === 'yesterday' && quick.date === yesterday) || (choice === 'other' && custom);
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
  const input = $('#quick-date');
  input.classList.toggle('hidden', !custom);
  input.value = quick.date;
}

function renderAll() {
  renderType();
  renderCategories();
  renderMethods();
  renderDates();
}

export function openQuickAdd({ type = 'expense', amount = '', note = '', categoryId = '' } = {}) {
  closeModal('modal-actions');
  quick.type = type === 'income' ? 'income' : 'expense';
  quick.categoryId = categoryId;
  quick.categoryTouched = !!categoryId;
  quick.method = defaultMethod(quick.type);
  quick.date = FinanceDB.getToday();
  quick.installments = 1;
  $('#quick-amount').value = Number(amount) > 0 ? FinanceDB.roundAmount(amount).toFixed(2) : '';
  $('#quick-note').value = note;
  $('#quick-installments').value = '1';
  renderAll();
  openModal('modal-quick');
  // El teclado numerico sale solo: lo primero que uno sabe es cuanto pago.
  requestAnimationFrame(() => $('#quick-amount').focus());
}

function suggestFromNote() {
  if (quick.categoryTouched) return;
  const note = $('#quick-note').value.trim();
  if (note.length < 3) return;
  const suggestion = FinanceDB.suggestCategory(note, getSnapshot().categoryClassifier, { type: quick.type });
  if (!suggestion) return;
  if (!getQuickCategories(quick.type).some((item) => item.id === suggestion.categoryId)) return;
  quick.categoryId = suggestion.categoryId;
  renderCategories();
}

function flagMissing(selector) {
  const element = $(selector);
  element.classList.remove('shake');
  // reflow para reiniciar la animacion si se repite el error
  void element.offsetWidth;
  element.classList.add('shake');
  haptic([30, 40, 30]);
}

async function saveQuick() {
  const amount = parseAmount($('#quick-amount').value);
  if (!(amount > 0)) {
    flagMissing('#quick-amount-wrap');
    showToast('Escribe cuánto fue', 'error');
    $('#quick-amount').focus();
    return;
  }
  const category = getQuickCategories(quick.type).find((item) => item.id === quick.categoryId);
  if (!category) {
    flagMissing('#quick-categories');
    showToast(quick.type === 'income' ? 'Elige de donde vino' : 'Elige en que gastaste', 'error');
    return;
  }
  if (!quick.method) {
    showToast('Primero crea una cuenta en la sección Cuentas', 'error');
    return;
  }

  const [kind, id] = quick.method.split(':');
  const note = $('#quick-note').value.trim();
  const payload = {
    amount,
    date: quick.date || FinanceDB.getToday(),
    description: note || category.name,
    categoryId: category.id,
    notes: '',
    sourceType: 'manual'
  };
  if (quick.type === 'income') {
    Object.assign(payload, { type: 'income', toAccountId: id });
  } else if (kind === 'card') {
    Object.assign(payload, {
      type: 'card_charge',
      cardId: id,
      purchaseDate: payload.date,
      installmentCount: quick.installments
    });
  } else {
    Object.assign(payload, { type: 'expense', fromAccountId: id });
  }

  const button = $('#quick-save');
  if (button.disabled) return;
  button.disabled = true;
  try {
    const saved = await FinanceDB.saveTransaction(payload);
    writeStorage(`${LAST_METHOD_KEY}-${quick.type}`, quick.method);
    haptic(15);
    closeModal('modal-quick');
    await refreshData();
    const verb = quick.type === 'income' ? 'Ingreso' : 'Gasto';
    showToast(`${verb} de ${formatCurrency(amount)} guardado`, 'success', {
      actionLabel: 'Deshacer',
      onAction: async () => {
        try {
          await FinanceDB.deleteTransaction(saved.id);
          await refreshData();
          showToast('Listo, no se guardó', 'info');
        } catch (error) {
          showToast(error.message, 'error');
        }
      }
    });
  } catch (error) {
    showToast(error.message, 'error');
  } finally {
    button.disabled = false;
  }
}

/* ===== Categorias propias ===== */

let categoryReturnToQuick = false;

export function openCategoryModal(category = null, { fromQuick = false } = {}) {
  categoryReturnToQuick = fromQuick;
  const form = $('#category-form');
  form.reset();
  form.dataset.editId = category?.id || '';
  $('#category-modal-title').textContent = category ? 'Editar categoría' : 'Nueva categoría';
  $('#category-name').value = category?.name || '';
  $('#category-type').value = category?.type || (fromQuick ? quick.type : 'expense');
  const selected = category?.icon || EMOJI_CHOICES[0];
  $('#category-icon').value = selected;
  $('#category-emoji-grid').innerHTML = EMOJI_CHOICES
    .map(
      (emoji) => `<button type="button" class="emoji-choice ${emoji === selected ? 'active' : ''}" data-emoji="${escapeHtml(emoji)}" aria-label="Icono ${escapeHtml(emoji)}">${escapeHtml(emoji)}</button>`
    )
    .join('');
  $('#btn-delete-category').classList.toggle('hidden', !category || category.system);
  openModal('modal-category');
  requestAnimationFrame(() => $('#category-name').focus());
}

async function handleCategorySubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  try {
    const saved = await FinanceDB.saveCategory({
      id: form.dataset.editId || '',
      name: $('#category-name').value,
      type: $('#category-type').value,
      icon: $('#category-icon').value
    });
    closeModal('modal-category');
    await refreshData();
    renderCategoryManager();
    if (categoryReturnToQuick) {
      quick.categoryId = saved.type === quick.type ? saved.id : quick.categoryId;
      quick.categoryTouched = true;
      renderCategories();
    }
    showToast('Categoría guardada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
}

async function handleCategoryDelete() {
  const id = $('#category-form').dataset.editId;
  if (!id) return;
  try {
    const result = await FinanceDB.deleteCategory(id);
    closeModal('modal-category');
    await refreshData();
    renderCategoryManager();
    showToast(result.archived ? 'Categoría oculta: tus movimientos anteriores la conservan' : 'Categoría eliminada', 'success');
  } catch (error) {
    showToast(error.message, 'error');
  }
}

export function renderCategoryManager() {
  const container = $('#settings-category-list');
  if (!container) return;
  const visible = getSnapshot().categories.filter((item) => !item.archived && !item.system);
  const categories = sortCategories(visible.filter((item) => item.type === 'expense'))
    .concat(sortCategories(visible.filter((item) => item.type === 'income')));
  container.innerHTML = categories
    .map(
      (category) => `
        <button type="button" class="category-pill" data-edit-category="${escapeHtml(category.id)}">
          <span aria-hidden="true">${escapeHtml(category.icon || '📦')}</span>
          ${escapeHtml(category.name)}
          <small>${category.type === 'income' ? 'ingreso' : 'gasto'}</small>
        </button>
      `
    )
    .join('');
}

export function bindQuickAddEvents() {
  $('#quick-type').addEventListener('click', (event) => {
    const button = event.target.closest('[data-qtype]');
    if (!button || button.dataset.qtype === quick.type) return;
    quick.type = button.dataset.qtype;
    quick.categoryId = '';
    quick.categoryTouched = false;
    quick.method = defaultMethod(quick.type);
    renderAll();
  });

  $('#quick-categories').addEventListener('click', (event) => {
    if (event.target.closest('[data-category-new]')) {
      openCategoryModal(null, { fromQuick: true });
      return;
    }
    const tile = event.target.closest('[data-category-id]');
    if (!tile) return;
    quick.categoryId = tile.dataset.categoryId;
    quick.categoryTouched = true;
    haptic(8);
    renderCategories();
  });

  $('#quick-methods').addEventListener('click', (event) => {
    const chip = event.target.closest('[data-method]');
    if (!chip) return;
    quick.method = chip.dataset.method;
    renderMethods();
  });

  $('#quick-dates').addEventListener('click', (event) => {
    const chip = event.target.closest('[data-date-choice]');
    if (!chip) return;
    const today = FinanceDB.getToday();
    if (chip.dataset.dateChoice === 'today') quick.date = today;
    if (chip.dataset.dateChoice === 'yesterday') quick.date = FinanceDB.addDays(today, -1);
    if (chip.dataset.dateChoice === 'other') {
      quick.date = FinanceDB.addDays(today, -2);
      renderDates();
      const input = $('#quick-date');
      input.focus();
      input.showPicker?.();
      return;
    }
    renderDates();
  });

  $('#quick-date').addEventListener('change', (event) => {
    if (event.target.value) quick.date = event.target.value;
    renderDates();
  });

  $('#quick-installments').innerHTML = INSTALLMENT_CHOICES
    .map((count) => `<option value="${count}">${count === 1 ? 'Sin cuotas (1 pago)' : `${count} cuotas`}</option>`)
    .join('');
  $('#quick-installments').addEventListener('change', (event) => {
    quick.installments = parseInt(event.target.value, 10) || 1;
  });

  let noteTimer = null;
  $('#quick-note').addEventListener('input', () => {
    clearTimeout(noteTimer);
    noteTimer = setTimeout(suggestFromNote, 350);
  });

  $('#quick-amount').addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      $('#quick-amount').blur();
    }
  });

  $('#quick-form').addEventListener('submit', (event) => {
    event.preventDefault();
    saveQuick();
  });

  $('#quick-more').addEventListener('click', () => {
    const amount = parseAmount($('#quick-amount').value);
    const note = $('#quick-note').value.trim();
    const [kind, id] = quick.method.split(':');
    closeModal('modal-quick');
    const isCard = kind === 'card' && quick.type === 'expense';
    openTransactionModal({
      preset: {
        type: quick.type === 'income' ? 'income' : isCard ? 'card_charge' : 'expense',
        amount: amount || '',
        date: quick.date,
        categoryId: quick.categoryId,
        description: note,
        fromAccountId: quick.type === 'expense' && kind === 'acc' ? id : '',
        toAccountId: quick.type === 'income' ? id : '',
        cardId: isCard ? id : '',
        installmentCount: quick.installments
      }
    });
  });

  $('#category-emoji-grid').addEventListener('click', (event) => {
    const choice = event.target.closest('[data-emoji]');
    if (!choice) return;
    $('#category-icon').value = choice.dataset.emoji;
    $$('#category-emoji-grid .emoji-choice').forEach((button) => button.classList.toggle('active', button === choice));
  });
  $('#category-form').addEventListener('submit', handleCategorySubmit);
  $('#btn-delete-category').addEventListener('click', handleCategoryDelete);

  $('#settings-category-list')?.addEventListener('click', (event) => {
    const pill = event.target.closest('[data-edit-category]');
    if (!pill) return;
    const category = getSnapshot().categories.find((item) => item.id === pill.dataset.editCategory);
    if (category) openCategoryModal(category);
  });
  $('#btn-add-category')?.addEventListener('click', () => openCategoryModal());
}
