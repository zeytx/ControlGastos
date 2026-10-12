/* Utilidades de DOM: consultas, modales, toasts y confirmaciones. */
import { state } from './state.js';

export const $ = (selector) => document.querySelector(selector);

export const $$ = (selector) => Array.from(document.querySelectorAll(selector));

export function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function fillSelect(select, options, selectedValue) {
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

export function renderEmptyPanel(title, text) {
  return `
    <div class="empty-state">
      <div class="empty-icon">[]</div>
      <h3>${escapeHtml(title)}</h3>
      <p>${escapeHtml(text)}</p>
    </div>
  `;
}

export let previouslyFocusedElement = null;

// Modales abiertos, el ultimo arriba. Con una sola entrada de historial para
// todos, el boton "atras" de Android cierra el modal en vez de salir de la app.
const openStack = [];
let ignoredPops = 0;

export function openModal(id) {
  const modal = document.getElementById(id);
  if (!modal) return;
  if (!modal.classList.contains('active')) {
    if (!openStack.length) history.pushState({ modalGuard: true }, '');
    openStack.push(id);
    modal.style.zIndex = String(120 + openStack.length);
  }
  previouslyFocusedElement = document.activeElement;
  modal.classList.add('active');
  document.body.style.overflow = 'hidden';
  const firstFocusable = modal.querySelector('button, input, select, textarea, [tabindex]:not([tabindex="-1"])');
  if (firstFocusable) {
    requestAnimationFrame(() => firstFocusable.focus({ preventScroll: true }));
  }
}

export function closeModal(id, { fromHistory = false } = {}) {
  const modal = document.getElementById(id);
  if (!modal || !modal.classList.contains('active')) return;
  modal.classList.remove('active');
  const index = openStack.lastIndexOf(id);
  if (index >= 0) openStack.splice(index, 1);
  if (!openStack.length) {
    document.body.style.overflow = '';
    if (!fromHistory && history.state?.modalGuard) {
      ignoredPops += 1;
      history.back();
    }
  }
  if (previouslyFocusedElement && typeof previouslyFocusedElement.focus === 'function') {
    previouslyFocusedElement.focus({ preventScroll: true });
    previouslyFocusedElement = null;
  }
}

window.addEventListener('popstate', () => {
  if (ignoredPops > 0) {
    ignoredPops -= 1;
    return;
  }
  const top = openStack[openStack.length - 1];
  if (!top) return;
  closeModal(top, { fromHistory: true });
  // Quedan modales debajo: se repone la entrada para que "atras" siga cerrando.
  if (openStack.length) history.pushState({ modalGuard: true }, '');
});

document.addEventListener('keydown', (event) => {
  if (event.key !== 'Escape') return;
  const top = openStack[openStack.length - 1];
  if (top) closeModal(top);
});

export function openConfirm({ title, text, onAccept }) {
  $('#confirm-title').textContent = title;
  $('#confirm-text').textContent = text;
  state.ui.pendingConfirm = onAccept;
  openModal('modal-confirm');
}

export function showToast(message, kind = 'success', { actionLabel = '', onAction = null, duration = 0 } = {}) {
  const container = $('#toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${kind}`;
  const text = document.createElement('span');
  text.textContent = message;
  toast.appendChild(text);

  const dismiss = () => {
    toast.classList.remove('visible');
    setTimeout(() => toast.remove(), 220);
  };

  if (actionLabel && typeof onAction === 'function') {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'toast-action';
    button.textContent = actionLabel;
    button.addEventListener('click', () => {
      dismiss();
      onAction();
    }, { once: true });
    toast.appendChild(button);
  }

  container.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add('visible'));
  setTimeout(dismiss, duration || (actionLabel ? 6000 : 2600));
}

/** Vibracion corta de confirmacion (solo Android la soporta; en el resto no hace nada). */
export function haptic(pattern = 12) {
  try {
    navigator.vibrate?.(pattern);
  } catch (error) {
    // sin vibracion no pasa nada
  }
}

export function bindSwipeToDismiss() {
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

export async function withFormLock(form, fn) {
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

export function toggleDeleteButton(selector, entity) {
  const button = $(selector);
  if (!button) return;
  button.classList.toggle('hidden', !entity);
  button.dataset.entityId = entity?.id || '';
  button.dataset.entityName = entity?.name || '';
}
