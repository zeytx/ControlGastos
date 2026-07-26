/* Navegacion por secciones: una pestana visible a la vez. */
import { $, $$ } from './dom.js';

export const TABS = [
  { id: 'inicio', label: 'Inicio', icon: '◉', hint: 'Tu ciclo de un vistazo' },
  { id: 'movimientos', label: 'Movs', icon: '≡', hint: 'Movimientos del ciclo' },
  { id: 'tarjetas', label: 'Tarjetas', icon: '▤', hint: 'Corte y pago' },
  { id: 'cuentas', label: 'Cuentas', icon: '$', hint: 'Patrimonio y saldos' },
  { id: 'plan', label: 'Plan', icon: '◆', hint: 'Metas, deudas y recurrentes' }
];

const STORAGE_KEY = 'active-tab';
const DEFAULT_TAB = 'inicio';

let currentTab = DEFAULT_TAB;

function isValidTab(id) {
  return TABS.some((tab) => tab.id === id);
}

function readTabFromUrl() {
  const hash = String(window.location.hash || '').replace('#', '').trim();
  return isValidTab(hash) ? hash : '';
}

export function getActiveTab() {
  return currentTab;
}

export function activateTab(id, { updateHash = true, scrollToTop = true } = {}) {
  const target = isValidTab(id) ? id : DEFAULT_TAB;
  currentTab = target;

  $$('[data-panel]').forEach((panel) => {
    panel.classList.toggle('hidden', panel.dataset.panel !== target);
  });

  $$('[data-tab]').forEach((button) => {
    const isActive = button.dataset.tab === target;
    button.classList.toggle('active', isActive);
    button.setAttribute('aria-selected', isActive ? 'true' : 'false');
    button.setAttribute('tabindex', isActive ? '0' : '-1');
  });

  const meta = TABS.find((tab) => tab.id === target);
  const title = $('#section-title');
  if (title && meta) title.textContent = meta.hint;

  try {
    localStorage.setItem(STORAGE_KEY, target);
  } catch (error) {
    // modo privado: se pierde la preferencia, no pasa nada
  }

  if (updateHash && window.location.hash !== `#${target}`) {
    history.replaceState(null, '', `#${target}`);
  }

  if (scrollToTop) {
    window.scrollTo({ top: 0, behavior: 'auto' });
  }
}

function moveFocus(direction) {
  const index = TABS.findIndex((tab) => tab.id === currentTab);
  const next = TABS[(index + direction + TABS.length) % TABS.length];
  activateTab(next.id);
  $(`[data-tab="${next.id}"]`)?.focus();
}

export function initNavigation() {
  $$('[data-tab]').forEach((button) => {
    button.addEventListener('click', () => activateTab(button.dataset.tab));
  });

  $('#tab-bar')?.addEventListener('keydown', (event) => {
    if (event.key === 'ArrowRight') {
      event.preventDefault();
      moveFocus(1);
    } else if (event.key === 'ArrowLeft') {
      event.preventDefault();
      moveFocus(-1);
    }
  });

  window.addEventListener('hashchange', () => {
    const fromUrl = readTabFromUrl();
    if (fromUrl) activateTab(fromUrl, { updateHash: false });
  });

  let stored = '';
  try {
    stored = localStorage.getItem(STORAGE_KEY) || '';
  } catch (error) {
    stored = '';
  }

  activateTab(readTabFromUrl() || stored || DEFAULT_TAB, { scrollToTop: false });
}

/** Lleva al usuario a la pestana que contiene un elemento, util para enlaces cruzados. */
export function revealPanelFor(selector) {
  const element = $(selector);
  const panel = element?.closest('[data-panel]');
  if (panel) activateTab(panel.dataset.panel);
  return element;
}
