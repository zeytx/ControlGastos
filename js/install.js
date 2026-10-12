/* Instalacion en la pantalla de inicio y accesos directos (Android primero). */
import { $, closeModal, escapeHtml, openModal, showToast } from './dom.js';
import { activateTab } from './navigation.js';
import { openQuickAdd, parseAmount } from './quickadd.js';
import { state } from './state.js';

let deferredPrompt = null;
const listeners = new Set();

export function onInstallAvailabilityChange(callback) {
  listeners.add(callback);
}

function notify() {
  state.platform.canInstall = !!deferredPrompt;
  listeners.forEach((callback) => callback());
}

export function detectBrowser() {
  const ua = navigator.userAgent || '';
  return {
    isAndroid: /Android/i.test(ua),
    isIOS: state.platform.isIOS,
    isSamsung: /SamsungBrowser/i.test(ua),
    isFirefox: /Firefox|FxiOS/i.test(ua),
    // WhatsApp, Facebook, Instagram, TikTok... abren los enlaces en su propio
    // navegador interno, que no puede instalar nada.
    isInApp: /FBAN|FBAV|FB_IAB|Instagram|WhatsApp|Line\/|TikTok|musical_ly|Snapchat|; wv\)/i.test(ua),
    isIOSChrome: /CriOS/i.test(ua)
  };
}

export function canPromptInstall() {
  return !!deferredPrompt;
}

export function isInstalled() {
  return state.platform.standalone;
}

export function initInstall() {
  window.addEventListener('beforeinstallprompt', (event) => {
    // Guardamos el evento para lanzar el dialogo desde nuestro boton grande.
    event.preventDefault();
    deferredPrompt = event;
    notify();
  });

  window.addEventListener('appinstalled', () => {
    deferredPrompt = null;
    closeModal('modal-install');
    showToast('Listo: ya la tienes en tu pantalla de inicio', 'success');
    notify();
  });

  window.matchMedia('(display-mode: standalone)').addEventListener?.('change', (event) => {
    state.platform.standalone = event.matches;
    notify();
  });
}

export async function promptInstall() {
  if (!deferredPrompt) {
    openInstallGuide();
    return false;
  }
  const promptEvent = deferredPrompt;
  deferredPrompt = null;
  promptEvent.prompt();
  const choice = await promptEvent.userChoice.catch(() => ({ outcome: 'dismissed' }));
  notify();
  return choice?.outcome === 'accepted';
}

function getInstallSteps() {
  const browser = detectBrowser();
  if (browser.isInApp) {
    return {
      title: 'Ábrela en Chrome primero',
      intro: 'Estás viendo la app dentro de otra aplicación (WhatsApp, Facebook o similar). Desde ahí no se puede instalar.',
      steps: [
        'Toca los tres puntos <b>⋮</b> de arriba a la derecha.',
        'Elige <b>Abrir en Chrome</b> (o "Abrir en el navegador").',
        'Ya en Chrome, vuelve a tocar <b>Instalar</b>.'
      ],
      copyLink: true
    };
  }
  if (browser.isIOS) {
    return {
      title: 'Agrégala a tu iPhone',
      intro: browser.isIOSChrome
        ? 'En iPhone conviene hacerlo desde Safari.'
        : 'Son tres toques en Safari:',
      steps: [
        'Toca el botón <b>Compartir</b> (el cuadrado con una flecha hacia arriba).',
        'Baja y toca <b>Agregar a inicio</b>.',
        'Toca <b>Agregar</b> arriba a la derecha.'
      ]
    };
  }
  if (browser.isSamsung) {
    return {
      title: 'Agrégala a tu Samsung',
      intro: 'En Samsung Internet:',
      steps: [
        'Toca el menú <b>≡</b> abajo a la derecha.',
        'Toca <b>Añadir página a</b> y luego <b>Pantalla de inicio</b>.',
        'Confirma con <b>Añadir</b>. El icono aparece en tu escritorio.'
      ]
    };
  }
  if (browser.isAndroid) {
    return {
      title: 'Ponla en tu pantalla de inicio',
      intro: 'En Chrome para Android:',
      steps: [
        'Toca los tres puntos <b>⋮</b> arriba a la derecha.',
        'Toca <b>Instalar app</b>. Según tu versión de Chrome puede decir <b>Agregar a la pantalla principal</b> o <b>Instalar y crear acceso directo</b>.',
        'Confirma con <b>Instalar</b>. El icono aparece en tu escritorio como cualquier app.'
      ]
    };
  }
  return {
    title: 'Instálala en este equipo',
    intro: 'En Chrome o Edge de computadora:',
    steps: [
      'Busca el icono de <b>instalar</b> (una pantalla con una flecha) al final de la barra de direcciones.',
      'Haz clic en <b>Instalar</b>.',
      'Para el celular, abre esta misma dirección en Chrome de Android.'
    ]
  };
}

export function openInstallGuide() {
  const guide = getInstallSteps();
  $('#install-guide-title').textContent = guide.title;
  // Los pasos son texto fijo nuestro con <b>; nada viene del usuario.
  $('#install-guide-body').innerHTML = `
    <p class="install-guide-intro">${escapeHtml(guide.intro)}</p>
    <ol class="install-steps">
      ${guide.steps.map((step, index) => `<li><span class="install-step-num">${index + 1}</span><span>${step}</span></li>`).join('')}
    </ol>
    ${guide.copyLink ? '<button type="button" class="btn-secondary btn-full" id="btn-copy-app-link">Copiar el enlace de la app</button>' : ''}
    <div class="install-tip">
      <strong>Truco:</strong> ya instalada, <b>mantén presionado el icono</b> para ver "Anotar gasto" y "Anotar ingreso".
      Puedes arrastrar ese acceso a tu escritorio y tener un botón directo para anotar.
    </div>
  `;
  $('#btn-install-now').classList.toggle('hidden', !canPromptInstall());
  openModal('modal-install');
}

export function bindInstallEvents() {
  $('#btn-install-now').addEventListener('click', async () => {
    await promptInstall();
  });

  $('#install-guide-body').addEventListener('click', async (event) => {
    if (!event.target.closest('#btn-copy-app-link')) return;
    const url = new URL('./', window.location.href).href;
    try {
      await navigator.clipboard.writeText(url);
      showToast('Enlace copiado. Pégalo en Chrome.', 'success');
    } catch (error) {
      showToast(url, 'info', { duration: 8000 });
    }
  });
}

/* ===== Accesos directos del icono y "Compartir con" desde otras apps ===== */

function extractSharedAmount(text) {
  // "S/ 25.90", "S/.25,90", "PEN 25.90", "monto: 25.90"
  const match = String(text || '').match(/(?:S\/\.?|PEN|soles?|monto:?)\s*([\d.,]+\d)/i);
  return match ? parseAmount(match[1]) : 0;
}

/** Atiende ?action=gasto|ingreso|movimientos y los textos compartidos (share_target). */
export function handleLaunchParams() {
  const params = new URLSearchParams(window.location.search);
  const action = params.get('action');
  const sharedText = [params.get('share_title'), params.get('share_text'), params.get('share_url')]
    .filter(Boolean)
    .join(' ')
    .trim();

  if (!action && !sharedText) return;

  // Se limpia la URL para que recargar no vuelva a abrir el formulario.
  ['action', 'share_title', 'share_text', 'share_url'].forEach((key) => params.delete(key));
  const query = params.toString();
  history.replaceState(null, '', `${window.location.pathname}${query ? `?${query}` : ''}${window.location.hash}`);

  if (sharedText) {
    const amount = extractSharedAmount(sharedText);
    openQuickAdd({ type: 'expense', amount: amount || '', note: sharedText.slice(0, 80) });
    showToast(amount ? 'Revisa el monto y elige la categoría' : 'Escribe el monto y elige la categoría', 'info');
    return;
  }
  if (action === 'gasto') openQuickAdd({ type: 'expense' });
  else if (action === 'ingreso') openQuickAdd({ type: 'income' });
  else if (action === 'movimientos') activateTab('movimientos');
}
