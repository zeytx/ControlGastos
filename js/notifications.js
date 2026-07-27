/* Avisos locales de corte y vencimiento de tarjeta.
   Sin servidor no hay push real: la revision corre cuando abres la app,
   que en una PWA instalada es suficiente para no perder un pago. */
import { $ } from './dom.js';
import { formatCurrency, formatDate } from './format.js';
import { getSnapshot } from './state.js';

const STORAGE_KEY = 'last-card-notification';

export function notificationsSupported() {
  return typeof Notification !== 'undefined';
}

export function notificationPermission() {
  return notificationsSupported() ? Notification.permission : 'unsupported';
}

export function describeNotificationState() {
  if (!notificationsSupported()) {
    return 'Este navegador no permite avisos. En iPhone funcionan solo si agregas la app a la pantalla de inicio.';
  }
  if (Notification.permission === 'granted') return 'Avisos activos en este dispositivo.';
  if (Notification.permission === 'denied') {
    return 'Bloqueaste los avisos. Habilitalos desde los ajustes del navegador para esta pagina.';
  }
  return 'Los avisos estan disponibles pero aun no los activaste.';
}

export async function requestNotificationPermission() {
  if (!notificationsSupported()) {
    throw new Error('Este navegador no soporta avisos. En iPhone instala la app en la pantalla de inicio.');
  }
  const result = await Notification.requestPermission();
  if (result !== 'granted') {
    throw new Error('No diste permiso para los avisos.');
  }
  return result;
}

/** Cortes y vencimientos dentro de la ventana configurada. */
export function collectCardAlerts(daysBefore = 3) {
  const snapshot = getSnapshot();
  const today = FinanceDB.getToday();
  const limit = FinanceDB.addDays(today, daysBefore);
  const alerts = [];

  (snapshot.cards || [])
    .filter((card) => !card.archived)
    .forEach((card) => {
      const label = `${card.bankName} • ${card.last4}`;

      if (
        card.nextClosingDate &&
        FinanceDB.compareDate(card.nextClosingDate, today) >= 0 &&
        FinanceDB.compareDate(card.nextClosingDate, limit) <= 0
      ) {
        alerts.push({
          key: `close-${card.id}-${card.nextClosingDate}`,
          kind: 'close',
          title: `${label} corta el ${formatDate(card.nextClosingDate)}`,
          body: 'Lo que compres despues ya entra al siguiente estado de cuenta.'
        });
      }

      const pending = (snapshot.statements || []).filter(
        (statement) => statement.cardId === card.id && statement.pendingAmount > 0 && statement.dueDate
      );

      pending.forEach((statement) => {
        if (
          FinanceDB.compareDate(statement.dueDate, today) >= 0 &&
          FinanceDB.compareDate(statement.dueDate, limit) <= 0
        ) {
          alerts.push({
            key: `due-${card.id}-${statement.dueDate}`,
            kind: 'due',
            title: `${label} vence el ${formatDate(statement.dueDate)}`,
            body: `Tienes ${formatCurrency(statement.pendingAmount)} pendientes.`
          });
        }
      });
    });

  return alerts;
}

function readNotified() {
  try {
    return new Set(JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]'));
  } catch (error) {
    return new Set();
  }
}

function writeNotified(keys) {
  try {
    // Solo interesan los avisos recientes; la lista no debe crecer sin fin.
    localStorage.setItem(STORAGE_KEY, JSON.stringify([...keys].slice(-40)));
  } catch (error) {
    // sin localStorage se reavisa: molesto pero inofensivo
  }
}

async function show(alert) {
  const options = {
    body: alert.body,
    tag: alert.key,
    icon: 'icons/icon-192.png',
    badge: 'icons/icon-192.png'
  };

  // En una PWA instalada la notificacion debe salir del service worker.
  const registration = await navigator.serviceWorker?.getRegistration?.();
  if (registration?.showNotification) {
    await registration.showNotification(alert.title, options);
    return;
  }
  new Notification(alert.title, options);
}

/** Revisa vencimientos y avisa una sola vez por evento. */
export async function runCardDueCheck() {
  const settings = getSnapshot().settings || {};
  const config = settings.notifications || {};
  if (config.cardDueEnabled === false) return { shown: 0, pending: 0 };

  const alerts = collectCardAlerts(config.daysBefore || 3);
  updateBadge(alerts.length);

  if (!notificationsSupported() || Notification.permission !== 'granted') {
    return { shown: 0, pending: alerts.length };
  }

  const notified = readNotified();
  const fresh = alerts.filter((alert) => !notified.has(alert.key));

  for (const alert of fresh) {
    try {
      await show(alert);
      notified.add(alert.key);
    } catch (error) {
      // si el navegador rechaza el aviso, seguimos con el resto
    }
  }

  writeNotified(notified);
  return { shown: fresh.length, pending: alerts.length };
}

function updateBadge(count) {
  if (!('setAppBadge' in navigator)) return;
  if (count > 0) navigator.setAppBadge(count).catch(() => {});
  else navigator.clearAppBadge?.().catch(() => {});
}

/** Aviso de prueba para que el usuario confirme que llegan. */
export async function sendTestNotification() {
  await show({
    key: 'test',
    title: 'Avisos activados',
    body: 'Te avisare cuando una tarjeta este por cortar o vencer.'
  });
}

export function renderNotificationStatus() {
  const status = $('#notifications-status');
  if (!status) return;
  const alerts = collectCardAlerts(getSnapshot().settings?.notifications?.daysBefore || 3);
  const base = describeNotificationState();
  status.textContent = alerts.length
    ? `${base} Ahora mismo hay ${alerts.length} aviso${alerts.length === 1 ? '' : 's'} cerca.`
    : base;
}
