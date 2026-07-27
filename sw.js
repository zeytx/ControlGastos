/* ============================================
   FINANZAS LOCALES - Service Worker
   App shell cache-first (es una app local-first),
   con revalidacion en segundo plano.
   La IA nunca se cachea.
   ============================================ */

const CACHE_NAME = 'finanzas-locales-v9';

// Sin ?v= a proposito: el matching ignora el query string, asi que una misma
// entrada sirve tanto para "js/main.js?v=3.0.0" (pedido por el HTML) como para
// "js/state.js" (pedido por los imports entre modulos).
const APP_SHELL = [
  './',
  './index.html',
  './styles.css',
  './db.js',
  './ai.js',
  './manifest.json',
  './icons/icon-192.png',
  './icons/icon-512.png',
  './js/main.js',
  './js/render.js',
  './js/state.js',
  './js/dom.js',
  './js/format.js',
  './js/metrics.js',
  './js/charts.js',
  './js/constants.js',
  './js/options.js',
  './js/modals.js',
  './js/forms.js',
  './js/theme.js',
  './js/navigation.js',
  './js/notifications.js',
  './js/reconcile.js',
  './js/ask.js',
  './js/views/hero.js',
  './js/views/dashboard.js',
  './js/views/accounts.js',
  './js/views/cards.js',
  './js/views/plan.js',
  './js/views/transactions.js'
  // pdf.js (2.7 MB) queda fuera del precache a proposito: se descarga la
  // primera vez que analizas un PDF y desde ahi queda cacheado en runtime.
];

const MATCH_OPTIONS = { ignoreSearch: true };

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) =>
        // cache: 'reload' evita que el precache se llene desde el cache HTTP
        // viejo del navegador justo cuando acabamos de desplegar.
        Promise.allSettled(
          APP_SHELL.map((url) => cache.add(new Request(url, { cache: 'reload' })))
        )
      )
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((key) => key !== CACHE_NAME)
            .map((key) => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

self.addEventListener('message', (event) => {
  if (event.data === 'skip-waiting') self.skipWaiting();
});

function isCacheable(response) {
  return !!response && response.status === 200 && response.type === 'basic';
}

async function revalidate(request) {
  try {
    const response = await fetch(request);
    if (isCacheable(response)) {
      const cache = await caches.open(CACHE_NAME);
      await cache.put(request, response.clone());
    }
    return response;
  } catch (error) {
    return null;
  }
}

self.addEventListener('fetch', (event) => {
  const request = event.request;
  const url = new URL(request.url);

  if (
    request.method !== 'GET' ||
    url.origin !== self.location.origin ||
    url.hostname === 'api.openai.com'
  ) {
    return;
  }

  // Navegacion: se sirve el shell cacheado de inmediato y se refresca detras.
  if (request.mode === 'navigate') {
    event.respondWith(
      caches.match('./index.html', MATCH_OPTIONS).then((cached) => {
        if (cached) {
          event.waitUntil(revalidate(new Request('./index.html')));
          return cached;
        }
        return fetch(request).catch(() => caches.match('./index.html', MATCH_OPTIONS));
      })
    );
    return;
  }

  event.respondWith(
    caches.match(request, MATCH_OPTIONS).then((cached) => {
      if (cached) {
        event.waitUntil(revalidate(request));
        return cached;
      }
      return revalidate(request).then((response) => response || caches.match('./index.html', MATCH_OPTIONS));
    })
  );
});
