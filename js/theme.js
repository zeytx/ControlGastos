/* Tema claro/oscuro, o el del sistema ("auto"). */

const systemDark = window.matchMedia('(prefers-color-scheme: dark)');
let preference = 'dark';

export function syncThemeMeta(theme) {
  const themeColor = theme === 'light' ? '#F6F5FA' : '#0B0B12';
  document.querySelectorAll('meta[name="theme-color"]').forEach((meta) => meta.setAttribute('content', themeColor));
}

function resolve(theme) {
  if (theme === 'auto') return systemDark.matches ? 'dark' : 'light';
  return theme === 'light' ? 'light' : 'dark';
}

export function applyTheme(theme) {
  preference = ['dark', 'light', 'auto'].includes(theme) ? theme : 'dark';
  const resolved = resolve(preference);
  document.documentElement.dataset.theme = resolved;
  document.documentElement.style.colorScheme = resolved;
  try {
    localStorage.setItem('theme', preference);
  } catch (error) {
    // modo privado: se vuelve a leer de los ajustes al abrir
  }
  syncThemeMeta(resolved);
}

export function getThemePreference() {
  return preference;
}

systemDark.addEventListener?.('change', () => {
  if (preference === 'auto') applyTheme('auto');
});
