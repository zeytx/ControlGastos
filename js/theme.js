/* Tema claro/oscuro. */

export function syncThemeMeta(theme) {
  const themeColor = theme === 'light' ? '#F5F3F7' : '#000000';
  const meta = document.querySelector('meta[name="theme-color"]');
  if (meta) {
    meta.setAttribute('content', themeColor);
  }
}

export function applyTheme(theme) {
  const normalizedTheme = theme === 'light' ? 'light' : 'dark';
  document.documentElement.dataset.theme = normalizedTheme;
  document.documentElement.style.colorScheme = normalizedTheme;
  localStorage.setItem('theme', normalizedTheme);
  syncThemeMeta(normalizedTheme);
}
