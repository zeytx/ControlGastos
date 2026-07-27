/* Graficos SVG dibujados a mano (dona y tendencia). */
import { $, escapeHtml } from './dom.js';

export function polarToCartesian(cx, cy, radius, angleInDegrees) {
  const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180;
  return {
    x: cx + radius * Math.cos(angleInRadians),
    y: cy + radius * Math.sin(angleInRadians)
  };
}

export function describeArc(cx, cy, radius, startAngle, endAngle) {
  const start = polarToCartesian(cx, cy, radius, endAngle);
  const end = polarToCartesian(cx, cy, radius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? '0' : '1';
  return `M ${start.x.toFixed(3)} ${start.y.toFixed(3)} A ${radius} ${radius} 0 ${largeArcFlag} 0 ${end.x.toFixed(3)} ${end.y.toFixed(3)}`;
}

export function renderDonutSvg(segments, total, centerTop, centerBottom) {
  if (!total) {
    return `
      <svg viewBox="0 0 220 220" class="donut-figure" role="img" aria-label="Sin datos del ciclo">
        <circle cx="110" cy="110" r="76" class="donut-track"></circle>
        <circle cx="110" cy="110" r="52" class="donut-core"></circle>
        <text x="110" y="106" text-anchor="middle" class="donut-number">S/ 0</text>
        <text x="110" y="126" text-anchor="middle" class="donut-label">Sin datos</text>
      </svg>
    `;
  }

  let angle = 0;
  const paths = segments
    .filter((segment) => segment.value > 0)
    .map((segment) => {
      const sweep = Math.max(4, (segment.value / total) * 360);
      const path = describeArc(110, 110, 76, angle, angle + sweep);
      angle += sweep;
      return `<path d="${path}" class="donut-segment ${segment.className}"></path>`;
    })
    .join('');

  return `
    <svg viewBox="0 0 220 220" class="donut-figure" role="img" aria-label="Balance visual del ciclo">
      <circle cx="110" cy="110" r="76" class="donut-track"></circle>
      ${paths}
      <circle cx="110" cy="110" r="52" class="donut-core"></circle>
      <text x="110" y="104" text-anchor="middle" class="donut-number">${escapeHtml(centerTop)}</text>
      <text x="110" y="126" text-anchor="middle" class="donut-label">${escapeHtml(centerBottom)}</text>
    </svg>
  `;
}

/** Barras de patrimonio por ciclo: positivas arriba, negativas abajo. */
export function renderNetWorthSvg(points) {
  if (points.length < 2) {
    return '<div class="chart-empty">Necesitas al menos dos ciclos cerrados para ver como evoluciona tu patrimonio.</div>';
  }

  const width = 520;
  const height = 200;
  const padding = 22;
  const values = points.map((point) => point.net);
  const max = Math.max(...values, 0);
  const min = Math.min(...values, 0);
  const range = max - min || 1;
  const usable = height - padding * 2;
  const zeroY = padding + ((max - 0) / range) * usable;
  const slot = (width - padding * 2) / points.length;
  const barWidth = Math.max(6, Math.min(38, slot * 0.6));

  const bars = points
    .map((point, index) => {
      const center = padding + slot * index + slot / 2;
      const valueY = padding + ((max - point.net) / range) * usable;
      const top = Math.min(valueY, zeroY);
      const barHeight = Math.max(2, Math.abs(valueY - zeroY));
      const className = point.net < 0 ? 'networth-bar negative' : 'networth-bar';
      return `<rect x="${(center - barWidth / 2).toFixed(2)}" y="${top.toFixed(2)}" width="${barWidth.toFixed(2)}" height="${barHeight.toFixed(2)}" rx="4" class="${className}"><title>${escapeHtml(point.date)}</title></rect>`;
    })
    .join('');

  return `
    <svg viewBox="0 0 ${width} ${height}" class="trend-svg" role="img" aria-label="Patrimonio por ciclo">
      <line x1="${padding}" y1="${zeroY.toFixed(2)}" x2="${width - padding}" y2="${zeroY.toFixed(2)}" class="trend-axis"></line>
      ${bars}
    </svg>
  `;
}

export function renderTrendSvg(points) {
  if (!points.length) {
    return '<div class="chart-empty">Aun no hay ritmo suficiente para graficar este ciclo.</div>';
  }

  const width = 520;
  const height = 200;
  const padding = 18;
  const values = points.map((point) => point.value);
  const min = Math.min(...values, 0);
  const max = Math.max(...values, 0);
  const range = max - min || 1;

  const coords = points.map((point, index) => {
    const x = padding + (index / Math.max(points.length - 1, 1)) * (width - padding * 2);
    const y = height - padding - ((point.value - min) / range) * (height - padding * 2);
    return { x, y, date: point.date, value: point.value };
  });

  const polyline = coords.map((point) => `${point.x.toFixed(2)},${point.y.toFixed(2)}`).join(' ');
  const area = `${padding},${height - padding} ${polyline} ${width - padding},${height - padding}`;
  const lastPoint = coords[coords.length - 1];

  return `
    <svg viewBox="0 0 ${width} ${height}" class="trend-svg" role="img" aria-label="Tendencia del ciclo">
      <defs>
        <linearGradient id="trend-fill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--chart-card-fill)"></stop>
          <stop offset="100%" stop-color="rgba(0, 0, 0, 0)"></stop>
        </linearGradient>
      </defs>
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="trend-axis"></line>
      <polygon points="${area}" fill="url(#trend-fill)" class="trend-area"></polygon>
      <polyline points="${polyline}" class="trend-line"></polyline>
      <circle cx="${lastPoint.x.toFixed(2)}" cy="${lastPoint.y.toFixed(2)}" r="5" class="trend-dot"></circle>
    </svg>
  `;
}
