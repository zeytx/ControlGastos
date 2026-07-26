# Finanzas Locales

Control financiero personal que corre entero en el navegador: sin backend, sin cuentas y sin paso de build. Los datos viven en IndexedDB del dispositivo; la unica salida a internet es la IA opcional de OpenAI, y solo cuando tu la disparas.

## Idea central: ciclos de sueldo, no meses calendario

Un mes calendario no dice nada si te pagan el 15. La app arma **ciclos** que van de un sueldo al siguiente, y todo (gastos, cuotas de tarjeta, sobrante) se mide contra el sueldo que lo cubre.

- Un **recurrente de ingreso** marcado como sueldo principal abre cada ciclo.
- Una **compra con tarjeta** no se paga cuando la haces, sino cuando la tarjeta corta: se asigna al primer sueldo que llega despues de la fecha de cierre.
- El **sobrante libre** de un ciclo es lo liquido que queda despues de cubrir tarjetas y deudas de ese ciclo.

## Estado derivado vs estado guardado

Regla del motor: **lo que se puede calcular desde los movimientos, se calcula**. Nunca se guarda un total que pueda quedar desincronizado.

| Se guarda | Se deriva de las transacciones |
|---|---|
| `openingOutstandingAmount` de la deuda | `outstandingAmount`, `paidAmount`, `installmentsPaid` |
| `openingAmount` de la meta | `currentAmount`, `contributedAmount` |
| Cargos y pagos de tarjeta | Estados de cuenta, pendiente, saldo a favor |
| Recurrentes (plantilla) | Sus ocurrencias mes a mes |

Cuando editas "saldo pendiente" o "ahorrado hoy" en un formulario, la app recalcula el valor de apertura para que los pagos ya registrados sigan cuadrando.

Una ocurrencia de recurrente editada a mano queda marcada con `manualOverride` y deja de regenerarse desde la plantilla.

## Sobrante a ahorro

Por defecto la app **propone** el traslado y tu lo confirmas, porque una transferencia que tu banco no hizo no deberia aparecer en tus saldos. En Ajustes puedes cambiarlo a automatico.

## Navegacion

Cinco secciones, una visible a la vez. En movil la barra va abajo (al alcance del pulgar), en escritorio arriba. La pestana activa queda en la URL (`#tarjetas`) y en `localStorage`, asi que al volver abres donde estabas.

`Inicio` ciclo, tope y dashboard · `Movs` movimientos y filtros · `Tarjetas` corte y pago · `Cuentas` patrimonio · `Plan` metas, deudas, recurrentes y agenda.

## Archivos

```
index.html          estructura, paneles y modales
styles.css          tema oscuro/claro
db.js               motor financiero + IndexedDB (global FinanceDB)
ai.js               borradores desde imagen, PDF o texto (global FinanceAI)
sw.js               service worker, app shell cache-first
vendor/             pdf.js, cargado bajo demanda
tests/              tests del motor con el runner nativo de Node

js/                 interfaz, modulos ES nativos
  main.js           arranque y cableado de eventos
  render.js         orquestador de render y recarga
  navigation.js     pestanas
  state.js          estado en memoria y selectores
  metrics.js        calculos derivados del dashboard
  format.js         montos, fechas y etiquetas
  charts.js         SVG de dona y tendencia
  dom.js            consultas, modales, toasts
  options.js        selects de los formularios
  modals.js         apertura y precarga de modales
  forms.js          envio de formularios y backups
  theme.js          tema claro/oscuro
  constants.js      etiquetas fijas
  views/            un modulo por seccion visible
```

Las dependencias entre modulos son aciclicas: `state` no importa a nadie, `format` y `metrics` leen estado, las vistas leen todo eso, `render` orquesta las vistas y `main` cablea eventos. No hay bundler ni paso de build: el navegador carga los modulos tal cual.

## Dependencias

Cero en runtime, a proposito. La unica libreria del repo es **pdf.js** (`vendor/`), y se carga con `import()` dinamico solo cuando analizas un PDF, asi que no pesa en el arranque ni en el precache del service worker.

Sin framework no hay paso de build, y eso es justo lo que permite que esto viva en GitHub Pages y funcione offline abriendo el HTML. Los tests corren con `node --test`, sin dependencias de desarrollo.

## Tests

Sin dependencias: usan `node --test`.

```bash
npm test
```

Cubren fechas locales, reparto de cuotas, asignacion a ciclos, derivacion de deudas y metas, sobrepago de tarjeta, overrides de recurrentes, saldos con corte y presupuesto.

## Desarrollo

Cualquier servidor estatico sirve:

```bash
python3 -m http.server 4173
```

Al desplegar, sube la version del query en `index.html` (`?v=3.0.0`) y el `CACHE_NAME` de `sw.js`. Sin eso, los navegadores sirven JS viejo desde cache.

## Backups

Los datos viven solo en este dispositivo y este navegador. Exporta el JSON completo desde Ajustes cada cierto tiempo: si borras los datos del navegador, no hay forma de recuperarlos. El import valida el archivo antes de tocar nada y restaura tus datos si algo falla.

## API key de OpenAI

Se guarda en `localStorage` de este navegador y viaja directo a la API de OpenAI. No pasa por ningun servidor intermedio. En un dispositivo compartido, mejor no guardarla.
