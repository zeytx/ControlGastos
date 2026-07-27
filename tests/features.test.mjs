import test from 'node:test';
import assert from 'node:assert/strict';

import {
  FinanceDB,
  internals,
  makeSettings,
  makeAccounts,
  makeCard,
  tx
} from './harness.mjs';

/* ===== Clasificador local de categorias ===== */

function historial() {
  return [
    tx({ type: 'expense', amount: 40, date: '2026-01-05', description: 'Metro Miraflores', categoryId: 'food', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 55, date: '2026-02-05', description: 'Metro San Isidro', categoryId: 'food', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 30, date: '2026-03-05', description: 'METRO surco', categoryId: 'food', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 25, date: '2026-01-08', description: 'Uber al trabajo', categoryId: 'transport', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 22, date: '2026-02-08', description: 'Uber a casa', categoryId: 'transport', fromAccountId: 'bank-main' }),
    tx({ type: 'card_charge', amount: 90, date: '2026-02-10', description: 'Netflix mensual', categoryId: 'entertainment', cardId: 'card-1', installmentCount: 1 })
  ];
}

test('sugiere la categoria por coincidencia exacta de descripcion', () => {
  const suggestion = internals.suggestCategory('Metro Miraflores', historial());
  assert.equal(suggestion.categoryId, 'food');
  assert.equal(suggestion.source, 'exact');
});

test('sugiere por palabra clave aunque la descripcion cambie', () => {
  const suggestion = internals.suggestCategory('Metro Chacarilla compra', historial());
  assert.equal(suggestion.categoryId, 'food');
  assert.equal(suggestion.source, 'token');
});

test('la sugerencia ignora tildes y mayusculas', () => {
  const historia = [
    tx({ type: 'expense', amount: 20, date: '2026-01-05', description: 'Farmacía Inkafarma', categoryId: 'health', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 20, date: '2026-02-05', description: 'farmacia inkafarma', categoryId: 'health', fromAccountId: 'bank-main' })
  ];
  const suggestion = internals.suggestCategory('FARMACIA INKAFARMA', historia);
  assert.equal(suggestion.categoryId, 'health');
});

test('no inventa categoria cuando nunca vio nada parecido', () => {
  assert.equal(internals.suggestCategory('Concierto en el estadio', historial()), null);
  assert.equal(internals.suggestCategory('', historial()), null);
});

test('gana la categoria mas usada cuando hay historial mezclado', () => {
  const historia = [
    tx({ type: 'expense', amount: 10, date: '2026-01-01', description: 'Wong', categoryId: 'food', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 10, date: '2026-01-02', description: 'Wong', categoryId: 'food', fromAccountId: 'bank-main' }),
    tx({ type: 'expense', amount: 10, date: '2026-01-03', description: 'Wong', categoryId: 'home', fromAccountId: 'bank-main' })
  ];
  const suggestion = internals.suggestCategory('Wong', historia);
  assert.equal(suggestion.categoryId, 'food');
  assert.ok(suggestion.confidence > 0.6);
});

/* ===== Multimoneda ===== */

test('convierte a la moneda base con el tipo de cambio configurado', () => {
  const settings = makeSettings({ exchangeRates: { USD: 3.8 } });
  assert.equal(internals.convertToBase(100, 'USD', settings), 380);
  assert.equal(internals.convertToBase(100, 'PEN', settings), 100);
  assert.equal(internals.convertToBase(100, '', settings), 100);
});

test('el patrimonio consolida cuentas en soles y dolares', () => {
  const settings = makeSettings({ exchangeRates: { USD: 4 } });
  const accounts = [
    internals.normalizeAccount({ id: 'pen', name: 'Soles', kind: 'bank', currency: 'PEN', includeInNetWorth: true }),
    internals.normalizeAccount({ id: 'usd', name: 'Dolares', kind: 'savings', currency: 'USD', includeInNetWorth: true })
  ];
  const balances = { pen: 1000, usd: 500 };

  const netWorth = internals.calculateNetWorth(accounts, balances, [], [], settings);
  assert.equal(netWorth.liquid, 3000, '1000 PEN + 500 USD * 4');
  assert.deepEqual(netWorth.byCurrency, { PEN: 1000, USD: 500 });
  assert.equal(netWorth.baseCurrency, 'PEN');
});

test('una transferencia entre monedas usa toAmount en la cuenta destino', () => {
  const accounts = [
    internals.normalizeAccount({ id: 'pen', name: 'Soles', kind: 'bank', currency: 'PEN' }),
    internals.normalizeAccount({ id: 'usd', name: 'Dolares', kind: 'savings', currency: 'USD' })
  ];
  const transactions = [
    tx({ type: 'transfer', amount: 380, toAmount: 100, date: '2026-02-01', fromAccountId: 'pen', toAccountId: 'usd', description: 'Compra de dolares' })
  ];

  const balances = internals.calculateBalances(accounts, transactions, '2026-02-02');
  assert.equal(balances.pen, -380, 'salen soles');
  assert.equal(balances.usd, 100, 'entran dolares, no soles');
});

test('sin toAmount la transferencia mueve el mismo monto en ambos lados', () => {
  const accounts = makeAccounts();
  const transactions = [
    tx({ type: 'transfer', amount: 300, date: '2026-02-01', fromAccountId: 'bank-main', toAccountId: 'savings-main', description: 'Ahorro' })
  ];
  const balances = internals.calculateBalances(accounts, transactions, '2026-02-02');
  assert.equal(balances['bank-main'], -300);
  assert.equal(balances['savings-main'], 300);
});

/* ===== Presupuesto por categoria ===== */

test('el presupuesto por categoria mide cada tope por separado', () => {
  const settings = makeSettings({
    monthlyBudget: 2000,
    categoryBudgets: { food: 500, transport: 200 }
  });
  const categories = [
    { id: 'food', name: 'Comida', type: 'expense' },
    { id: 'transport', name: 'Transporte', type: 'expense' }
  ];
  const transactions = [
    tx({ type: 'expense', amount: 600, date: '2026-02-01', description: 'Mercado', categoryId: 'food', fromAccountId: 'bank-main', budgetCycleId: 'c1' }),
    tx({ type: 'expense', amount: 50, date: '2026-02-02', description: 'Taxi', categoryId: 'transport', fromAccountId: 'bank-main', budgetCycleId: 'c1' })
  ];

  const budget = internals.buildBudgetStatus(settings, { id: 'c1' }, transactions, [], categories, makeAccounts());
  const comida = budget.byCategory.find((item) => item.categoryId === 'food');
  const transporte = budget.byCategory.find((item) => item.categoryId === 'transport');

  assert.equal(comida.spent, 600);
  assert.equal(comida.overspent, true, 'comida se paso de 500');
  assert.equal(transporte.spent, 50);
  assert.equal(transporte.overspent, false);
  assert.equal(budget.byCategory[0].categoryId, 'food', 'se ordena por lo mas ajustado');
});

test('el presupuesto por categoria cuenta tambien las compras con tarjeta', () => {
  const settings = makeSettings({ categoryBudgets: { shopping: 300 } });
  const categories = [{ id: 'shopping', name: 'Compras', type: 'expense' }];
  const transactions = [
    tx({ type: 'card_charge', amount: 400, date: '2026-02-05', description: 'Zapatillas', categoryId: 'shopping', cardId: 'card-1', installmentCount: 1, budgetCycleId: 'c1' })
  ];

  const budget = internals.buildBudgetStatus(settings, { id: 'c1' }, transactions, [], categories, makeAccounts());
  assert.equal(budget.byCategory[0].spent, 400);
  assert.equal(budget.byCategory[0].overspent, true);
});

test('un gasto en dolares se convierte antes de medir el tope', () => {
  const settings = makeSettings({ monthlyBudget: 1000, exchangeRates: { USD: 4 } });
  const accounts = [internals.normalizeAccount({ id: 'usd', name: 'Dolares', kind: 'bank', currency: 'USD' })];
  const transactions = [
    tx({ type: 'expense', amount: 100, date: '2026-02-01', description: 'Suscripcion', categoryId: 'services', fromAccountId: 'usd', budgetCycleId: 'c1' })
  ];

  const budget = internals.buildBudgetStatus(settings, { id: 'c1' }, transactions, [], [], accounts);
  assert.equal(budget.spent, 400, '100 USD son 400 soles contra el tope');
});

/* ===== Conciliacion con el estado de cuenta ===== */

test('cuadra los movimientos del estado con los registrados', () => {
  const appTransactions = [
    tx({ id: 't1', type: 'card_charge', amount: 120.5, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Rappi almuerzo', cardId: 'card-1', installmentCount: 1 }),
    tx({ id: 't2', type: 'card_charge', amount: 89.9, date: '2026-02-06', purchaseDate: '2026-02-06', description: 'Netflix', cardId: 'card-1', installmentCount: 1 })
  ];
  const movements = [
    { date: '2026-02-03', description: 'RAPPI PERU SAC', amount: 120.5 },
    { date: '2026-02-06', description: 'NETFLIX.COM', amount: 89.9 }
  ];

  const result = internals.compareStatementMovements(movements, appTransactions);
  assert.equal(result.matched.length, 2);
  assert.equal(result.missing.length, 0);
  assert.equal(result.extra.length, 0);
  assert.equal(result.difference, 0);
});

test('detecta el movimiento que olvidaste registrar', () => {
  const appTransactions = [
    tx({ id: 't1', type: 'card_charge', amount: 120.5, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Rappi', cardId: 'card-1', installmentCount: 1 })
  ];
  const movements = [
    { date: '2026-02-03', description: 'RAPPI PERU', amount: 120.5 },
    { date: '2026-02-09', description: 'FARMACIA UNIVERSAL', amount: 45.8 }
  ];

  const result = internals.compareStatementMovements(movements, appTransactions);
  assert.equal(result.matched.length, 1);
  assert.equal(result.missing.length, 1);
  assert.equal(result.missing[0].amount, 45.8);
  assert.equal(result.difference, 45.8, 'el estado tiene 45.80 mas que la app');
});

test('detecta lo que registraste de mas y no aparece en el estado', () => {
  const appTransactions = [
    tx({ id: 't1', type: 'card_charge', amount: 120.5, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Rappi', cardId: 'card-1', installmentCount: 1 }),
    tx({ id: 't2', type: 'card_charge', amount: 200, date: '2026-02-04', purchaseDate: '2026-02-04', description: 'Compra duplicada', cardId: 'card-1', installmentCount: 1 })
  ];
  const movements = [{ date: '2026-02-03', description: 'RAPPI PERU', amount: 120.5 }];

  const result = internals.compareStatementMovements(movements, appTransactions);
  assert.equal(result.extra.length, 1);
  assert.equal(result.extra[0].id, 't2');
  assert.equal(result.difference, -200);
});

test('tolera unos dias de diferencia entre la compra y lo que reporta el banco', () => {
  const appTransactions = [
    tx({ id: 't1', type: 'card_charge', amount: 75, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Cine', cardId: 'card-1', installmentCount: 1 })
  ];
  const movements = [{ date: '2026-02-05', description: 'CINEPLANET', amount: 75 }];

  const result = internals.compareStatementMovements(movements, appTransactions);
  assert.equal(result.matched.length, 1, '2 dias de diferencia siguen siendo el mismo cargo');
});

test('no cuadra dos cargos del mismo monto con un solo movimiento', () => {
  const appTransactions = [
    tx({ id: 't1', type: 'card_charge', amount: 50, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Cafe', cardId: 'card-1', installmentCount: 1 }),
    tx({ id: 't2', type: 'card_charge', amount: 50, date: '2026-02-03', purchaseDate: '2026-02-03', description: 'Cafe', cardId: 'card-1', installmentCount: 1 })
  ];
  const movements = [{ date: '2026-02-03', description: 'STARBUCKS', amount: 50 }];

  const result = internals.compareStatementMovements(movements, appTransactions);
  assert.equal(result.matched.length, 1);
  assert.equal(result.extra.length, 1, 'el segundo cargo queda como sobrante');
});

/* ===== Patrimonio en el tiempo ===== */

test('el historial de patrimonio devuelve un punto por ciclo ya empezado', () => {
  const settings = makeSettings();
  const accounts = makeAccounts();
  const hoy = FinanceDB.getToday();

  const cycles = [
    internals.normalizeCycle({ id: 'c1', startDate: FinanceDB.addMonths(hoy, -2, 15), endDate: FinanceDB.addMonths(hoy, -1, 14), status: 'closed' }),
    internals.normalizeCycle({ id: 'c2', startDate: FinanceDB.addMonths(hoy, -1, 15), endDate: FinanceDB.addMonths(hoy, 0, 14), status: 'closed' }),
    internals.normalizeCycle({ id: 'c3', startDate: FinanceDB.addMonths(hoy, 6, 15), endDate: FinanceDB.addMonths(hoy, 7, 14), status: 'open' })
  ];
  const transactions = [
    tx({ type: 'income', amount: 1000, date: FinanceDB.addMonths(hoy, -2, 16), toAccountId: 'bank-main', description: 'Sueldo' }),
    tx({ type: 'income', amount: 1000, date: FinanceDB.addMonths(hoy, -1, 16), toAccountId: 'bank-main', description: 'Sueldo' })
  ];

  const history = internals.buildNetWorthHistory(cycles, accounts, transactions, [], settings);
  assert.equal(history.length, 2, 'el ciclo futuro no entra');
  assert.equal(history[0].net, 1000);
  assert.equal(history[1].net, 2000, 'el patrimonio acumula');
});
