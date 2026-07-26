import test from 'node:test';
import assert from 'node:assert/strict';

import {
  FinanceDB,
  internals,
  makeSettings,
  makeAccounts,
  makeSalaryRecurring,
  makeCard,
  tx
} from './harness.mjs';

test('getToday usa la fecha local, no UTC', () => {
  const local = new Date().toLocaleDateString('en-CA');
  assert.equal(FinanceDB.getToday(), local);
});

test('normalizeDate cae a la fecha local cuando el valor es invalido', () => {
  const local = new Date().toLocaleDateString('en-CA');
  assert.equal(FinanceDB.normalizeDate('no es fecha'), local);
});

test('addDays y addMonths no se corren de dia por zona horaria', () => {
  assert.equal(FinanceDB.addDays('2026-03-31', 1), '2026-04-01');
  assert.equal(FinanceDB.addDays('2026-01-01', -1), '2025-12-31');
  assert.equal(FinanceDB.addMonths('2026-01-31', 1), '2026-02-28');
  assert.equal(FinanceDB.addMonths('2026-12-15', 1, 15), '2027-01-15');
});

test('las cuotas reparten el redondeo en la primera y suman el total', () => {
  const parts = internals.splitInstallmentAmounts(100, 3);
  assert.deepEqual(parts, [33.34, 33.33, 33.33]);
  assert.equal(FinanceDB.roundAmount(parts.reduce((a, b) => a + b, 0)), 100);

  const odd = internals.splitInstallmentAmounts(0.05, 4);
  assert.equal(FinanceDB.roundAmount(odd.reduce((a, b) => a + b, 0)), 0.05);
});

test('una compra en 12 cuotas queda repartida en 12 cierres consecutivos', () => {
  const card = makeCard();
  const charge = tx({
    type: 'card_charge',
    cardId: card.id,
    amount: 1200,
    originalPurchaseAmount: 1200,
    date: '2026-02-05',
    purchaseDate: '2026-02-05',
    description: 'Laptop',
    installmentCount: 12
  });

  const installments = internals.buildInstallmentTransactions(charge, card);
  assert.equal(installments.length, 12);

  const keys = installments.map((item) => item.statementCycleKey);
  assert.equal(new Set(keys).size, 12, 'cada cuota debe caer en un cierre distinto');
  assert.equal(keys[0], '2026-02-10');
  assert.equal(keys[11], '2027-01-10');
  assert.equal(
    FinanceDB.roundAmount(installments.reduce((sum, item) => sum + item.amount, 0)),
    1200
  );
});

test('cada cuota se asigna al ciclo del sueldo que la cubre, sin apilarse en el ultimo', () => {
  const settings = makeSettings();
  const recurring = [makeSalaryRecurring()];
  const card = makeCard();

  const charge = tx({
    type: 'card_charge',
    cardId: card.id,
    amount: 1200,
    originalPurchaseAmount: 1200,
    date: '2026-02-05',
    purchaseDate: '2026-02-05',
    description: 'Laptop',
    installmentCount: 12
  });
  const installments = internals.buildInstallmentTransactions(charge, card);

  const materialized = internals.materializeRecurringTransactions(recurring, installments, settings);
  const cycles = internals.buildFinancialCycles(recurring[0], materialized, settings);
  const assigned = internals.assignBudgetCycles(materialized, [card], cycles);

  const assignedInstallments = assigned.filter((item) => item.type === 'card_charge');
  assert.equal(assignedInstallments.length, 12);

  const cyclesUsed = new Set(assignedInstallments.map((item) => item.budgetCycleId));
  assert.ok(!cyclesUsed.has(''), 'ninguna cuota debe quedar sin ciclo');
  assert.equal(cyclesUsed.size, 12, 'cada cuota va a un sueldo distinto');
});

test('un cargo sin ciclo disponible queda sin asignar en vez de ensuciar el ultimo ciclo', () => {
  const card = makeCard();
  const charge = tx({
    type: 'card_charge',
    cardId: card.id,
    amount: 500,
    date: '2026-06-05',
    purchaseDate: '2026-06-05',
    description: 'Compra futura',
    installmentCount: 1
  });

  const cycles = [
    internals.normalizeCycle({ id: 'cycle-a', startDate: '2026-01-15', endDate: '2026-02-14' }),
    internals.normalizeCycle({ id: 'cycle-b', startDate: '2026-02-15', endDate: '2026-03-14' })
  ];

  const [assigned] = internals.assignBudgetCycles([charge], [card], cycles);
  assert.equal(assigned.budgetCycleId, '');
});

test('el saldo de una deuda baja con cada pago registrado', () => {
  const debt = internals.normalizeDebt({
    id: 'debt-1',
    name: 'Prestamo',
    totalAmount: 3000,
    outstandingAmount: 1000,
    minimumPayment: 200,
    installmentCount: 5,
    installmentsPaid: 0
  });

  const payments = [
    tx({ type: 'debt_payment', amount: 200, date: '2026-02-01', fromAccountId: 'bank-main', linkedEntityType: 'debt', linkedEntityId: 'debt-1', description: 'Cuota 1' }),
    tx({ type: 'debt_payment', amount: 200, date: '2026-03-01', fromAccountId: 'bank-main', linkedEntityType: 'debt', linkedEntityId: 'debt-1', description: 'Cuota 2' })
  ];

  const [derived] = internals.deriveDebtProgress([debt], payments);
  assert.equal(derived.outstandingAmount, 600);
  assert.equal(derived.paidAmount, 400);
  assert.equal(derived.installmentsPaid, 2);
  assert.equal(derived.settled, false);
  assert.equal(derived.lastPaymentDate, '2026-03-01');
});

test('una deuda pagada por completo queda en cero y marcada como saldada', () => {
  const debt = internals.normalizeDebt({ id: 'debt-2', name: 'Tele', outstandingAmount: 500, totalAmount: 500 });
  const payments = [
    tx({ type: 'debt_payment', amount: 500, date: '2026-02-01', fromAccountId: 'bank-main', linkedEntityType: 'debt', linkedEntityId: 'debt-2', description: 'Pago total' })
  ];

  const [derived] = internals.deriveDebtProgress([debt], payments);
  assert.equal(derived.outstandingAmount, 0);
  assert.equal(derived.settled, true);
});

test('los aportes suben el avance de la meta', () => {
  const goal = internals.normalizeGoal({ id: 'goal-1', name: 'Viaje', targetAmount: 1000, currentAmount: 100 });
  const contributions = [
    tx({ type: 'goal_contribution', amount: 250, date: '2026-02-01', fromAccountId: 'bank-main', toAccountId: 'savings-main', linkedEntityType: 'goal', linkedEntityId: 'goal-1', description: 'Aporte' }),
    tx({ type: 'goal_contribution', amount: 650, date: '2026-03-01', fromAccountId: 'bank-main', toAccountId: 'savings-main', linkedEntityType: 'goal', linkedEntityId: 'goal-1', description: 'Aporte' })
  ];

  const [derived] = internals.deriveGoalProgress([goal], contributions);
  assert.equal(derived.currentAmount, 1000);
  assert.equal(derived.contributedAmount, 900);
  assert.equal(derived.completed, true);
});

test('pagar de mas una tarjeta deja saldo a favor y no se pierde', () => {
  const card = makeCard();
  const transactions = [
    tx({ type: 'card_charge', cardId: card.id, amount: 300, date: '2026-02-05', purchaseDate: '2026-02-05', statementCycleKey: '2026-02-10', description: 'Compra', installmentCount: 1 }),
    tx({ type: 'card_payment', cardId: card.id, amount: 500, date: '2026-02-20', fromAccountId: 'bank-main', description: 'Pago' })
  ];

  const { statements, credits } = internals.buildStatementLedger([card], transactions, []);
  const statement = statements.find((item) => item.statementCycleKey === '2026-02-10');

  assert.equal(statement.pendingAmount, 0);
  assert.equal(credits.get(card.id), 200, 'los 200 de mas quedan como saldo a favor');
});

test('el saldo a favor se aplica a un cargo posterior', () => {
  const card = makeCard();
  const transactions = [
    tx({ type: 'card_charge', cardId: card.id, amount: 300, date: '2026-02-05', purchaseDate: '2026-02-05', statementCycleKey: '2026-02-10', description: 'Compra 1', installmentCount: 1 }),
    tx({ type: 'card_payment', cardId: card.id, amount: 500, date: '2026-02-20', fromAccountId: 'bank-main', description: 'Pago' }),
    tx({ type: 'card_charge', cardId: card.id, amount: 120, date: '2026-03-05', purchaseDate: '2026-03-05', statementCycleKey: '2026-03-10', description: 'Compra 2', installmentCount: 1 })
  ];

  const { statements, credits } = internals.buildStatementLedger([card], transactions, []);
  const second = statements.find((item) => item.statementCycleKey === '2026-03-10');

  assert.equal(second.pendingAmount, 0, 'el cargo nuevo se cubre con el saldo a favor');
  assert.equal(credits.get(card.id), 80);
});

test('una ocurrencia de recurrente editada a mano no se regenera', () => {
  const settings = makeSettings();
  const recurring = [makeSalaryRecurring()];
  const key = 'rec-salary@2026-02-15';

  const edited = tx({
    id: 'tx-editada',
    type: 'income',
    amount: 4500,
    date: '2026-02-15',
    description: 'Sueldo con bono',
    toAccountId: 'bank-main',
    linkedEntityType: 'recurring',
    linkedEntityId: 'rec-salary',
    recurringOccurrenceKey: key,
    manualOverride: true,
    autoGenerated: true
  });

  const result = internals.materializeRecurringTransactions(recurring, [edited], settings);
  const found = result.find((item) => item.recurringOccurrenceKey === key);

  assert.equal(found.amount, 4500, 'el monto editado se conserva');
  assert.equal(found.description, 'Sueldo con bono');
});

test('una ocurrencia no editada si se regenera desde la plantilla', () => {
  const settings = makeSettings();
  const recurring = [makeSalaryRecurring()];
  const key = 'rec-salary@2026-02-15';

  const stale = tx({
    id: 'tx-vieja',
    type: 'income',
    amount: 1,
    date: '2026-02-15',
    description: 'Monto viejo',
    toAccountId: 'bank-main',
    linkedEntityType: 'recurring',
    linkedEntityId: 'rec-salary',
    recurringOccurrenceKey: key,
    autoGenerated: true
  });

  const result = internals.materializeRecurringTransactions(recurring, [stale], settings);
  const found = result.find((item) => item.recurringOccurrenceKey === key);

  assert.equal(found.amount, 4000);
  assert.equal(found.description, 'Sueldo');
});

test('los saldos por cuenta respetan el corte de fecha', () => {
  const accounts = makeAccounts();
  const transactions = [
    tx({ type: 'income', amount: 1000, date: '2026-02-01', toAccountId: 'bank-main', description: 'Sueldo' }),
    tx({ type: 'expense', amount: 250, date: '2026-02-05', fromAccountId: 'bank-main', description: 'Mercado' }),
    tx({ type: 'transfer', amount: 300, date: '2026-02-08', fromAccountId: 'bank-main', toAccountId: 'savings-main', description: 'Ahorro' })
  ];

  const balances = internals.calculateBalances(accounts, transactions, '2026-02-09');
  assert.equal(balances['bank-main'], 450);
  assert.equal(balances['savings-main'], 300);

  const partial = internals.calculateBalances(accounts, transactions, '2026-02-05');
  assert.equal(partial['bank-main'], 1000, 'el gasto del dia de corte no entra');
});

test('el barrido en modo suggest propone pero no inventa el movimiento', () => {
  const settings = makeSettings({ financialCycleConfig: { savingsSweepMode: 'suggest' } });
  const accounts = makeAccounts();
  const cycles = [
    internals.normalizeCycle({
      id: 'cycle-1',
      startDate: '2026-01-15',
      endDate: '2026-02-14',
      status: 'closed',
      liquidAccountIds: ['cash-main', 'bank-main']
    })
  ];
  const transactions = [
    tx({ type: 'income', amount: 4000, date: '2026-01-15', toAccountId: 'bank-main', description: 'Sueldo', budgetCycleId: 'cycle-1' }),
    tx({ type: 'expense', amount: 1500, date: '2026-01-20', fromAccountId: 'bank-main', description: 'Gastos', budgetCycleId: 'cycle-1' })
  ];

  const result = internals.buildCyclesWithSweeps(cycles, transactions, accounts, [], [], settings);
  const cycle = result.cycles[0];

  assert.equal(cycle.suggestedSweepAmount, 2500);
  assert.equal(cycle.sweepTransferId, '');
  assert.equal(result.sweepTransactions.length, 0, 'no se crea ninguna transferencia fantasma');
  assert.equal(result.transactions.length, 2);
});

test('el barrido en modo auto si crea la transferencia', () => {
  const settings = makeSettings({ financialCycleConfig: { savingsSweepMode: 'auto' } });
  const accounts = makeAccounts();
  const cycles = [
    internals.normalizeCycle({
      id: 'cycle-1',
      startDate: '2026-01-15',
      endDate: '2026-02-14',
      status: 'closed',
      liquidAccountIds: ['cash-main', 'bank-main']
    })
  ];
  const transactions = [
    tx({ type: 'income', amount: 4000, date: '2026-01-15', toAccountId: 'bank-main', description: 'Sueldo', budgetCycleId: 'cycle-1' }),
    tx({ type: 'expense', amount: 1500, date: '2026-01-20', fromAccountId: 'bank-main', description: 'Gastos', budgetCycleId: 'cycle-1' })
  ];

  const result = internals.buildCyclesWithSweeps(cycles, transactions, accounts, [], [], settings);
  assert.equal(result.sweepTransactions.length, 1);
  assert.equal(result.cycles[0].sweptAmount, 2500);
});

test('un ciclo abierto tambien reporta su libre neto', () => {
  const settings = makeSettings();
  const accounts = makeAccounts();
  const cycles = [
    internals.normalizeCycle({
      id: 'cycle-open',
      startDate: FinanceDB.addDays(FinanceDB.getToday(), -5),
      endDate: FinanceDB.addDays(FinanceDB.getToday(), 20),
      status: 'open',
      liquidAccountIds: ['cash-main', 'bank-main']
    })
  ];
  const transactions = [
    tx({ type: 'income', amount: 3000, date: FinanceDB.addDays(FinanceDB.getToday(), -5), toAccountId: 'bank-main', description: 'Sueldo', budgetCycleId: 'cycle-open' })
  ];

  const result = internals.buildCyclesWithSweeps(cycles, transactions, accounts, [], [], settings);
  assert.equal(result.cycles[0].freeNetAmount, 3000);
  assert.equal(result.cycles[0].suggestedSweepAmount, 0, 'un ciclo abierto no propone barrido todavia');
});

test('normalizeCycle conserva las metricas calculadas del ciclo', () => {
  const cycle = internals.normalizeCycle({
    id: 'cycle-1',
    startDate: '2026-01-15',
    endDate: '2026-02-14',
    freeNetAmount: 1234.56,
    sweptAmount: 200,
    suggestedSweepAmount: 300,
    pendingCardAmount: 50,
    pendingDebtAmount: 25
  });

  assert.equal(cycle.freeNetAmount, 1234.56);
  assert.equal(cycle.sweptAmount, 200);
  assert.equal(cycle.suggestedSweepAmount, 300);
  assert.equal(cycle.pendingCardAmount, 50);
  assert.equal(cycle.pendingDebtAmount, 25);
});

test('el presupuesto suma gasto en efectivo y cargos de tarjeta del ciclo', () => {
  const settings = makeSettings({ monthlyBudget: 2000 });
  const cycle = { id: 'cycle-1' };
  const transactions = [
    tx({ type: 'expense', amount: 800, date: '2026-02-01', fromAccountId: 'bank-main', description: 'Mercado', budgetCycleId: 'cycle-1' }),
    tx({ type: 'debt_payment', amount: 200, date: '2026-02-02', fromAccountId: 'bank-main', linkedEntityType: 'debt', linkedEntityId: 'd1', description: 'Cuota', budgetCycleId: 'cycle-1' }),
    tx({ type: 'expense', amount: 999, date: '2026-03-01', fromAccountId: 'bank-main', description: 'Otro ciclo', budgetCycleId: 'cycle-2' })
  ];
  const statements = [
    { budgetCycleId: 'cycle-1', chargedAmount: 600, pendingAmount: 600 },
    { budgetCycleId: 'cycle-2', chargedAmount: 400, pendingAmount: 400 }
  ];

  const budget = internals.buildBudgetStatus(settings, cycle, transactions, statements);
  assert.equal(budget.enabled, true);
  assert.equal(budget.spent, 1600);
  assert.equal(budget.remaining, 400);
  assert.equal(budget.overspent, false);

  const tight = internals.buildBudgetStatus(makeSettings({ monthlyBudget: 1000 }), cycle, transactions, statements);
  assert.equal(tight.overspent, true);
  assert.equal(tight.ratio, 1);
});

test('sin tope configurado el presupuesto queda desactivado', () => {
  const budget = internals.buildBudgetStatus(makeSettings({ monthlyBudget: 0 }), { id: 'c' }, [], []);
  assert.equal(budget.enabled, false);
});

test('la proyeccion de recurrentes alcanza a cubrir las cuotas mas lejanas', () => {
  const recurring = [makeSalaryRecurring()];
  const today = FinanceDB.getToday();
  const lejano = FinanceDB.addMonths(today, 18, 10);

  const bounds = internals.getRecurringProjectionBounds(recurring, [
    tx({ type: 'card_charge', cardId: 'card-1', amount: 100, date: today, purchaseDate: today, statementCycleKey: lejano, description: 'Cuota lejana', installmentCount: 1 })
  ]);

  assert.ok(
    FinanceDB.compareDate(bounds.endDate, lejano) >= 0,
    'la proyeccion debe llegar al menos hasta la ultima cuota'
  );
});
