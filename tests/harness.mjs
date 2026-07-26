/* ============================================
   Carga db.js en Node exponiendo sus funciones
   internas, sin ensuciar el bundle del navegador.
   ============================================ */

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));

const INTERNALS = [
  'materializeRecurringTransactions',
  'buildFinancialCycles',
  'assignBudgetCycles',
  'buildCyclesWithSweeps',
  'buildStatementLedger',
  'buildCycleSummaries',
  'buildCardSummaries',
  'buildBudgetStatus',
  'buildAgendaItems',
  'deriveDebtProgress',
  'deriveGoalProgress',
  'calculateBalances',
  'splitInstallmentAmounts',
  'buildInstallmentTransactions',
  'getRecurringProjectionBounds',
  'normalizeSettings',
  'normalizeTransaction',
  'normalizeRecurring',
  'normalizeCard',
  'normalizeDebt',
  'normalizeGoal',
  'normalizeCycle',
  'toLocalISODate'
];

function loadEngine() {
  const source = readFileSync(join(here, '..', 'db.js'), 'utf8');
  const patched = source.replace(
    /\n  return \{\n    initDB,/,
    `\n  return {\n    __internals: { ${INTERNALS.join(', ')} },\n    initDB,`
  );

  if (patched === source) {
    throw new Error('No se pudo inyectar el hook de tests en db.js (cambio la firma del return).');
  }

  // db.js solo toca indexedDB dentro de funciones asincronas, asi que las
  // funciones puras se pueden ejercitar sin ningun shim de base de datos.
  (0, eval)(`${patched}\nglobalThis.__FinanceDB = FinanceDB;`);
  return globalThis.__FinanceDB;
}

export const FinanceDB = loadEngine();
export const internals = FinanceDB.__internals;

export function makeSettings(overrides = {}) {
  return internals.normalizeSettings({
    monthlyBudget: 0,
    financialCycleConfig: {
      primarySalaryRecurringId: 'rec-salary',
      savingsAccountId: 'savings-main',
      sweepSourceAccountId: 'bank-main',
      liquidAccountIds: ['cash-main', 'bank-main'],
      savingsSweepEnabled: true,
      ...(overrides.financialCycleConfig || {})
    },
    ...overrides
  });
}

export function makeAccounts() {
  return [
    { id: 'cash-main', name: 'Efectivo', kind: 'cash', openingBalance: 0, includeInNetWorth: true, archived: false },
    { id: 'bank-main', name: 'Banco', kind: 'bank', openingBalance: 0, includeInNetWorth: true, archived: false },
    { id: 'savings-main', name: 'Ahorros', kind: 'savings', openingBalance: 0, includeInNetWorth: true, archived: false }
  ];
}

export function makeSalaryRecurring(overrides = {}) {
  return internals.normalizeRecurring({
    id: 'rec-salary',
    type: 'income',
    amount: 4000,
    dayOfMonth: 15,
    startDate: '2026-01-15',
    description: 'Sueldo',
    categoryId: 'salary',
    accountId: 'bank-main',
    active: true,
    isPrimarySalary: true,
    opensFinancialCycle: true,
    ...overrides
  });
}

export function makeCard(overrides = {}) {
  return internals.normalizeCard({
    id: 'card-1',
    bankName: 'BCP',
    last4: '4321',
    closingDay: 10,
    dueDay: 25,
    paymentAccountId: 'bank-main',
    creditLimit: 10000,
    openingDebtAmount: 0,
    ...overrides
  });
}

export function tx(overrides = {}) {
  return internals.normalizeTransaction(overrides);
}
