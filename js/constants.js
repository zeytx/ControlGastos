/* Etiquetas y nombres fijos de la interfaz. */

export const MONTH_NAMES = [
  'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
  'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
];

export const SHORT_MONTH_NAMES = [
  'Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun',
  'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic'
];

export const TYPE_LABELS = {
  expense: 'Gasto',
  income: 'Ingreso',
  transfer: 'Transferencia',
  card_charge: 'Compra con tarjeta',
  card_payment: 'Pago de tarjeta',
  debt_payment: 'Pago de deuda',
  goal_contribution: 'Aporte a meta'
};

export const TYPE_ICONS = {
  expense: 'v',
  income: '^',
  transfer: '<>',
  card_charge: 'TC',
  card_payment: 'Pago',
  debt_payment: 'Deuda',
  goal_contribution: 'Meta'
};

export const ACCOUNT_KIND_LABELS = {
  cash: 'Efectivo',
  bank: 'Banco',
  savings: 'Ahorros'
};

export const DEBT_KIND_LABELS = {
  loan: 'Prestamo',
  installment: 'Cuotas',
  personal: 'Personal'
};
