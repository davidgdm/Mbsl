SELECT
lpl.currency,
la.id AS loan_account_id,
sp.amount_subunit/100 AS payment_amount,
sp.created_at AS payment_at,
lpl.down_payment_subunit/100 AS down_payment
FROM loan_accounts la
INNER JOIN split_payments sp ON sp.loan_account_id = la.id
INNER JOIN loan_plans lpl ON lpl.id = la.loan_plan_id

The following query gets the loan_plan info:

SELECT
la.id AS loan_account_id,
lpl.currency,
la.handover_at,
lpl.installment_period_days,
lpl.installment_periods,
la.installment_subunit/100 AS installment
FROM loan_accounts la
INNER JOIN loan_plans lpl ON lpl.id = la.loan_plan_id; 