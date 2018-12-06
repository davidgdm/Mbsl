SELECT
la.id AS loan_account_id,
la.loan_portfolio_id,
la.currency,
la.handover_at,
lpl.installment_period_days,
lpl.installment_periods,
la.installment_subunit/100 AS installment,
lpl.down_payment_subunit/100,
lpl.loan_subunit/100 AS total_amount_loan,
lpl.down_payment_subunit/lpl.loan_subunit as Porc_DP
#la.balance_subunit/100 AS repayment_balance,
#(lplan.loan_subunit/100 - la.balance_subunit/100) AS total_outstanding,
FROM loan_accounts la
INNER JOIN loan_plans lpl ON lpl.id = la.loan_plan_id
