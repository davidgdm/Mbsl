SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit)/100 as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>='2018-12-12'
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date)

SELECT count(*)
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>='2018-12-12'
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date)