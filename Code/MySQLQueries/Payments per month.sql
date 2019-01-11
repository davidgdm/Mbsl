SELECT pa.loan_portfolio_id, 
pa.customer_id, 
convert(p.transaction_at,date) as transaction_at, 
sum(p.amount_subunit)/100 as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>= '2018-06-1' and convert(transaction_at,date)< '2018-07-1'
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date)  