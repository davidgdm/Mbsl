select left(hubs.hub_name,2) as Country,lwc.id as LWCid,u.name as 'Action_by',c.id as CustomerID,lp.id as LoanPortfolioID,pa.id as Payment_AccountId ,lwc.closed_at, 
(sum(if (p.created_at between lwc.closed_at - interval 0.5 day and lwc.closed_at + interval 5 day,p.amount_subunit,null))/100) as "payments within 5 days",
LPsub.nominal_installment,
(lwc.la_days_late/30.5) * LPsub.nominal_installment as 'Negative Balance at time of LWC'

from loan_workout_cases lwc
inner join customers c on c.id=lwc.customer_id
inner join loan_portfolios lp on lp.customer_id=c.id
inner join payment_accounts pa on pa.loan_portfolio_id=lp.id
inner join
(select 
sum(la.installment_subunit)/100 as 'Nominal_Installment',
sum(la.repayment_balance_subunit)/100 as 'Negative_Balance',
sum(la.balance_subunit)/100 as 'Total_Payments', 
lp.id as 'LPid'
from loan_portfolios lp
left join loan_accounts la on la.loan_portfolio_id=lp.id
where la.state not in ("paid off","potentially paid offf","defaulted")
group by lp.id) LPsub
on lp.id=LPsub.LPid
left outer join payments p on p.payment_account_id=pa.id
inner join users u on lwc.user_id = u.id
inner join hubs on u.hub_id=hubs.id
where lwc.could_reach_customer=1 and lwc.activity_category_id="5253" and lwc.closed_at > "2018-03-01" and p.created_at > "2018-03-01"
group by lwc.id


