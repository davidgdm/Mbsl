/****** Script for SelectTopNRows command from SSMS  ******/
/*
delete Extr_payments
where [transaction_at]<'2018-11-01'

delete Extr_payments
where [loan_portfolio_id] not in (select [LoanPortfolioID] from [Extr_workcases] group by LoanPortfolioID)

*/
alter table extr_payments
add [LWCiD] bigint null

--update extr_payments set LWCiD=NULL

update extr_payments
set [LWCiD]=(select max(wc.[LWCiD]) from [Extr_workcases] wc 
where (wc.closed_at <= extr_payments.[transaction_at]
and wc.closed_at >= dateadd(day,-5,  extr_payments.[transaction_at]))
and wc.[LoanPortfolioID] =extr_payments.[loan_portfolio_id])

select top 100 * from extr_payments
where lwcid is not null
where loabn