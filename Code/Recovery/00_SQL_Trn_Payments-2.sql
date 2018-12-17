
--Creates hitorical wc data--ONLY run the first time
--select * into t_wc_consolidated from [Extr_workcases]

delete from t_wc_consolidated where [closed_at]>=(select min([closed_at]) from [Extr_workcases] );
insert into t_wc_consolidated select * from [Extr_workcases];

delete from t_payments_consolidated where [transaction_at]>=(select min([transaction_at]) from [Extr_payments] );
insert into t_payments_consolidated select * from [Extr_payments];
/*
truncate table [t_daily_wc];
insert into [t_daily_wc] ([Country],[LWCid],[Action_by],[CustomerID],[LoanPortfolioID],[closed_at],[activity_category_id])
select * from [Extr_workcases]
*/
alter table extr_payments
add [LWCiD] bigint null

--update extr_payments set LWCiD=NULL

update extr_payments
set [LWCiD]=(select max(wc.[LWCiD]) from [Extr_workcases] wc 
where (wc.closed_at <= extr_payments.[transaction_at]
and wc.closed_at >= dateadd(day,-5,  extr_payments.[transaction_at]))
and wc.[LoanPortfolioID] =extr_payments.[loan_portfolio_id])
/*
select count(*)--top 100 * from extr_payments
from extr_payments
where lwcid is not null
*/

if object_id('[extr_recovery_calls]') is not null
	drop table [extr_recovery_calls]
go

select 
	p.[loan_portfolio_id]
      ,p.[transaction_at]
      ,p.[Payment_Amount] as 'payments within 5 days'
      ,wc.[LWCiD]
	  ,wc.[Country]
	  ,wc.[Action_by]
      ,wc.[CustomerID]
      ,wc.[closed_at]
      ,wc.[activity_category_id]
into [extr_recovery_calls]
from extr_payments p, [Extr_workcases] wc
where p.lwcid is not null
and p.LWCiD=wc.LWCid
and wc.[activity_category_id]=5252


if object_id('[extr_recovery_visits]') is not null
	drop table [extr_recovery_visits]
go
select 
	p.[loan_portfolio_id]
      ,p.[transaction_at]
      ,p.[Payment_Amount] as 'payments within 5 days'
      ,wc.[LWCiD]
	  ,wc.[Country]
	  ,wc.[Action_by]
      ,wc.[CustomerID]
      ,wc.[closed_at]
      ,wc.[activity_category_id]
	  into [extr_recovery_visits]
from extr_payments p, [Extr_workcases] wc
where p.lwcid is not null
and p.LWCiD=wc.LWCid
and wc.[activity_category_id]=5253
   