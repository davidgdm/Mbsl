--Refresh data using latest extraction
declare @last_dt date
declare @payment_min_dt date

set @last_dt =(select max([transaction_at]) from [Extr_payments] )
set @payment_min_dt=(select min([transaction_at]) from [Extr_payments] )

delete from t_payments_consolidated where [transaction_at]>=@payment_min_dt;
insert into t_payments_consolidated select * from [Extr_payments];

delete from t_wc_consolidated where [closed_at]>=(select min([closed_at]) from [Extr_workcases] );
insert into t_wc_consolidated select * from [Extr_workcases];
/*
truncate table [t_daily_wc];
insert into [t_daily_wc] ([Country],[LWCid],[Action_by],[CustomerID],[LoanPortfolioID],[closed_at],[activity_category_id])
select * from [Extr_workcases]
*/
truncate table [Trn_Recovery]
insert into [Trn_Recovery] ([loan_portfolio_id],[transaction_at],[payments within 5 days])
(select * from [Extr_payments] where loan_portfolio_id in 
(select [LoanPortfolioID]  from t_wc_consolidated where [closed_at]<=dateadd(day, -6, getdate()) group by [LoanPortfolioID]))

update trn_recovery
set [LWCiD]=(select max(wc.[LWCiD]) from [Extr_workcases] wc 
where (wc.closed_at <= trn_recovery.[transaction_at] and wc.closed_at >= dateadd(day,-5, trn_recovery.[transaction_at]))
and wc.[LoanPortfolioID] =trn_recovery.[loan_portfolio_id])

delete from trn_recovery where [LWCiD] is null

update trn_recovery
set country= wc.[Country],
	  [Action_by]=wc.[Action_by],
	  [CustomerID]=wc.[CustomerID],
      [closed_at]=wc.[closed_at],
      [activity_category_id]=wc.[activity_category_id]
	from [Extr_workcases] wc 
	where wc.[LWCiD]=trn_recovery.[LWCiD];

update trn_recovery set [activity_category]= 'call' where [activity_category_id]=5252
update trn_recovery set [activity_category]= 'visit' where [activity_category_id]=5253

delete from t_recovery_consolidated where [transaction_at]>=(select min([transaction_at]) from [Extr_payments] )--@payment_min_dt;
insert into t_recovery_consolidated select * from trn_recovery; 
