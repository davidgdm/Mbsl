use mbsol

update Con_migration
set Bucket0='PIGP'
where bucket0 like 'PIGP%'

update Con_migration
set Bucket1='PIGP'
where bucket1 like 'PIGP%'

if object_id('Migration_Agg') is not null drop table Migration_Agg

SELECT [lfo_name], Currency
      ,count([loan_account_id]) as #
	  ,[Bucket0]
      ,[Bucket1]
      ,[snapshot_at]
      --,sum([Bal1]) as Bal1
      ,[MonthEnd]
      ,case when [Bucket0] like 'PIGP%' then sum([Bal0]) else 0 end as Bal0
	  ,case when [Bucket1] in ('PaR1','PAR15') then sum([Bal1]) else 0 end as Bal1
	  --,sum(bal0) as Bal0
	  ,case when [Bucket1] in ('PaR30','PAR60') then sum([Bal1]) else 0 end as [Bal>30]
	  ,sum(bal1) as Balance
	  into Migration_Agg
  FROM [Mbsol].[dbo].Con_migration
  where [state0] not in('paid_off', 'defaulted', 'repossess_assets', 'canceled')
  --and bucket1 like 'par%'
  group by [Bucket1],currency, [snapshot_at], [lfo_name]
      ,[Bucket0]
      ,[MonthEnd]

if object_id('Migration_Target') is not null drop table Migration_Target

CREATE TABLE Migration_Target(
	[Currency] [varchar](max) NULL,
	[PIGPtoPARTarget] float NULL
) 
go
insert into Migration_Target values('tzs','1.75')
insert into Migration_Target values('rwf','2.8')
insert into Migration_Target values('kes','1.75')
go