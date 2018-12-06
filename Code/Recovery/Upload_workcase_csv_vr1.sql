use mbsol

if object_id('extr_workcases') is not null
	drop table temp_dec
go

declare @query as nvarchar(max)
declare @Select as nvarchar(max)
declare @folder as nvarchar(500)

set @folder='C:\Users\gmartinez\Documents\Docs\coding\Recovery\'
set @Select='  *'
/*

set @query=' insert into temp_nov_data ( [currency]
      ,[loan_account_id]
      ,[risk_category]
      ,[par_days]
      ,[handover_at]
      ,[total_outstanding]
      ,[lfo_name]
      ,[snapshot_at])select *
 from openrowset(
''MSDASQL'',
''Driver={Microsoft Access Text Driver (*.txt, *.csv)}; DBQ=' + @folder + ''',
'' select ' + @select + ' from "28_11_2018.csv"'') T'
--print (@query)
*/
set @query=' select * into extr_workcases
 from openrowset(
''MSDASQL'',
''Driver={Microsoft Access Text Driver (*.txt, *.csv)}; DBQ=' + @folder + ''',
'' select ' + @select + ' from "workcases november.csv"'') T'


exec(@query)

/*
update  temp_nov_data
set snapshot_at='2018-11-21'
*/