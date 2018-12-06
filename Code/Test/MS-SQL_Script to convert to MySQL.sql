Use Mbsol

if object_id('dailyPortfolio') is not null drop table DailyPortfolio 
go

declare @query as nvarchar(max)
declare @Select as nvarchar(max)
declare @folder as nvarchar(500)
declare @filename as nvarchar(500)
declare @table as nvarchar(500)

set @folder='C:\Users\gmartinez\Documents\Docs\SpecialReports\Daily Reports'   
set @Select='  *'
set @filename='portfolio_overview_export_TZ20180705-7874-8n6vv6.csv'
set @table='DailyPortfolio'

set @query='
select * into '+@table+' from openrowset(''MSDASQL'',
''Driver={Microsoft Access Text Driver (*.txt, *.csv)}; DBQ=' + @folder + ''',
'' select ' + @select + ' from "'+ @filename +'"'') T'
exec(@query)

------------------------------
if object_id('Wasfile_D') is not null drop table Wasfile_D 
go

select [loan_portfolio_id],customerId, [snapshot_at],[Bucket] As Del0,cast('-99' as varchar(8)) as Del1,[outstanding_amount] as Bal0, 0 as Bal1,  [LFO] as [Loan Officer], currency, Region as Branch1
into Wasfile_D
from portfolio
where nomonth=1
go

insert into WasFile_D 
select Loan_Account_Number ,customer_id, getdate(), '-99',risk_category, 0, total_outstanding, lfo_name, currency,area1 as Branch1 from DailyPortfolio where customer_id not in (select customerid  from WasFile_D) 
go


Alter Table wasFile_D add Range0 varchar(10),Range1 varchar(10), Range0In varchar(10), Range1In varchar(10)
go
Update WasFile_D set Bal1 = b.total_outstanding, del1 = b.risk_category from WasFile_D a, DailyPortfolio b where a.customerid = b.customer_id 
go
/*
Update WasFile_D set Range0= Del0
Update WasFile_D set Range0In= Del0
Update WasFile_D set Range1= Del1
Update WasFile_D set Range1In=Del1
*/

Update WasFile_D set Range0= 'New' where Del0 ='-99'
Update WasFile_D set Range0= 'Current' where (Del0 like 'PIGP%')
Update WasFile_D set Range0= '1-30' where (Del0= 'par1' OR Del0='par15')
Update WasFile_D set Range0= '31-60' where (Del0='par30')
Update WasFile_D set Range0= '61-90' where (Del0= 'par60')


Update WasFile_D set Range0In= 'New' where Del0 ='-99'
Update WasFile_D set Range0In= 'Current' where (Del0 like 'PIGP%')
Update WasFile_D set Range0In= '1-15' where  (Del0= 'par1')
Update WasFile_D set Range0In= '15-30' where (Del0= 'par15')
Update WasFile_D set Range0In= '31-60' where (Del0='par30')
Update WasFile_D set Range0In= '61-90' where (Del0='par60')


Update WasFile_D set Range1= 'Close' where Del1 ='-99'
Update WasFile_D set Range1= 'Current' where(Del1 like 'PIGP%')
Update WasFile_D set Range1= '1-30' where (Del1= 'par1' OR Del1='par15')
Update WasFile_D set Range1= '31-60' where (Del1='par30')
Update WasFile_D set Range1= '61-90' where(Del1='par60')


Update WasFile_D set Range1In= 'CLOSED' where Del1 ='-99'
Update WasFile_D set Range1In= 'Current' where (Del1 like 'PIGP%')
Update WasFile_D set Range1In= '1-15' where  (Del1= 'par1')
Update WasFile_D set Range1In= '15-30' where (Del1= 'par15')
Update WasFile_D set Range1In= '31-60' where (Del1='par30')
Update WasFile_D set Range1In= '61-90' where (Del1='par60')



If Object_ID('DailyIsWas') is not null
Drop table DailyIsWas

Select * into DailyIsWas from WasFile_D 
go

Drop table WasFile_D
go