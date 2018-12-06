import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine
import pymssql

connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

sqlquery="""
use mbsol

update Con_migration
set Bucket0='PIGP'
where bucket0 like 'PIGP%'

go
update Con_migration
set Bucket1='PIGP'
where bucket1 like 'PIGP%'

go
if object_id('Migration_Agg') is not null drop table Migration_Agg

go
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
	  ,case when [Bucket1] in ('PaR1','PAR15','PaR30','PAR60') then sum([Bal1]) else 0 end as [Bal>1]
	  ,sum(bal1) as Balance
	  into Migration_Agg
  FROM [Mbsol].[dbo].Con_migration
  where [state0] not in('paid_off', 'defaulted', 'repossess_assets', 'canceled')
  --and bucket1 like 'par%'
  group by [Bucket1],currency, [snapshot_at], [lfo_name]
      ,[Bucket0]
      ,[MonthEnd]

go
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

if object_id('temp_supervisor_lfo') is not null drop table temp_supervisor_lfo
go
create table temp_supervisor_lfo(
	[Supervisor] [varchar](256) NULL,
	[lfo_name] [varchar](256) NULL
) 
go
insert into temp_supervisor_lfo values('John Gikunda','Dennis Odhiambo')
insert into temp_supervisor_lfo values('John Gikunda','Festus Mitei')
insert into temp_supervisor_lfo values('John Gikunda','Clinton Olonyi')
insert into temp_supervisor_lfo values('John Gikunda','Paul Ikapel')
insert into temp_supervisor_lfo values('John Gikunda','Stanley Otiende')
insert into temp_supervisor_lfo values('John Gikunda','Wilson Kibet')
insert into temp_supervisor_lfo values('John Gikunda','Xavier Omodia')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Albert Rutayisire')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Albert Urimube')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Alphonse Nsengiyumva')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Augustin Gasana')
insert into temp_supervisor_lfo values('Jean Safari','Christophe Ngirinshuti')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Cleophas Ntakije')
insert into temp_supervisor_lfo values('Jean Safari','Darius Kamanzi')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Eugene Murenzi')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Fabien Bimenyimana')
insert into temp_supervisor_lfo values('Alphonse Batumanyeho','Janvier Kubwayo')
insert into temp_supervisor_lfo values('Jean Safari','Laurien Mugwaneza')
insert into temp_supervisor_lfo values('Jean Safari','Paul Mufata')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Abubakar Ahmed')
insert into temp_supervisor_lfo values('Baltazar Mngongo','Ahimidiwe Mbilinyi')
insert into temp_supervisor_lfo values('Yasin Msangi','Albert Mgimwa')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Ally Kafuku')
insert into temp_supervisor_lfo values('Eliezer Kefa','Anna Kassuga')
insert into temp_supervisor_lfo values('Eliezer Kefa','Antidus Mwangwa')
insert into temp_supervisor_lfo values('Yusuph Goah','Arthur Mangombe')
insert into temp_supervisor_lfo values('Yusuph Goah','Baraka Lyimo')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Baraka Mbesere')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Benedicto Msimamo')
insert into temp_supervisor_lfo values('Ally Kitundu','Berton Lushwela')
insert into temp_supervisor_lfo values('Yasin Msangi','Bonus Vasolela')
insert into temp_supervisor_lfo values('Eliezer Kefa','Bosco Mtuy')
insert into temp_supervisor_lfo values('Yasin Msangi','Calystus Nyoni')
insert into temp_supervisor_lfo values('Ally Kitundu','Christopher Reginald')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Cleophas Ntakije')
insert into temp_supervisor_lfo values('Yusuph Goah','Daniel Elirehema')
insert into temp_supervisor_lfo values('Baltazar Mngongo','David Maro')
insert into temp_supervisor_lfo values('Baltazar Mngongo','Dionece Haule')
insert into temp_supervisor_lfo values('Yasin Msangi','Dominick Einhard')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Edgar Kadilikansimba')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Emmanuel Luoga')
insert into temp_supervisor_lfo values('Eliezer Kefa','Emmanuel Njau')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Erick Tongora')
insert into temp_supervisor_lfo values('Baltazar Mngongo','Exaud Wimmo')
insert into temp_supervisor_lfo values('Yasin Msangi','Fadhili Hassan')
insert into temp_supervisor_lfo values('Yasin Msangi','Festo Ignas')
insert into temp_supervisor_lfo values('Ally Kitundu','Gerald Mcha')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Gipson Mawalla')
insert into temp_supervisor_lfo values('Yusuph Goah','Hafidhu Shabani')
insert into temp_supervisor_lfo values('Ally Kitundu','Haruni Nakatwanga')
insert into temp_supervisor_lfo values('Ally Kitundu','Honest Silayo')
insert into temp_supervisor_lfo values('Baltazar Mngongo','Japhet Kisinga')
insert into temp_supervisor_lfo values('Eliezer Kefa','Japhet Ngitoria')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Jerry Ngilangwa')
insert into temp_supervisor_lfo values('Yusuph Goah','Jonathan Nicholaus')
insert into temp_supervisor_lfo values('Eliezer Kefa','Ludovick Kilowoko')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Macdonald Senyangwa')
insert into temp_supervisor_lfo values('Eliezer Kefa','Martine Magabe')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Matthew Masanja')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Msham Mgao')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Nsajigwa Kaisi')
insert into temp_supervisor_lfo values('Baltazar Mngongo','Paul Nyagalu')
insert into temp_supervisor_lfo values('Yusuph Goah','Philip Munishi')
insert into temp_supervisor_lfo values('Yusuph Goah','Phineas Kauswa')
insert into temp_supervisor_lfo values('Yusuph Goah','Prosper Mfoi')
insert into temp_supervisor_lfo values('Not Label','Rehema Mgonja')
insert into temp_supervisor_lfo values('Yasin Msangi','Richard Lyapembile')
insert into temp_supervisor_lfo values('Emmanuel Mwingira','Salehe Ngonja')
insert into temp_supervisor_lfo values('Fadhili Lekajo','Salvanio Selestin')
insert into temp_supervisor_lfo values('Eliezer Kefa','Simon Nyambuka')
insert into temp_supervisor_lfo values('Ally Kitundu','Simphone Tairo')
insert into temp_supervisor_lfo values('Ally Kitundu','Siprian Mwanga')
insert into temp_supervisor_lfo values('Eliezer Kefa','Yassin Swai')

go
if object_id('supervisor_lfo') is not null drop table supervisor_lfo
go

select lfo_name 
into supervisor_lfo 
from [Migration_Agg] group by lfo_name

ALTER TABLE supervisor_lfo 
ADD lfo_id INT IDENTITY,
[Supervisor] varchar(256) null
go
update supervisor_lfo set [Supervisor]=t.supervisor from temp_supervisor_lfo t where t.lfo_name=supervisor_lfo.lfo_name
go
update supervisor_lfo set [Supervisor]='Other' where Supervisor is null

if object_id('temp_supervisor_lfo') is not null drop table temp_supervisor_lfo
go
alter table migration_agg
ADD Supervisor varchar(128) null
go
update migration_agg set Supervisor=s.supervisor from supervisor_lfo s where s.lfo_name=migration_agg.lfo_name
    """

with engineMS.connect() as con:
    rs = con.execute(sqlquery)