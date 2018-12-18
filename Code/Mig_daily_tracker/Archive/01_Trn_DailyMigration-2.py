import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pyodbc 


#AZURE DWH SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh;UID=Reports;PWD=mbsl1234!")
engineDWH = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')

cursor = cnxn_dev.cursor()
#Performing all transformations
querystring ="""
    --to create dailyconsolidated table
    delete from t_daily_consolidated where snapshot_at=(select snapshot_at from [Extr_Dailysnapshot] group by snapshot_at);
    insert into t_daily_consolidated select * from [Extr_Dailysnapshot];
    
    declare @var_MonthEnd date
    declare @var_Last_snapshot date

    set @var_Last_snapshot = (select max(snapshot_at) from t_daily_consolidated)
    set @var_MonthEnd= EOMONTH ( @var_Last_snapshot,-1)
    --select @var_MonthEnd

    --delete from t_daily_iswas where snapshot_at=@var_Last_snapshot
    delete from t_daily_iswas 

    --inserts into t_daily_iswas the data from last month end
    insert into t_daily_iswas ([loan_account_id],[lfo_name],[currency],bucket0,[Monthend],bal0,[state0])
    select [loan_account_id], [lfo_name], Currency, case when risk_category like 'PIGP%' then 'PIGP' ELSE risk_category end as Bucket0, snapshot_at as Monthend, outstanding_amount as Bal0, [state] as state0
    from t_daily_consolidated 
    where snapshot_at= @var_MonthEnd
    --where snapshot_at= '2018-11-30'

    --update with latest snapshot
    update t_daily_iswas 
    set bucket1= case when d.risk_category like 'PIGP%' then 'PIGP' ELSE d.risk_category end, 
    bal1=d.[outstanding_amount], 
    state1=d.[state], 
    snapshot_at=d.snapshot_at 
    from [t_daily_consolidated] d 
    where d.snapshot_at=@var_Last_snapshot--'2018-12-06'
    and t_daily_iswas.loan_account_id =d.loan_account_id

    update t_daily_iswas set snapshot_at=@var_Last_snapshot where snapshot_at is null

    delete from t_daily_iswas_consolidated where snapshot_at=@var_Last_snapshot

    insert into t_daily_iswas_consolidated select * from t_daily_iswas

    --IT'S STILL NECESARY TO ADD THE NEW SALES PER DAY. FOR NOW IT'S NOT REQUIRED FOR MIGRATION BUT FOR FUTURE IS/WAS REPORT IT WILL BE NECESARY

    delete from Migration_Agg where snapshot_at =@var_Last_snapshot--'2018-12-06'

    insert into Migration_Agg 
    ([lfo_name]
        ,[Currency]
        ,[#]
        ,[Bucket0]
        ,[Bucket1]
        ,[snapshot_at]
        ,[MonthEnd]
        ,[Bal0]
        ,[Bal1]
        ,[Bal>30]
        ,[Bal>1]
        ,[Balance]
        ,[Bal1-30]
        ,[Bal30]
        ,[Bal0PaR1]
        ,[Bal1_1PIGP]
        ,[Bal0PaR30]
        ,[Bal1_30PIGP]
        ,[Bal0PaR60]
        ,[Bal1_60PIGP])

    SELECT [lfo_name], Currency
        ,count([loan_account_id]) as #
        ,[Bucket0]
        ,[Bucket1]
        ,[snapshot_at]
        ,[MonthEnd]
        ,case when [Bucket0] like 'PIGP%' then sum([Bal0]) else 0 end as Bal0
        ,case when ([Bucket0] like 'PIGP%' and [Bucket1] in ('PaR1','PAR15')) then sum([Bal1]) else 0 end as Bal1
        --,case when [Bucket1] in ('PaR1','PAR15') then sum([Bal1]) else 0 end as Bal1
        ,case when [Bucket1] in ('PaR30','PAR60') then sum([Bal1]) else 0 end as [Bal>30]
        ,case when [Bucket1] in ('PaR1','PAR15','PaR30','PAR60') then sum([Bal1]) else 0 end as [Bal>1]
        ,sum(bal1) as Balance
        ,case when [Bucket0] in ('PaR1','PAR15') then sum([Bal0]) else 0 end as [Bal1-30]
        ,case when ([Bucket1] in ('PaR30') and [Bucket0] in ('PaR1','PAR15')) then sum([Bal1]) else 0 end as [Bal30]
        ,case when [Bucket0]in ('PaR1','PAR15') then sum(bal0) else 0 end as	[Bal0PaR1]
        ,case when ([Bucket1] ='PIGP' and [Bucket0] in ('PaR1','PAR15')) then sum(bal1) else 0 end as [Bal1_1PIGP]
        ,case when [Bucket0]in ('PaR30') then sum(bal0) else 0 end as [Bal0PaR30]
        ,case when ([Bucket1] ='PIGP' and [Bucket0] in ('PaR30')) then sum(bal1) else 0 end as [Bal1_30PIGP]
        ,case when [Bucket0] in ('PaR60') then sum(bal0) else 0 end as [Bal0PaR60]
        ,case when ([Bucket1] ='PIGP' and [Bucket0] in ('PaR60')) then sum(bal1) else 0 end as [Bal1_60PIGP] 
    FROM t_daily_iswas 
    where [state0] not in('paid_off', 'defaulted', 'repossess_assets', 'canceled')
    and loan_account_id not in (select resultant_loan_account_id from [Extr_Reschedule])--reschedules
    and loan_account_id not in (SELECT  [loan_account_id] FROM [Extr_Cooperatives_rw])--rw cooperatives
    group by [lfo_name], currency, [Bucket0], [Bucket1], [snapshot_at], [MonthEnd]

    /*
    declare @var_MonthEnd date
    declare @var_Last_snapshot date

    set @var_Last_snapshot = (select max(snapshot_at) from t_daily_consolidated)
    set @var_MonthEnd= EOMONTH ( @var_Last_snapshot,-1)
    */
    update migration_agg set [Nb_Months]=datediff(month,[MonthEnd],@var_MonthEnd)
    update migration_agg set Supervisor=s.supervisor from supervisor_lfo s where s.lfo_name=migration_agg.lfo_name;
    """
cursor.execute(querystring)
cnxn_dev.commit()

#Uploading last day into DWH
df_Migration = pd.read_sql_query("""select * from Migration_agg where snapshot_at =(select max(snapshot_at)from Migration_agg)""", con=cnxn_dev)
df_Migration.to_sql('Migration_agg',engineDWH, if_exists='append',index=False)

