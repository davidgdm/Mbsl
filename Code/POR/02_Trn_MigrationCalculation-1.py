import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine
import pymssql

connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

df_daily = pd.read_sql_query("""SELECT 
    loan_account_id,
    risk_category as Bucket1,
    snapshot_at,
    state,
    outstanding_amount as Bal1
FROM Con_HLA_Today
where snapshot_at='2018-07-22' """, con=engineMS)
#Extr_Hla_Today
#state=['paid_off', 'defaulted', 'repossess_assets','rescheduled']
#state=['paid_off', 'defaulted', 'repossess_assets', 'canceled']
#to exclude also handover dt during the  month
#df_daily=df_daily[(~df_daily.state.isin(state))]

#TodaysDT=df_daily[df_daily['snapshot_at'].notna()]

df_month = pd.read_sql_query("""SELECT 
    loan_account_id,
    risk_category as Bucket0,
    snapshot_at as MonthEnd,
    outstanding_amount as Bal0,
    lfo_name,
    currency
FROM
    HLA_Monthly;""", con=engineMS)

#state=['paid_off', 'defaulted', 'canceled','repossess_assets','rescheduled']
#state=['paid_off', 'defaulted', 'repossess_assets', 'canceled']

#df_month=df_month[(~df_month.state.isin(state))]

#Select only the columns to be used
#df_month=df_month['loan_account_id','Bucket0', 'MonthEnd', 'Bal0',  'lfo_name', 'currency']

df_month=df_month.merge(df_daily, left_on='loan_account_id', right_on='loan_account_id', how='left')

df_month.to_sql('migrationdaily',engineMS,if_exists='replace')

with engineMS.connect() as con:
    rs = con.execute("""  update [migrationdaily] set [snapshot_at] =(select max([snapshot_at]) from [migrationdaily] where [snapshot_at] is not null) 
  where [snapshot_at] is null""")


with engineMS.connect() as con:
    rs = con.execute("""delete from Con_migration where snapshot_at >= (select max(snapshot_at) from [migrationdaily])
insert into Con_migration select * from [migrationdaily]""")
