import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine
import pymssql

connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

df_daily = pd.read_sql_query("""SELECT 
    loan_account_id,
    currency,
    risk_category as Bucket1,
    snapshot_at,
    state,
    outstanding_amount as Bal1,
    lfo_name
FROM
    Con_hla_today;""", con=engineMS)

#state=['paid_off', 'defaulted', 'repossess_assets','rescheduled']
state=['paid_off', 'defaulted', 'repossess_assets']
#to exclude also handover dt during the  month
df_daily=df_daily[(~df_daily.state.isin(state))]

#TodaysDT=df_daily[df_daily['snapshot_at'].notna()]

df_month = pd.read_sql_query("""SELECT 
    loan_account_id,
    risk_category as Bucket0,
    snapshot_at as MonthEnd,
    state,
    outstanding_amount as Bal0
FROM
    HLA_Monthly;""", con=engineMS)

#state=['paid_off', 'defaulted', 'canceled','repossess_assets','rescheduled']
state=['paid_off', 'defaulted', 'canceled','repossess_assets']

df_month=df_month[(~df_month.state.isin(state))]

#Select only the columns to be used
df_month=df_month[['loan_account_id','Bucket0', 'MonthEnd', 'Bal0']]

df_daily=df_daily.merge(df_month, left_on='loan_account_id', right_on='loan_account_id', how='left')

df_daily.to_sql('migrationdaily',engineMS,if_exists='replace')

#SQL SERVER CONNECTION
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

df_daily.to_sql('migrationdaily',engineMS,if_exists='replace')