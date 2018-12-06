import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine

connMySQL = r'mysql+pymysql://cf:Vision2020@10.100.25.13/CF_Portfolio'
engineMy = create_engine(connMySQL)

df_month = pd.read_sql_query("""SELECT 
    id AS loan_account_id,
    risk_category,
    state,
    currency,
    lfo_name,
    snapshot_at,
    outstanding_amount
FROM
    hla_monthly limit 10;""", con=engineMy)

state=['paid_off', 'defaulted', 'canceled','repossess_assets','rescheduled']
df_month=df_month[(~df_month.state.isin(state))]

df_daily = pd.read_sql_query("""SELECT 
    loan_account_id,
    risk_category,
    snapshot_at,
    state,
    outstanding_amount
FROM
    hla_today limit 10;""", con=engineMy)

df_daily=df_daily[(~df_daily.state.isin(state))]

#TodaysDT=df_daily[df_daily['snapshot_at'].notna()]

TodaysDT=df_daily.snapshot_at.max().notna()
print(TodaysDT)

df_migration=df_month.merge(df_daily, left_on='loan_account_id', right_on='loan_account_id', how='left')


df_migration.columns=['loan_account_id',
    'risk_category',
    'state',
    'currency',
    'lfo_name',
    'snapshot_at',
    'outstanding_amount',
    'risk_category1',
    'snapshot_at1',
    'state1',
    'outstanding_amount1']

#print(df_migration)
