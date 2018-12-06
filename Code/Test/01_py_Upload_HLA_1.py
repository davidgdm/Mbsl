import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine

df_csv=pd.read_csv(r'C:\Users\gmartinez\Documents\Docs\Monthly Portfolio\Data\06 Jun\par_report_la_2018_06_30.csv')

#MSSQL Connection
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

#Insert in MySQL hla_la active customers
#df_csv.query("state not in @state").to_sql('hla_la',engineMy,if_exists='replace')
max_dt=df_csv.snapshot_at.max()

#Taking LFO from solarhubs
conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

df_LFO = pd.read_sql_query("""SELECT 
    la.id AS Loan_Account_id,
    c.customer_number AS customer_number,
    u.name AS lfo_name
FROM
    loan_accounts la
        INNER JOIN
    loan_portfolios lp ON lp.id = la.loan_portfolio_id
        INNER JOIN
    customers c ON c.id = lp.customer_id
        INNER JOIN
    locations l ON l.id = c.location_id
        INNER JOIN
    location_areas a ON a.id = l.location_area_id
        INNER JOIN
    loan_manager_location_areas lmla ON lmla.location_area_id = a.id
        INNER JOIN
    users u ON u.id = lmla.user_id
GROUP BY la.id""", con=conSolar)

#Adding LFO's name to df_csv
df_csv=df_csv.merge(df_LFO, left_on='id', right_on='Loan_Account_id', how='left')

#Insert in MySQL daily HLA
#df_csv[(df_csv.snapshot_at==max_dt) & (~df_csv.state.isin(state))].to_sql('daily_la',engineMy,if_exists='replace'))

df_csv=df_csv[(df_csv.snapshot_at==max_dt)]

df_csv.to_sql('hla_monthly',engineMS,if_exists='replace')