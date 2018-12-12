import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pypyodbc
import pyodbc
import time
import pymssql

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of WC
df_dailysnapshot = pd.read_sql_query("""SELECT la.id as loan_account_id,
	la.loan_portfolio_id,
    la.currency,
    u.name AS lfo_name,
    la.state,
    la.risk_category,
    la.par_days,
    la.handover_at,
    (lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    date_add(CURDATE(),  INTERVAL -1 day) as snapshot_at
    FROM loan_accounts la 
        INNER JOIN 
    loan_plans lpl ON lpl.id = la.loan_plan_id
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
    
    WHERE la.risk_category IS NOT NULL
    AND la.state NOT IN('paid_off', 'defaulted', 'canceled')
    GROUP BY la.id
    ORDER BY la.id limit 10;""", con=conSolar)

df_dailysnapshot.to_csv('/home/dwh/ETL/mbsl/Code/Test/daily.csv')
#df_dailysnapshot.to_csv('c:/daily.csv')