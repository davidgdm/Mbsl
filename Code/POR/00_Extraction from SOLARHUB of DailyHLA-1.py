import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#Query extracting dayly HLAs data from SolarHUB
df_hladaily = pd.read_sql_query("""SELECT
la.id as Loan_Account_Id,
la.risk_category,
la.state,
lpl.currency,
u.name AS lfo_name,
LAST_DAY(la.handover_at) AS handover_at,
(CURDATE() - INTERVAL 1 day) AS snapshot_at, 
(lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount
FROM
loan_accounts la 
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
GROUP BY la.id;""", con=conSolar)

#SQL SERVER CONNECTION
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)
#To put the date into MS-SQL -> Extr_hla_today
df_hladaily.to_sql('Extr_hla_today',engineMS,if_exists='replace')

with engineMS.connect() as con:
    rs = con.execute("""delete from Con_HLA_Today where snapshot_at >= (select max(snapshot_at) from [Extr_hla_today])
insert into Con_HLA_Today select * from [Extr_hla_today]""")