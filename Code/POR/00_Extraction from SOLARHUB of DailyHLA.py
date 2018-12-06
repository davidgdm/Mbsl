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
GROUP BY la.id limit 10;""", con=conSolar)

#new_date=df_hladaily.snapshot_at.max().strftime("%Y-%m-%d")
new_date=df_hladaily.snapshot_at.max()
#print(new_date)

#MSSQL-CF Connection
connMySQL = r'mysql+pymysql://cf:Vision2020@10.100.25.13/CF_Portfolio'
engineMy = create_engine(connMySQL)

last_date=pd.read_sql_query("""SELECT max(snapshot_at) as snapshot_at from hla_today;""", con=connMySQL)
date=last_date.snapshot_at.max().strftime("%Y-%m-%d")
print(last_date)

#if last_date == new_date:
engineMy.execute('"DELETE from hla_today where snapshot_at >= (:date_1) ', date_1=new_date)
    
#df_hladaily.to_sql('hla_today',engineMy,if_exists='append')

#Still to create the algorithm to import collect historical data, to save all "hla_today" into an historical table