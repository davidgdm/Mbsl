import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pyodbc
import time
import pymssql
import os

#------------------------EXTRACTION---------------------------------
os.system("sudo systemctl restart db-ssh")
print("service restarted")
time.sleep(2)
print("2 sec waited")
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')
print('connection established')
#extraction of daily snapshot
df_dailysnapshot = pd.read_sql_query("""SELECT
la.id as Loan_Account_Id,
#'1st' as PaymentDefault,
 c.name,
la.risk_category,
la.state,
lpl.currency,
u.name AS lfo_name,
la.handover_at,
(lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    a.area1,
    a.area2,
    a.area3,
    a.area4,
    c.phone_number,
    c.second_phone_number,
    c.third_phone_number,
    CONCAT(con.name, ' ', con.surname) AS Sales_Agent,
    resultant_loan_account_id,
    case when rr.resultant_loan_account_id is not null then 1 else 0 End as Reschedule,
    CURDATE() as snapshot_at
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
   LEFT JOIN customer_forms cf ON cf.customer_id = lp.customer_id  AND cf.form_name = 'application'
   LEFT JOIN contractors con ON con.id = cf.contractor_id
   left join loan_reschedule_requests rr on rr.resultant_loan_account_id=la.id
   where month(la.handover_at)>=month(date_add(CURDATE(),  INTERVAL -5 month))
     and year(la.handover_at)>=year(date_add(CURDATE(),  INTERVAL -5 month))
    and cf.form_name = 'application'
    and la.risk_category not like null
GROUP BY la.id
order by la.handover_at;""", con=conSolar)
print ('df created')

df_dailysnapshot.to_csv('/home/dwh/ETL/mbsl/Code/daily-pd.csv', index=False)
#df_dailysnapshot.to_csv('C:/testETL/daily.csv', index=False)
print('csv saved')

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')
cursor = cnxn_dev.cursor()
querystring ="""truncate table Extr_Paydefault_SA;"""
cursor.execute(querystring)
cnxn_dev.commit()
print('truncate Extr_Paydefault_SA finished')

os.system("""/opt/mssql-tools/bin/bcp Extr_Paydefault_SA in "/home/dwh/ETL/mbsl/Code/daily-pd.csv" -S mbslbiserver.database.windows.net -d mbsldwh -U Reports -P mbsl1234! -q -c -t ,""")
