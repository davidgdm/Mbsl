import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import datetime
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
#conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
print('connection established')

#WC extraction last 3 days
df_workcases = pd.read_sql_query("""select left(hubs.hub_name,2) as Country,lwc.id as LWCid,u.name as 'Action_by',c.id as CustomerID, lp.id as LoanPortfolioID, lwc.closed_at, 
lwc.activity_category_id
from loan_workout_cases lwc
inner join customers c on c.id=lwc.customer_id
inner join loan_portfolios lp on lp.customer_id=c.id
inner join payment_accounts pa on pa.loan_portfolio_id=lp.id
inner join payments p on p.payment_account_id=pa.id
inner join users u on lwc.user_id = u.id
inner join hubs on c.hub_id=hubs.id
where lwc.could_reach_customer=1 and lwc.activity_category_id IN ("5252","5253") and lwc.closed_at >= ( CURDATE() - INTERVAL 3 DAY )
group by lwc.id;""", con=conSolar)

df_workcases.to_csv('/home/dwh/ETL/mbsl/Code/wc.csv', index=False)
print('csv WC saved')

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')
cursor = cnxn_dev.cursor()
querystring ="""truncate table Extr_workcases;"""
cursor.execute(querystring)
cnxn_dev.commit()
print('truncate Extr_workcases finished')
os.system("""bcp Extr_workcases in "/home/dwh/ETL/mbsl/Code/wc.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""")

#payments extraction 
os.system("sudo systemctl restart db-ssh")
print("service restarted")
time.sleep(2)
print("2 sec waited")
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')
print('payments MySQl query started')
df_payments = pd.read_sql_query("""SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit)/100 as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>=2018-08-01
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date);""", con=conSolar)

print('payments MySQl query finished')

df_payments.to_csv('/home/dwh/ETL/mbsl/Code/payments.csv', index=False)
print('csv payments saved')

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')
cursor = cnxn_dev.cursor()
querystring ="""truncate table Extr_payments;"""
cursor.execute(querystring)
cnxn_dev.commit()
print('truncate Extr_payments finished')

os.system("""bcp Extr_payments in "/home/dwh/ETL/mbsl/Code/payments.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""")