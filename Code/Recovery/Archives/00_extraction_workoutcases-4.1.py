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
#payments extraction 
os.system("sudo systemctl restart db-ssh")
print("service restarted")
time.sleep(2)
print("2 sec waited")
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')
print('payments MySQl query started')

df_payments = pd.read_sql_query("""SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit)/100 as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>=2018-12-12
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date);""", con=conSolar, chunksize=10000)

print('payments MySQl query finished')

df_payments.to_csv('/home/dwh/ETL/mbsl/Code/payments.csv', index=False, chunksize=1000)
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