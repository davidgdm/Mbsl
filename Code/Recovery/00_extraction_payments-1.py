import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib


conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of reschedule loan_portoflio_id
df_payments = pd.read_sql_query("""SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit)/100 as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>=2018-12-01
and  pa.loan_portfolio_id is not null
group by pa.loan_portfolio_id, convert(p.transaction_at,date);""", con=conSolar)

#temporal, to work with csv file
df_payments.to_csv('C:\Users\gmartinez\Documents\Docs\coding\Files\RecoveryFiles\payments_dec1-dec10.csv', index=False)


#AZURE SQL SERVER CONNECTION
#params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
#engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#df_payments.to_sql('Extr_payments',engineAzure,if_exists='replace',index=False)

