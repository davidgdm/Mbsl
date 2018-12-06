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
df_payments = pd.read_sql_query("""SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit) as "Payment_Amount"
FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
where convert(transaction_at,date)>=2018-11-01
group by pa.loan_portfolio_id, convert(p.transaction_at,date) ;""", con=conSolar)


#AZURE SQL SERVER CONNECTION
#params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mbsol;UID=sa;PWD=Admin1234.,")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_payments.to_sql('Extr_payments',engineAzure,if_exists='replace',index=False)

