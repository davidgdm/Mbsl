import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pypyodbc
import pyodbc
#import ceODBC
import time
import pymssql

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of WC
df_workcases = pd.read_sql_query("""select left(hubs.hub_name,2) as Country,lwc.id as LWCid,u.name as 'Action_by',c.id as CustomerID, lp.id as LoanPortfolioID, lwc.closed_at, 
lwc.activity_category_id
from loan_workout_cases lwc
inner join customers c on c.id=lwc.customer_id
inner join loan_portfolios lp on lp.customer_id=c.id
inner join payment_accounts pa on pa.loan_portfolio_id=lp.id
inner join payments p on p.payment_account_id=pa.id
inner join users u on lwc.user_id = u.id
inner join hubs on c.hub_id=hubs.id
where lwc.could_reach_customer=1 and lwc.activity_category_id IN ("5252","5253") and lwc.closed_at >= "2018-12-01"
group by lwc.id;""", con=conSolar)

cnxn_dev =pymssql.connect(server='mbslbiserver.database.windows.net', user='Reports', password='mbsl1234!', database='mbsldwh_dev')

tuples = list(zip(df_workcases["Country"],df_workcases["LWCid"],df_workcases["Action_by"],df_workcases["CustomerID"],df_workcases["LoanPortfolioID"],df_workcases["closed_at"],df_workcases["activity_category_id"]))

cursor = cnxn_dev.cursor()
query="""Insert into Extr_workcases ([Country],[LWCid],[Action_by],[CustomerID],[LoanPortfolioID],[closed_at],[activity_category_id]) values(?,?,?,?,?,?,?)"""

start = time.time()

cursor.executemany(query,tuples)
cnxn_dev.commit()

end = time.time()
print(end - start)

cursor.close
cnxn_dev.close()
