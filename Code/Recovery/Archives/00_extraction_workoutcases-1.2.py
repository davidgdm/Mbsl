import time
#import ceODBC
import pyodbc
 
import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pypyodbc
import pyodbc

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
group by lwc.id limit 10;""", con=conSolar)

dsn=""" 'Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!' """

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')


data = 1000 * [(1, "testing", 5555.5555)]
 
def multi_insert(odbc):
 
    conn = odbc.connect(dsn)
    crs = conn.cursor()
 
    crs.execute("""
   CREATE TABLE ##test (
       field_int int,
       field_vc varchar(50),
       field_dec decimal(10, 5)
   )
   """)
 
    def test():
        start_time = time.time()
        crs.executemany("""
       INSERT INTO ##test (field_int, field_vc, field_dec) VALUES (?, ?, ?)
       """, data)
        end_time = time.time()
        return end_time - start_time
 
    times = []
 
    for x in range(3):
        times.append(test())
 
    conn.close()
 
    return times
 
print "ceODBC:", multi_insert(ceODBC)
print "pyodbc:", multi_insert(pyodbc)
















import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pypyodbc
import pyodbc

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
group by lwc.id limit 10;""", con=conSolar)

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')

cursor = cnxn_dev.cursor()
#Performing all transformations
querystring ="""
DELETE FROM EXTR_WORKCASES


                INSERT INTO Extr_workcases ([Country]      ,[LWCid]      ,[Action_by]      ,[CustomerID]      ,[LoanPortfolioID]      ,[closed_at]      ,[activity_category_id])
                VALUES (?,?,?,?,?,?,?)
    """
cursor.executemany(querystring, [df_workcases['Country'].values, df_workcases['LWCid'].values, df_workcases['Action_by'].values, df_workcases['CustomerID'].values, df_workcases['LoanPortfolioID'].values, df_workcases['closed_at'].values, df_workcases['activity_category_id'].values])
cnxn_dev.commit()
