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
import pandas.io.sql as psql

#------------------------EXTRACTION---------------------------------
#payments extraction 
os.system("sudo systemctl restart db-ssh")
print("service restarted")
time.sleep(2)
print("2 sec waited")
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')
#conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
print('payments MySQl query started')

chunk_size = 50000
offset = 0
dfs = []
while True:
  sql = """SELECT pa.loan_portfolio_id, convert(p.transaction_at,date) as transaction_at, sum(p.amount_subunit)/100 as "Payment_Amount"
            FROM payments p inner join payment_accounts pa on p.payment_account_id=pa.id
            where convert(transaction_at,date)>= DATE_ADD(CURDATE(), INTERVAL -30 day) #'2018-10-1'
            and  pa.loan_portfolio_id is not null
            and pa.loan_portfolio_id in (SELECT loan_portfolio_id FROM loan_workout_cases where closed_at>=DATE_ADD(CURDATE(), INTERVAL -30 day)) #Extracts only cases with a Workout case
            group by pa.loan_portfolio_id, convert(p.transaction_at,date) limit %d offset %d """ % (chunk_size,offset) 
  dfs.append(pd.read_sql_query(sql, conSolar))
  offset += chunk_size
  print(offset)
  if len(dfs[-1]) < chunk_size:
    break
full_df = pd.concat(dfs)

print('payments MySQl query finished')

#df_payments.to_csv('/home/dwh/ETL/mbsl/Code/payments.csv', index=False, chunksize=1000)
#full_df.to_csv('c:/testETL/payments.csv', index=False,  chunksize=10000, mode='a')
full_df.to_csv('/home/dwh/ETL/mbsl/Code/payments.csv', index=False)

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

#os.system("""bcp Extr_payments in "/home/dwh/ETL/mbsl/Code/payments.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""")
os.system("""/opt/mssql-tools/bin/bcp Extr_payments in "/home/dwh/ETL/mbsl/Code/payments.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""")

#######################################################################################################################

print('Extraction WC started')
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
where lwc.could_reach_customer=1 and lwc.activity_category_id IN ("5252","5253") and lwc.closed_at >= DATE_ADD(CURDATE(), INTERVAL -30 day) #'2018-10-1'
group by lwc.id;""", con=conSolar)

df_workcases.to_csv('/home/dwh/ETL/mbsl/Code/wc.csv', index=False)

querystring ="""truncate table Extr_workcases;"""
cursor.execute(querystring)
cnxn_dev.commit()
print('truncate Extr_workcases finished')

os.system("""/opt/mssql-tools/bin/bcp Extr_workcases in "/home/dwh/ETL/mbsl/Code/wc.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,""")
print('upload WC finished')

#######################Transformation
querystring ="""--Refresh data using latest extraction
declare @last_dt date
declare @payment_min_dt date

set @last_dt =(select max([transaction_at]) from [Extr_payments] )
set @payment_min_dt=(select min([transaction_at]) from [Extr_payments] )

delete from t_payments_consolidated where [transaction_at]>=@payment_min_dt;
insert into t_payments_consolidated select * from [Extr_payments];

delete from t_wc_consolidated where [closed_at]>=(select min([closed_at]) from [Extr_workcases] );
insert into t_wc_consolidated select * from [Extr_workcases];
/*
truncate table [t_daily_wc];
insert into [t_daily_wc] ([Country],[LWCid],[Action_by],[CustomerID],[LoanPortfolioID],[closed_at],[activity_category_id])
select * from [Extr_workcases]
*/
truncate table [Trn_Recovery]
insert into [Trn_Recovery] ([loan_portfolio_id],[transaction_at],[payments within 5 days])
(select * from [Extr_payments] where loan_portfolio_id in 
(select [LoanPortfolioID]  from t_wc_consolidated where [closed_at]<=dateadd(day, -6, getdate()) group by [LoanPortfolioID]))

update trn_recovery
set [LWCiD]=(select max(wc.[LWCiD]) from [Extr_workcases] wc 
where (wc.closed_at <= trn_recovery.[transaction_at] and wc.closed_at >= dateadd(day,-5, trn_recovery.[transaction_at]))
and wc.[LoanPortfolioID] =trn_recovery.[loan_portfolio_id])

delete from trn_recovery where [LWCiD] is null

update trn_recovery
set country= wc.[Country],
	  [Action_by]=wc.[Action_by],
	  [CustomerID]=wc.[CustomerID],
      [closed_at]=wc.[closed_at],
      [activity_category_id]=wc.[activity_category_id]
	from [Extr_workcases] wc 
	where wc.[LWCiD]=trn_recovery.[LWCiD];

update trn_recovery set [activity_category]= 'call' where [activity_category_id]=5252
update trn_recovery set [activity_category]= 'visit' where [activity_category_id]=5253

delete from t_recovery_consolidated where [transaction_at]>=(select min([transaction_at]) from [Extr_payments] )--@payment_min_dt;
insert into t_recovery_consolidated select * from trn_recovery; """

cursor.execute(querystring)
cnxn_dev.commit()
print('transformation finished')