import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from turbodbc import connect, make_options


conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of WC
df_workcases = pd.read_sql_query("""select left(hubs.hub_name,2) as Country,lwc.id as LWCid, u.name as 'Action_by',c.id as CustomerID, lp.id as LoanPortfolioID, lwc.closed_at, 
lwc.activity_category_id
from loan_workout_cases lwc
inner join customers c on c.id=lwc.customer_id
inner join loan_portfolios lp on lp.customer_id=c.id
inner join payment_accounts pa on pa.loan_portfolio_id=lp.id
inner join payments p on p.payment_account_id=pa.id
inner join users u on lwc.user_id = u.id
inner join hubs on c.hub_id=hubs.id
where lwc.could_reach_customer=1 and lwc.activity_category_id IN ("5252","5253") and lwc.closed_at >= "2018-12-10" 
group by lwc.id 
limit 10;""", con=conSolar)



options = make_options(parameter_sets_to_buffer=1000)
conn = connect(driver='{ODBC Driver 17 for SQL Server}', server='mbslbiserver.database.windows.net', database='mbsldwh_dev', turbodbc_options=options)

test_query = '''DELETE FROM EXTR_WORKCASES


                INSERT INTO Extr_workcases ([Country],[LWCid]      ,[Action_by]      ,[CustomerID]      ,[LoanPortfolioID]      ,[closed_at]      ,[activity_category_id])
                VALUES (?,?,?,?,?,?,?) '''

cursor.executemanycolumns(test_query, [df_workcases['Country'].values, df_workcases['LWCid'].values, df_workcases['Action_by'].values, df_workcases['CustomerID'].values, df_workcases['activity_category_id'].values, df_workcases['closed_at'].values, df_workcases['closed_at'].values])