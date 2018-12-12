import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pypyodbc
import pyodbc
import time
import pymssql

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of WC
df_dailysnapshot = pd.read_sql_query("""SELECT la.id as loan_account_id,
	la.loan_portfolio_id,
    la.currency,
    u.name AS lfo_name,
    la.state,
    la.risk_category,
    la.par_days,
    la.handover_at,
    (lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    date_add(CURDATE(),  INTERVAL -1 day) as snapshot_at
    FROM loan_accounts la 
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
    
    WHERE la.risk_category IS NOT NULL
    AND la.state NOT IN('paid_off', 'defaulted', 'canceled')
    GROUP BY la.id
    ORDER BY la.id;""", con=conSolar)

#cnxn_dev =pyodbc.connect(server='localhost', user='sa', password='Admin1234.,', database='mbsol')

cnxn_dev=pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')

                      


tuples = list(zip(df_dailysnapshot["loan_account_id"],df_dailysnapshot["loan_portfolio_id"],df_dailysnapshot["currency"],df_dailysnapshot["lfo_name"],df_dailysnapshot["state"],df_dailysnapshot["risk_category"],df_dailysnapshot["par_days"],df_dailysnapshot["handover_at"],df_dailysnapshot["outstanding_amount"],df_dailysnapshot["snapshot_at"]))

cursor = cnxn_dev.cursor()

query="""Insert into [Extr_Dailysnapshot] ([loan_account_id]
      ,[loan_portfolio_id]
      ,[currency]
      ,[lfo_name]
      ,[state]
      ,[risk_category]
      ,[par_days]
      ,[handover_at]
      ,[outstanding_amount]
      ,[snapshot_at]) values(?,?,?,?,?,?,?,?,?,?) """

start = time.time()

cursor.executemany(query,tuples)
cnxn_dev.commit()

end = time.time()
print(end - start)

cursor.close
cnxn_dev.close()

#params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mbsol;UID=sa;PWD=Admin1234.,")
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineMSlocal = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

start = time.time()
df_dailysnapshot.to_sql('Extr_Dailysnapshot',engineMSlocal,if_exists='replace',index=False)
end = time.time()
print(end - start)