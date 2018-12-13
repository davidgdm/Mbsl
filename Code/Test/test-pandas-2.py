import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
from urllib.parse import quote  # Python 3+
import pyodbc
import os

#payments extraction 
os.system("sudo systemctl restart db-ssh")
print("service restarted")
time.sleep(2)
print("2 sec waited")
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')

print('mysql query stared')
df_la = pd.read_sql_query("""SELECT *
FROM loan_accounts  limit 10000;""", con=conSolar)

print('uploading started')
params = quote("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineDev = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

#engineDev=pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
#                      'Server=mbslbiserver.database.windows.net;'
#                      'Database=mbsldwh_dev;'
#                      'UID=Reports;'
#                      'PWD=mbsl1234!')

df_la.to_sql('Temp_la',engineDev, if_exists='replace',index=False)



