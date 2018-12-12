import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import os

os.system("""bcp Extr_Dailysnapshot in "/home/dwh/ETL/mbsl/Code/Test/daily.csv" -S mbslbiserver.database.windows.net -d mbsldwh_dev -U Reports -P mbsl1234! -q -c -t ,'""")


#conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production', port='3306')
#conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production',port='3306')

#df_la = pd.read_sql_query("""SELECT *
#FROM loan_accounts  limit 10;""", con=conSolar)

#print(df_la)
