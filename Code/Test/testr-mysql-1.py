import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib


#conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
conSolar = sql.connect(user='mobisol_data_warehouse',password='mydLalm8EjimLojOd3',host='127.0.1.1',database='solarhub_production')

df_la = pd.read_sql_query("""SELECT *
FROM loan_accounts  limit 10;""", con=conSolar)

print(df_la)
