import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine
import pymssql
import pyodbc
import urllib

params = urllib.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER=mbsldavidtest.database.windows.net;DATABASE=MBSLTest;UID=david;PWD=Admin1234.,")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)


df_Target=pd.read_sql_query("""SELECT  * FROM lfo_country""", con=engineMS)
df_Target.to_sql('lfo_country',engineAzure,if_exists='replace')