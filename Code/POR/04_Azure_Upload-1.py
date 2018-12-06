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

df_Migration_agg=pd.read_sql_query("""SELECT  * FROM Migration_Agg""", con=engineMS)
df_Migration_agg.to_sql('Migration_agg',engineAzure,if_exists='replace')

df_Target=pd.read_sql_query("""SELECT  * FROM Migration_Target""", con=engineMS)
df_Target.to_sql('Migration_Target',engineAzure,if_exists='replace')

