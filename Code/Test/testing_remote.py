import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import ctds

#AZURE SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_Migration_Agg = pd.read_sql_query("""SELECT top 10 * FROM Migration_Agg """,   engineAzure )

conn = ctds.connect('mbslbiserver.database.windows.net', user='Reports', password='mbsl1234!', database='mbsldwh_dev')
conn.bulk_insert('Migration_Agg', (df_Migration_Agg.to_records(index=False).tolist()))