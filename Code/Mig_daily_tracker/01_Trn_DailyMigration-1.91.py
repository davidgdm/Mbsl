import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib
import pyodbc 


#AZURE DWH SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineDWH = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

cnxn_dev = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=mbslbiserver.database.windows.net;'
                      'Database=mbsldwh_dev;'
                      'UID=Reports;'
                      'PWD=mbsl1234!')

#Uploading last day into DWH
df_Migration = pd.read_sql_query("""select * from Migration_agg where snapshot_at =(select max(snapshot_at)from Migration_agg)""", con=cnxn_dev)
df_Migration.to_sql('Migration_Agg',engineDWH,if_exists='append',index=False)

