import pandas as pd
import mysql.connector as sql
import pymssql
from sqlalchemy import create_engine

#MSSQL Connection
#connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@10.100.25.13/mbsol'
engineMS = create_engine(connMSSQL)
#connection = engine1.connect()

#df = pd.read_sql_query("""SELECT top 10 *
	#from DailyIsWas""", engineMS)#con=connection)
#print df

#Connecting to solarhub
conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub
df = pd.read_sql_query("""SELECT	* from hubs limit 10""", con=conSolar)
#Inserting data from solarhub into MS-SQL
df.to_sql('test_fromsolarhub',engineMS,if_exists='replace')


