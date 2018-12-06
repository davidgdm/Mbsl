import pandas as pd
import mysql.connector as sql
import pymssql
import pymysql
from sqlalchemy import create_engine

#MSSQL Connection
connMySQL = r'mysql+pymysql://cf:Vision2020@10.100.25.13/CF_Portfolio'

engineMy = create_engine(connMySQL)

#Connecting to solarhub
conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub
df = pd.read_sql_query("""SELECT	* from hubs limit 10""", con=conSolar)




#Inserting data from solarhub into MS-SQL
df.to_sql('test_fromsolarhub_2',engineMy,if_exists='replace')


