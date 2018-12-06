import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine

#MSSQL Connection
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

df_portfolio = pd.read_sql_query("""SELECT 
    * from portfolio where monthobs>='201801' """, con=engineMS)

connMySQL = r'mysql+pymysql://cf:Vision2020@10.100.25.13/cf_portfolio'
engineMy = create_engine(connMySQL)

df_portfolio.to_sql('portfolio',engineMy,if_exists='replace')