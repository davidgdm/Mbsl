import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine

#Upload daily HLA
df_csv=pd.read_csv(r'C:\Users\gmartinez\Documents\Docs\coding\POR\portfolio_overview_export_TZ20180709-1016-1i3uqh5.csv')

#MSSQL Connection
connMSSQL = r'mssql+pymssql://sa:Admin1234.,@localhost/mbsol'
engineMS = create_engine(connMSSQL)

df_csv.to_sql('Extr_POR_20180709',engineMS,if_exists='replace')

