import pandas as pd
import pymssql
from sqlalchemy import create_engine

connMSSQL = r'mssql+pymssql://localhost/mbsol'
engineMS = create_engine(connMSSQL)
#connection = engine1.connect()

df = pd.read_sql_query("""SELECT top 10 *
	from DailyIsWas""", engineMS)#con=connection)
print df



#result = connection.execute("SELECT top 10 * from DailyIsWas")
#row = result.fetchone()
#print(row['bal1'])