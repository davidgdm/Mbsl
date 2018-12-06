import pymssql
import pandas as pd

conMSSQL = pymssql.connect("10.100.25.13", "cf", "vision2020", "Mbsol")
cursor = conMSSQL.cursor()

df = pd.read_sql_query("""SELECT TOP 10 * 
FROM portfolio""", con=conMSSQL)

print df

#closing the function
cursor.close()
