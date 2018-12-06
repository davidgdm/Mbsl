import pymssql
import pandas as pd

conn = pymssql.connect("DE-HQ-PC0191", "sa", "Admin1234.,", "Mbsol")
cursor = conn.cursor()

df = pd.read_sql_query("""SELECT TOP 10 * 
FROM DAILYISWAS""", con=conn)

print df

#closing the function
cursor.close()
