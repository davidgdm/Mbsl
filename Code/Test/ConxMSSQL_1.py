import pyodbc 
import pandas as pd

server = '10.100.25.13'
database = 'Mbsol' 
username = 'cf' 
password = 'vision2020' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

cursor = cnxn.cursor()
df = pd.read_sql_query("""SELECT TOP 10 * FROM portfolio""", con=cnxn)

print df

#closing the function
cursor.close()
