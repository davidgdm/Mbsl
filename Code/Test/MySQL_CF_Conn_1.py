import pandas as pd
import mysql.connector as sql
from sqlalchemy import create_engine


#Connecting to our database

#engine = create_engine('mysql+mysqlconnector://cf:vision2020@10.100.25.13/test12')


con = sql.connect(user='cf',password='Admin1234.,',host='10.100.25.13',database='cf_portfolio')
#creating cursor to use its various methods further
#cur = con.cursor()

df = pd.read_sql_query("""SELECT
	* from samplesm limit 10""", con=con)
print df