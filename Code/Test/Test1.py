#import mysql module
import mysql.connector

#Connecting to our database
con = mysql.connector.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')

#creating cursor to use its various methods further
cur = con.cursor()

#this is our mysql query
cur.execute("""select * from hubs""")

#It is going to fetch the executed mysql query into rows.
#Hence assigning it to row
row = cur.fetchall()

#printing the result
print row

#closing the function
cur.close()