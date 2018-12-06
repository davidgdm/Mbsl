import pandas as pd
import mysql.connector as sql


#Connecting to our database

con = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#creating cursor to use its various methods further
cur = con.cursor()

df = pd.read_sql_query("""SELECT
	la.id as Loan_Account_id,
    la.currency,
    c.customer_number AS customer_number,
    la.risk_category,
    la.par_days,
    u.name AS lfo_name
    FROM loan_accounts la
    INNER JOIN loan_portfolios lp ON lp.id = la.loan_portfolio_id
    INNER JOIN customers c ON c.id = lp.customer_id
    INNER JOIN locations l ON l.id=c.location_id
    INNER JOIN location_areas a ON a.id=l.location_area_id
    INNER JOIN loan_manager_location_areas lmla ON lmla.location_area_id = a.id
    INNER JOIN users u ON u.id = lmla.user_id
    GROUP BY la.id""", con=con)
print df

#closing the function
cur.close()