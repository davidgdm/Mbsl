import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub


#extraction of cooperatives loan_account_id in Rwanda
df_cooperatives_rw = pd.read_sql_query("""select id as loan_account_id from loan_accounts
    where loan_portfolio_id in
    (
    SELECT
    lp.id AS loan_portfolio_id
    FROM loan_portfolios lp
    INNER JOIN
        (SELECT
    MIN(pa.id) AS id,
    pa.loan_portfolio_id
    FROM payment_accounts pa
    GROUP BY pa.loan_portfolio_id) AS tab ON tab.loan_portfolio_id = lp.id INNER JOIN sales_cases sc ON sc.payment_account_id = tab.id INNER JOIN sales_orders so ON so.sales_case_id = sc.id INNER JOIN sales_order_items soi ON soi.sales_order_id = so.id INNER JOIN sales_products soi_sp ON soi.sales_product_id = soi_sp.id WHERE lp.id IS NOT NULL AND soi_sp.name IN("Cooperative premium bundle 100 W", "Cooperative basic bundle 100 W")
    )
        ;""", con=conSolar)


#AZURE SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)


df_cooperatives_rw.to_sql('Extr_Cooperatives_rw',engineAzure,if_exists='replace',index=False)

