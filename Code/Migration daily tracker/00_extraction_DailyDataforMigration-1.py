import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#extraction of reschedule loan_portoflio_id
df_reschedule = pd.read_sql_query("""SELECT resultant_loan_account_id FROM loan_reschedule_requests
where resultant_loan_account_id is not null;""", con=conSolar)

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

#extraction of daily snatshot
df_dailysnapshot = pd.read_sql_query("""SELECT la.id as loan_account_id,
	la.loan_portfolio_id,
    la.currency,
    u.name AS lfo_name,
    la.state,
    la.risk_category,
    la.par_days,
    la.handover_at,
    (lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    date_add(CURDATE(),  INTERVAL -1 day) as snapshot_at
    FROM loan_accounts la 
        INNER JOIN 
    loan_plans lpl ON lpl.id = la.loan_plan_id
        INNER JOIN
    loan_portfolios lp ON lp.id = la.loan_portfolio_id
        INNER JOIN
    customers c ON c.id = lp.customer_id
        INNER JOIN
    locations l ON l.id = c.location_id
        INNER JOIN
    location_areas a ON a.id = l.location_area_id
        INNER JOIN
    loan_manager_location_areas lmla ON lmla.location_area_id = a.id
        INNER JOIN
    users u ON u.id = lmla.user_id
    
    WHERE la.risk_category IS NOT NULL
    AND la.state NOT IN('paid_off', 'defaulted', 'canceled')
    GROUP BY la.id
    ORDER BY la.id""", con=conSolar)

#AZURE SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh_dev;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_reschedule.to_sql('Extr_Reschedule',engineAzure,if_exists='replace',index=False)
df_cooperatives_rw.to_sql('Extr_Cooperatives_rw',engineAzure,if_exists='replace',index=False)
df_dailysnapshot.to_sql('Extr_Dailysnapshot',engineAzure,if_exists='replace',index=False)
