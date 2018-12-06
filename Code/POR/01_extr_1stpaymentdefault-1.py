import pandas as pd 
import mysql.connector as sql
from sqlalchemy import create_engine, text
from sqlalchemy import delete
from datetime import datetime
import time
import urllib

conSolar = sql.connect(user='david',password='ItJubSheg6',host='kaa.plugintheworld.com',database='solarhub_production')
#Retrieving data from SOLARhub

#Query extracting dayly HLAs data from SolarHUB
df_payment_default = pd.read_sql_query("""#1st payment default
SELECT
la.id as Loan_Account_Id,
'1st' as PaymentDefault,
 c.name,
la.risk_category,
la.state,
lpl.currency,
u.name AS lfo_name,
la.handover_at,
(lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    a.area1,
    a.area2,
    a.area3,
    a.area4,
    c.phone_number,
    c.second_phone_number,
    c.third_phone_number,
    CONCAT(con.name, ' ', con.surname) AS Sales_Agent,
    resultant_loan_account_id,
    case when rr.resultant_loan_account_id is not null then "Yes" else "No" End as Reschedule,
    CURDATE() as snapshot_at
FROM
loan_accounts la 
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
   LEFT JOIN customer_forms cf ON cf.customer_id = lp.customer_id  AND cf.form_name = 'application'
   LEFT JOIN contractors con ON con.id = cf.contractor_id
   left join loan_reschedule_requests rr on rr.resultant_loan_account_id=la.id
    where la.risk_category in ('par1','par15')
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -3 month) 
    and cf.form_name = 'application'
GROUP BY la.id

UNION


#1st & 2nd payment default
SELECT
la.id as Loan_Account_Id,
'1st & 2nd' as PaymentDefault,
 c.name,
la.risk_category,
la.state,
lpl.currency,
u.name AS lfo_name,
la.handover_at,
(lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    a.area1,
    a.area2,
    a.area3,
    a.area4,
    c.phone_number,
    c.second_phone_number,
    c.third_phone_number,
    CONCAT(con.name, ' ', con.surname) AS Sales_Agent,
    resultant_loan_account_id,
    case when rr.resultant_loan_account_id is not null then "Yes" else "No" End as Reschedule,
    CURDATE() as snapshot_at
FROM
loan_accounts la 
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
   LEFT JOIN customer_forms cf ON cf.customer_id = lp.customer_id  AND cf.form_name = 'application'
   LEFT JOIN contractors con ON con.id = cf.contractor_id
   left join loan_reschedule_requests rr on rr.resultant_loan_account_id=la.id
    where la.risk_category in ('par30')
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -4 month) 
    and cf.form_name = 'application'
GROUP BY la.id

UNION


#1st & 2nd & 3RD payment default
SELECT
la.id as Loan_Account_Id,
'1st & 2nd & 3rd' as PaymentDefault,
 c.name,
la.risk_category,
la.state,
lpl.currency,
u.name AS lfo_name,
la.handover_at,
(lpl.loan_subunit - la.balance_subunit)/100 AS outstanding_amount,
    a.area1,
    a.area2,
    a.area3,
    a.area4,
    c.phone_number,
    c.second_phone_number,
    c.third_phone_number,
    CONCAT(con.name, ' ', con.surname) AS Sales_Agent,
    resultant_loan_account_id,
    case when rr.resultant_loan_account_id is not null then "Yes" else "No" End as Reschedule,
	CURDATE() as snapshot_at
FROM
loan_accounts la 
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
   LEFT JOIN customer_forms cf ON cf.customer_id = lp.customer_id  AND cf.form_name = 'application'
   LEFT JOIN contractors con ON con.id = cf.contractor_id
   left join loan_reschedule_requests rr on rr.resultant_loan_account_id=la.id
    where la.risk_category in ('par60')
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -5 month) 
    and cf.form_name = 'application'
GROUP BY la.id""", con=conSolar)

#SQL SERVER CONNECTION
params = urllib.quote_plus("DRIVER={ODBC Driver 13 for SQL Server};SERVER=mbslbiserver.database.windows.net;DATABASE=mbsldwh;UID=Reports;PWD=mbsl1234!")
engineAzure = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

df_payment_default.to_sql('Extr_Paydefault',engineAzure,if_exists='replace',index=False)
