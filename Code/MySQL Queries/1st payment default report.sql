#1st payment default
SELECT
la.id as Loan_Account_Id,
'1' as PaymentDefault,
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
    c.third_phone_number
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
    where la.risk_category in ('par1','par15')
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -2 month) 
GROUP BY la.id
#2nd payment default
UNION
SELECT
la.id as Loan_Account_Id,
'2' as PaymentDefault,
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
    c.third_phone_number
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
    where la.risk_category='par30'
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -3 month) 
GROUP BY la.id
#3er payment default
UNION
SELECT
la.id as Loan_Account_Id,
'3' as PaymentDefault,
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
    c.third_phone_number
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
    where la.risk_category='par60'
    and la.handover_at>=date_add(CURDATE(),  INTERVAL -4 month) 
GROUP BY la.id
