SELECT la.id as loan_account_id,
    la.currency,
    u.name AS lfo_name,
    la.state,
    la.risk_category,
    la.par_days,
    la.handover_at,
    date_add(CURDATE(),  INTERVAL -1 day) as snapshot_at
    FROM loan_accounts la
    INNER JOIN loan_portfolios lp ON lp.id = la.loan_portfolio_id
    INNER JOIN customers c ON c.id = lp.customer_id
   
    INNER JOIN locations l ON l.id=c.location_id
    INNER JOIN location_areas a ON a.id=l.location_area_id
    INNER JOIN loan_manager_location_areas lmla ON lmla.location_area_id = a.id
    INNER JOIN users u ON u.id = lmla.user_id
   
    INNER JOIN payment_accounts pa ON pa.id = tab.original_payment_account_id
    INNER JOIN payment_references pr ON pr.payment_account_id = pa.id AND pr.principal = 1
    INNER JOIN charge_accounts ca ON ca.loan_account_id = la.id
    LEFT JOIN (SELECT gpct.charge_account_id AS ca_id, SUM(gpct.days) AS gpct_days_sum FROM charge_transactions gpct WHERE gpct.type = 'GracePeriodChargeTransaction' GROUP BY gpct.charge_account_id) gp_sums
     ON gp_sums.ca_id = ca.id
    INNER JOIN loan_plans lplan ON lplan.id = la.loan_plan_id
    LEFT JOIN referrals r ON r.referred_customer_id=c.id AND r.state = 'paid_out'
    LEFT JOIN contractors con ON con.id = r.referrer_id
    LEFT JOIN contractors con_fundi ON con_fundi.id = s.installation_fundi_id
    INNER JOIN sales_cases sc ON sc.payment_account_id = pa.id
    INNER JOIN sales_orders so ON so.sales_case_id = sc.id
    INNER JOIN sales_order_items soi ON soi.sales_order_id = so.id
    INNER JOIN sales_products soi_sp ON soi.sales_product_id = soi_sp.id
    INNER JOIN sales_product_lines spl ON spl.id = soi_sp.sales_product_line_id
    
    WHERE la.risk_category IS NOT NULL
    AND la.state NOT IN('paid_off', 'defaulted', 'canceled')
    GROUP BY la.id
    ORDER BY la.id