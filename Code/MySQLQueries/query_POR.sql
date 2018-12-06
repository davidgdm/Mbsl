SELECT
    la.currency,
    c.name,
    pr.number AS loan_account_number,
    c.customer_number AS customer_number,
    la.state,
    la.state_at,
    MIN(ca.charged_until) AS charged_until,
    DATEDIFF(CURRENT_DATE(), MIN(ca.charged_until)) AS days_since_system_off,
    la.risk_category,
    IF(lplan.grace_period_days = 0, 0, ((IFNULL(ANY_VALUE(gpct_days_sum), 0) + IF(la.state = 'exceeded_grace_period', DATEDIFF(CURRENT_DATE(), la.state_at), 0) - IFNULL(ANY_VALUE(gpe_days_sum), 0)) / DATEDIFF(CURRENT_DATE(), LAST_DAY(la.handover_at))) * ((lplan.installment_periods * lplan.installment_period_days + lplan.down_payment_days) / lplan.grace_period_days) * 100) AS relative_gp_used,
    la.par_days,
    LAST_DAY(la.handover_at) AS handover_at,
    ANY_VALUE(spl.name) AS system_product,
    lplan.loan_subunit/100 AS total_amount_loan,
    la.balance_subunit/100 AS repayment_balance,
    (lplan.loan_subunit/100 - la.balance_subunit/100) AS total_outstanding,
    mh.hub_number AS hub_name,
    a.area1,
    a.area2,
    a.area3,
    a.area4,
    c.phone_number,
    c.second_phone_number,
    c.third_phone_number,
    CONCAT(ANY_VALUE(con.name), ' ', ANY_VALUE(con.surname)) AS referrer_contractor_name,
    CONCAT(ANY_VALUE(con_fundi.name), ' ', ANY_VALUE(con_fundi.surname)) AS fundi_name,
    u.name AS lfo_name,
    tab.rescheduled_at
    FROM loan_accounts la
    INNER JOIN (SELECT
      lrr.resultant_loan_account_id,
      IF(lrr.resultant_loan_account_id IS NOT NULL, lrr.resultant_loan_account_id, la.id) AS loan_account_id,
      la.assigned_payment_account_id AS original_payment_account_id,
      CASE
      WHEN lrr.resultant_loan_account_id IS NOT NULL THEN LAST_DAY(lrr.approved_at)
      WHEN tab.loan_portfolio_id IS NOT NULL THEN LAST_DAY(tab.created_at)
      WHEN tab2.loan_portfolio_id IS NOT NULL THEN LAST_DAY(tab2.created_at)
      ELSE NULL 
      END AS rescheduled_at
      FROM loan_portfolios lps
      LEFT JOIN loan_reschedule_requests lrr ON lrr.loan_portfolio_id = lps.id
      INNER JOIN loan_accounts la ON la.loan_portfolio_id = lps.id
      LEFT JOIN (SELECT 
      lps.id AS loan_portfolio_id,
      ts.created_at
      FROM loan_portfolios lps
      INNER JOIN loan_accounts la ON la.loan_portfolio_id = lps.id 
      LEFT JOIN taggings ts ON ts.taggable_type = 'LoanAccount' AND ts.taggable_id = la.id 
      LEFT JOIN tags t ON t.id = ts.tag_id AND t.name IN('rescheduled', 'rwanda drought rescheduling', 'Rescheduling pilot')
      WHERE t.name IS NOT NULL) tab ON tab.loan_portfolio_id = lps.id
      LEFT JOIN (SELECT 
      lps.id AS loan_portfolio_id,
      ts_cus.created_at
      FROM loan_portfolios lps
      INNER JOIN customers c ON lps.customer_id = c.id
      LEFT JOIN taggings ts_cus ON ts_cus.taggable_type = 'Customer'  AND ts_cus.taggable_id = c.id 
      LEFT JOIN tags t_cus ON t_cus.id = ts_cus.tag_id AND t_cus.name IN('rescheduled', 'rwanda drought rescheduling', 'Rescheduling pilot')
    WHERE t_cus.name IS NOT NULL) tab2 ON tab2.loan_portfolio_id = lps.id) AS tab ON tab.loan_account_id = la.id
    INNER JOIN loan_portfolios lp ON lp.id = la.loan_portfolio_id
    INNER JOIN customers c ON c.id = lp.customer_id
    INNER JOIN systems s ON s.id = lp.system_id
    INNER JOIN locations l ON l.id=c.location_id
    INNER JOIN location_areas a ON a.id=l.location_area_id
    INNER JOIN loan_manager_location_areas lmla ON lmla.location_area_id = a.id
    INNER JOIN users u ON u.id = lmla.user_id
    INNER JOIN hubs mh ON c.hub_id=mh.id AND mh.role='market'
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
    LEFT JOIN (SELECT gpe.loan_account_id AS la_id, SUM(gpe.days) AS gpe_days_sum FROM loan_account_grace_period_extensions gpe GROUP BY gpe.loan_account_id) gpe_sums
     ON gpe_sums.la_id = la.id
    WHERE la.risk_category IS NOT NULL
    AND la.state NOT IN('paid_off', 'defaulted', 'canceled')
    AND spl.segment = 0
    GROUP BY la.id
    ORDER BY la.id