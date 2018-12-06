SELECT
lp.id AS loan_portfolio_id
FROM loan_portfolios lp
INNER JOIN
(SELECT
  MIN(pa.id) AS id,
  pa.loan_portfolio_id
  FROM payment_accounts pa
  GROUP BY pa.loan_portfolio_id) AS tab ON tab.loan_portfolio_id = lp.id INNER JOIN sales_cases sc ON sc.payment_account_id = tab.id INNER JOIN sales_orders so ON so.sales_case_id = sc.id INNER JOIN sales_order_items soi ON soi.sales_order_id = so.id INNER JOIN sales_products soi_sp ON soi.sales_product_id = soi_sp.id WHERE lp.id IS NOT NULL AND soi_sp.name IN("Cooperative premium bundle 100 W", "Cooperative basic bundle 100 W")
