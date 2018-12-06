select la.id, la.handover_at, la.currency, li.product_name
from loan_accounts la inner join loan_items li on la.id=li.loan_account_id
where li.product_name like 'one%' and la.currency not like 'rwf';

