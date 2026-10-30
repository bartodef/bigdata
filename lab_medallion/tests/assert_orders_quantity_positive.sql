-- Exercise 2: no generic test for "> 0" without dbt_utils, so a singular test
select order_id, quantity
from {{ ref('stg_orders') }}
where quantity <= 0
