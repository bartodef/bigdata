-- A user cannot order before being born: the test fails if this query returns rows
-- Invalid birthdates are set to NULL in stg_users (exercise 3), so the test is an error again, not a warning
select
    o.order_id,
    o.ordered_at,
    u.user_id,
    u.birthdate
from {{ ref('stg_orders') }} o
join {{ ref('stg_users') }} u on o.user_id = u.user_id
where o.ordered_at < u.birthdate
