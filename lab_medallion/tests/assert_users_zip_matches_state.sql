-- Exercise 4: the first 3 digits of the ZIP code must belong to a range allowed for the state
-- Faker draws the ZIP code independently of the state, so most rows fail: kept as a warning to monitor
{{ config(severity='warn') }}

select u.user_id, u.state, u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_ranges') }} z
    where z.code = u.state
      and left(u.zip_code, 3) between z.zip3_min and z.zip3_max
)
