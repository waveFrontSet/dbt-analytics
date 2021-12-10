-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Return records where this ins't true to make the test fail.
select
    order_id,
    sum(amount) as total_amount
from {{ ref('stg_payments') }}
group by order_id
having total_amount < 0