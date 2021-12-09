with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

successful_payments as (
    select * from payments
    where payments.status = 'success'
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(successful_payments.amount, 0) as amount
    from orders
    left join successful_payments using (order_id)
)

select * from final