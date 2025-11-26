with orders as (
    select * from `etl_demo`.`stg_orders`
),

summary as (
    select
        customer_id,
        count(*)        as total_orders,
        sum(amount)     as total_amount
    from orders
    group by customer_id
)

select * from summary