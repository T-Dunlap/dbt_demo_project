with customers as (
    select * from {{ ref('dim_customers') }}
)

, orders as (
    select * from {{ ref('fct_orders') }}
)


, combined as (

    select 
        customers.customer_key,
        customers.name, 
        customers.market_segment,
        sum(orders.gross_item_sales_amount) as gross_sales
    from customers  
    inner join orders on customers.customer_key = orders.customer_key
    group by 1,2,3
)

select * from combined

