with customers as (
    select * from {{ ref('dim_customers') }}
)

,orders as (
    select * from {{ ref('fct_orders') }}
)

select 
    customers.customer_key
    ,name
    ,market_segment
    ,SUM(NET_ITEM_SALES_AMOUNT) as LTV
from customers
inner join orders on customers.customer_key = orders.customer_key 
group by 1,2,3