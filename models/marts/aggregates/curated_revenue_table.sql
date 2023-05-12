select
    order_date,
    region_name,
    nation,
    sum(gross_item_sales_amount) as gross_revenue,
    sum(item_cogs) as cost_of_goods_sold
    

from {{ ref('fct_order_items') }}
    group by 
        order_date, region_name, nation
    order by 
        order_date, region_name, nation