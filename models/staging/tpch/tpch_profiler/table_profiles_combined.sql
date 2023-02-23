select 
    'tpch_customers' as TableSource
    ,*
from {{ ref('tpch_customer_profiler') }}

union 

select 
    'tpch_orders' as TableSource
    ,*
from {{ ref('tpch_orders_profiler') }}

union 

select 
    'tpch_line_items' as TableSource
    ,*
from {{ ref('tpch_line_items_profiler') }}