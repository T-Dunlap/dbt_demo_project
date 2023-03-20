{{
    config(
        enabled=true,
        severity='error',
        tags = ['finance']
    )
}}

with orders as ( select * from {{ ref('fct_sf_orders') }} )

select *
from   orders 
where  
    LEN(TRUNCATE(REVERSE(total_price))) < 0
    OR 
    LEN(TRUNCATE(REVERSE(total_price))) > 2