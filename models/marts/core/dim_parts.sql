{{
    config(
        materialized = 'table'
    )
}}
with part as (

    select * from {{ref('stg_tpch_parts')}}

),

final as (
    select 
        part_key,
        manufacturer,
        name,
        brand as brand,
        type,
        size,
        container,
        retail_price,
        'test' as test_column
    from
        part
)
select *
from final  
order by part_key