with source as (

    select * from {{ source('tpch', 'customer') }}

),

rename as (

    select
    
        c_custkey as customer_key,
        c_name as name,
        c_address as address, 
        c_nationkey as nation_key,
        c_phone as phone_number,
        {{cents_to_dollars('c_acctbal')}} as account_balance,
        c_mktsegment as market_segment,
        c_comment as comment

    from source

)

select * from rename
