{{
    config(
        materialized='table'
    )
}}

with freshness as (

    select * from {{ ref('stg_dino_freshness') }}

),

final as (

    select
    
        rownumber,
        dinosaur,
        source_insert_timestamp,
        CURRENT_TIMESTAMP() as table_inserted_timestamp

    from freshness

)

select * from final