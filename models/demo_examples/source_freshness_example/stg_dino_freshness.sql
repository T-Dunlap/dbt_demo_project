{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('freshness', 'freshness_dinosaurs') }}

),

renamed as (

    select
    
        rownumber as rownumber,
        dinosaur as dinosaur,
        insert_timestamp as source_insert_timestamp

    from source

)

select * from renamed