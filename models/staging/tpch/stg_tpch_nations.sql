{{
    config(
        schema='managed_access_test'
    )
}}
with source as (

    select * from {{ source('tpch', 'nation') }}

),

renamed as (

    select
    
        n_nationkey as nation_key,
        n_name as name,
        n_regionkey as region_key,
        n_comment as comment,
        '{{env_var('DBT_ENV_TYPE')}}' as env_type

    from source

)

select * from renamed