{{
    config(
        materialized='ephemeral'
    )
}}

Select * from {{ ref('stg_tpch_orders') }}