{{
    config(
        materialized='ephemeral',
        database = 'tdunlap_ghx',
        schema = 'data_acq'
    )
}}

with poh as (
    select * from {{ source('ghx', 'poh') }}
)

select 
*
from poh