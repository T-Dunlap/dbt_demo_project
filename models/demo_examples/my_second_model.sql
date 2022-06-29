{{
    config(
        materialized='view'
    )
}}

select 
    my_favorite_number, 
    my_favorite_number * 2 as my_lucky_number
from {{ ref('my_first_model') }}