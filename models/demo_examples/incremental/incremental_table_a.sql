{{
    config(
        materialized='incremental'
    )
}}

select 
    a.*
    ,current_timestamp() as insert_timestamp
from {{ ref('incremental_source') }} a

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where id not in (select id from {{this}} )

{% endif %}

/*
select * from TDUNLAP_SANDBOX_DEV.DBT_TDUNLAP.incremental_source
select * from  TDUNLAP_SANDBOX_DEV.DBT_TDUNLAP.incremental_table_a order by id
*/