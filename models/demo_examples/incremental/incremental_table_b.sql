{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        on_schema_change='fail',
        merge_exclude_columns = ['insert_timestamp']
        
    )
}}

select 
    a.*
    ,current_timestamp() as insert_timestamp
from {{ ref('incremental_source') }} a 

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where source_timestamp > (select max(source_timestamp) from {{ this }})

{% endif %}
/*
select * from TDUNLAP_SANDBOX_DEV.DBT_TDUNLAP.incremental_source
select * from  TDUNLAP_SANDBOX_DEV.DBT_TDUNLAP.incremental_table_b order by id
*/