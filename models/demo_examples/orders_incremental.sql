{{
  config(
    materialized='incremental',
    incremental_strategy='pgmerge',
    unique_key='order_id',
  )
}}

with source as (

    select * from {{ source('example', 'orders_custom_materialization') }}

),


renamed as (

    select
        order_id,
        status as order_status,
        shipment_delayed_flag,
        contact_name as contact_first_name, 
        created_at,
        updated_at

    from source

)

select * from renamed

--{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
--  where order_id not in (select order_id from {{this}} )

--{% endif %}


/*


---STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
      drop table tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization;
      drop table tdunlap_sandbox_dev.dbt_tdunlap.orders_incremental;
      

--STEP 2: Create RAW table that will be updated/overwritten
      create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization (
          order_id integer,
          status varchar (100),
          shipment_delayed_flag integer,
          contact_name varchar (100),
          created_at datetime,
          updated_at datetime
      );


--STEP 3: Insert initial values into table 
      insert into tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization (order_id, status, shipment_delayed_flag, contact_name, created_at, updated_at)
      values (1, 'delivered', 0, 'Stephanie', '2020-01-01', '2020-01-04'),
             (2, 'shipped', 0, 'Karl', '2020-01-02', '2020-01-04'),
             (3, 'shipped', 0, 'Vanesa', '2020-01-03', '2020-01-04'),
             (4, 'processed', 0, 'Beth','2020-01-04', '2020-01-04');
      commit;


--STEP 4: Run dbt for the first time to create the initial load
      select * from tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization;
      --dbt run --select orders_incremental
      select * from tdunlap_sandbox_dev.dbt_tdunlap.orders_incremental
      
--STEP 5: Truncate Source Table 
     truncate table tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization;

--STEP 6: Insert new values into source table 
       insert into tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization (order_id, status, shipment_delayed_flag, contact_name, created_at, updated_at)
      values
             (4, 'delivered', 0, 'Beth','2020-01-04', '2020-01-08'),
             (5, 'shipped', 0, 'Katherine', '2020-01-08', '2020-01-08'),
             (6, 'shipped', 0, 'Amanda', '2020-01-08', '2020-01-08');

      commit; 

--STEP 7: Run dbt for the second time to create the incremental load
      select * from tdunlap_sandbox_dev.dbt_tdunlap.orders_custom_materialization;
      --dbt run;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.orders_incremental
;


*/

