{% snapshot snap_orders_timestamp_type2 %}

{{
    config(
      target_database= target.database,
      target_schema=target.schema,
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('snap_orders_source', 'snap_orders_timestamp') }}

{% endsnapshot %}

/*


--STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
      drop table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp;
      drop table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp_type2;


--STEP 2: Create RAW table that will be updated/overwritten
      create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp (
          order_id integer,
          status varchar (100),
          created_at datetime,
          updated_at datetime
      );


--STEP 3: Insert initial values into table 
      insert into tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp (order_id, status, created_at, updated_at)
      values (1, 'delivered', '2020-01-01', '2020-01-04'),
             (2, 'shipped', '2020-01-02', '2020-01-04'),
             (3, 'shipped', '2020-01-03', '2020-01-04'),
             (4, 'processed', '2020-01-04', '2020-01-04');
      commit;


--STEP 4: Run dbt Snapshot for the first time and create the "_type2" table for the first time
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp_type2 order by order_id;


--STEP 5: Update values from table
      update tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp
      set status = 'delivered'
        ,updated_at = current_timestamp(2)
      where order_id in (2,3,4)


--STEP 6: Run dbt Snapshot for the second time and view updated snapshot table
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_timestamp_type2 order by order_id;

*/





