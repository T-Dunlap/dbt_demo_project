{% snapshot mock_orders_snapshot %}

{{
    config(
      target_database= target.database,
      target_schema=target.schema,
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('mock_orders_source', 'mock_orders') }}

{% endsnapshot %}

/*

--STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
      drop table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
      drop table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders_snapshot;

      create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (
          order_id integer,
          status varchar (100),
          created_at datetime,
          updated_at datetime
      );


--STEP 2: Insert initial values into table 
      insert into tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (order_id, status, created_at, updated_at)
      values (1, 'delivered', '2020-01-01', '2020-01-04'),
             (2, 'shipped', '2020-01-02', '2020-01-04'),
             (3, 'shipped', '2020-01-03', '2020-01-04'),
             (4, 'processed', '2020-01-04', '2020-01-04');
      commit;


--STEP 3: Run dbt Snapshot for the first time and create the "_snapshot" table for the first time
      select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders_snapshot order by order_id;


--STEP 4: Update values from table
      
      update tdunlap_sandbox_dev.dbt_tdunlap.mock_orders
      set status = 'delivered'
        ,updated_at = current_timestamp(2)
      where order_id in (2,3,4)


--STEP 5: Run dbt Snapshot for the second time and view updated snapshot table
      select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders_snapshot order by order_id;

*/





