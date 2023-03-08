{% snapshot scd_orders_custom %}

{{
    config(
      target_database= target.database,
      target_schema=target.schema,
      unique_key='order_id',
      strategy= 'check',
      check_cols=['status', 'shipment_delayed_flag'],
      invalidate_hard_deletes=true,
    )
}}

select * from {{ source('snap_orders_source', 'snap_orders_custom') }}

{% endsnapshot %}

/*


--STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
      drop table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom;
      drop table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom_type2;


--STEP 2: Create RAW table that will be updated/overwritten
      create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom (
          order_id integer,
          status varchar (100),
          shipment_delayed_flag integer,
          contact_name varchar (100),
          created_at datetime,
          updated_at datetime
      );


--STEP 3: Insert initial values into table 
      insert into tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom (order_id, status, shipment_delayed_flag, contact_name, created_at, updated_at)
      values (1, 'delivered', 0, 'Stephanie', '2020-01-01', '2020-01-04'),
             (2, 'shipped', 0, 'Karl', '2020-01-02', '2020-01-04'),
             (3, 'shipped', 0, 'Vanesa', '2020-01-03', '2020-01-04'),
             (4, 'processed', 0, 'Beth','2020-01-04', '2020-01-04');
      commit;


--STEP 4: Run dbt Snapshot for the first time and create the "_type2" table for the first time
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom_type2 order by order_id;


--STEP 5: Update values from table
      update tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom
      set shipment_delayed_flag = 1
        ,updated_at = current_timestamp(2)
      where order_id in (2,3);      

      
--STEP 6: Run dbt Snapshot for the second time and view updated snapshot table
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom_type2 order by order_id;

      
--STEP 7: Update values from table that we don't care about
      update tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom
      set contact_name = 'Kevin'
        ,updated_at = current_timestamp(2)
      where order_id =2;

      update tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom
      set contact_name = 'Vanessa'
        ,updated_at = current_timestamp(2)
      where order_id =3;


--STEP 8: Run dbt Snapshot for the third time and view updated snapshot table
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom_type2 order by order_id;

      
--STEP 9: Update values from table that we DO care about
      update tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom
      set status = 'delivered'
        ,updated_at = current_timestamp(2)
      where order_id in (2,3,4);
      

--STEP 10: Run dbt Snapshot for the third time and view updated snapshot table
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom;
      --dbt snapshot;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.snap_orders_custom_type2 order by order_id;


*/





