select
  id, 
  {{ dbt_utils.pivot(
      'name',
      dbt_utils.get_column_values(table=source('pivot', 'pivot_orders_source'), column='name'),
      agg = 'max',
      cmp = '=',
      then_value = 'Value', 
      else_value = 'NULL'
  ) }}
from {{ source('pivot', 'pivot_orders_source') }}
group by id 

--select * from {{ source('pivot', 'pivot_orders_source') }}

/*
--STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
      drop table tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders_source;
      drop table tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders;


--STEP 2: Create RAW table that will be updated/overwritten
      create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders_source (
          id integer,
          name varchar (100),
          value varchar (100) 
      );


--STEP 3: Insert initial values into table 
      insert into tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders_source (id, name, value)
      values (1, 'order_id', '1'),
            (2, 'order_id', '2'),
            (3, 'order_id', '3'),
            (4, 'order_id', '4'),
            (1, 'status', 'delivered'),
            (2, 'status', 'shipped'),
            (3, 'status', 'returned'),
            (4, 'status', 'processed'),
            (1, 'contact_name', 'Stephanie'),
            (2, 'contact_name', 'Karl'),
            (3, 'contact_name', 'Vanessa'),
            (4, 'contact_name', 'Beth'),
            (1, 'created_at', '2020-01-01'),
            (2, 'created_at', '2020-01-02'),
            (3, 'created_at', '2020-01-03'),
            (4, 'created_at', '2020-01-04'),
            (1, 'updated_at', '2020-01-23'),
            (2, 'updated_at', '2020-01-23'),
            (3, 'updated_at', '2020-01-23'),
            (4, 'updated_at', '2020-01-23');
      commit;

--STEP 4: Insert initial values into table 

      select * from tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders_source;
      select * from tdunlap_sandbox_dev.dbt_tdunlap.pivot_orders;
*/