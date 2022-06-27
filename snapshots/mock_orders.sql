{# --remove code block, save, then run 

{% snapshot mock_orders %}

{% set new_schema = target.schema + '_snapshot' %}

{{
    config(
      target_database='tdunlap_sandbox_dev',
      target_schema=new_schema,
      unique_key='order_id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from tdunlap_sandbox_dev.{{target.schema}}.mock_orders

{% endsnapshot %}


--STEP 1: Drop existing tables and create net new table on tdunlap_sandbox_dev
/*
drop table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
drop table tdunlap_sandbox_dev.dbt_tdunlap_snapshot.mock_orders;
create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);
*/

--STEP 2: Insert initial values into table 
/*
insert into analytics.dbt_kcoapman.mock_orders (order_id, status, created_at, updated_at)
values (1, 'delivered', '2020-01-01', '2020-01-04'),
       (2, 'shipped', '2020-01-02', '2020-01-04'),
       (3, 'shipped', '2020-01-03', '2020-01-04'),
       (4, 'processed', '2020-01-04', '2020-01-04');
commit;
*/

--STEP 3: Run dbt Snapshot for the first time and create the "_snapshot" table for the first time
--select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
--dbt snapshot;
--select * from tdunlap_sandbox_dev.dbt_tdunlap_snapshot.mock_orders order by order_id;

--STEP 4: Recreate table and insert net new values
/*
create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);
insert into tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (order_id, status, created_at, updated_at)
values (1, 'delivered', '2020-01-01', '2020-01-05'),
       (2, 'delivered', '2020-01-02', '2020-01-05'),
       (3, 'delivered', '2020-01-03', '2020-01-05'),
       (4, 'delivered', '2020-01-04', '2020-01-05');
commit;
*/

--STEP 5: Run dbt Snapshot for the second time and view updated snapshot table
--select * from tdunlap_sandbox_dev.dbt_tdunlap.mock_orders;
--dbt snapshot;
--select * from tdunlap_sandbox_dev.dbt_tdunlap_snapshot.mock_orders order by order_id;

#} --remove code block, save, then run 




