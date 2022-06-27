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


--create or replace transient table tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (
--    order_id integer,
--    status varchar (100),
--    created_at date,
--    updated_at date
--);
--insert into tdunlap_sandbox_dev.dbt_tdunlap.mock_orders (order_id, status, created_at, updated_at)
--values (1, 'delivered', '2020-01-01', '2020-01-05'),
--       (2, 'delivered', '2020-01-02', '2020-01-05'),
--       (3, 'delivered', '2020-01-03', '2020-01-05'),
--       (4, 'delivered', '2020-01-04', '2020-01-05');
--commit;




