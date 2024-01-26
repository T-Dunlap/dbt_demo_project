{{
    config(
        enabled=true,
        severity='warn',
        tags = ['finance']
    )
}}


{% set dbt_relation=ref('fct_orders') %}
{% if execute %}
    {%- set old_etl_relation = adapter.get_relation(
        database="TDUNLAP_SANDBOX_DEV",
        schema="PROD_CORE",
        identifier="FCT_ORDERS_ORIGINAL") -%}
    {{ audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_key",
        summarize=false
    ) }}
{% endif %}

