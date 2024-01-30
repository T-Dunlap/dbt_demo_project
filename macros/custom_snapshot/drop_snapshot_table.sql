{% macro drop_snapshot_table(schema, table) %}
   
   {% set sql %}
     drop table {{target.database}}.{{schema}}.{{table}};
   {% endset %}

   {% do run_query(sql) %}

{% endmacro %}