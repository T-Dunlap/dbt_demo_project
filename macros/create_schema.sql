{% macro create_schema(relation) %}
{%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }} with managed access
  {% endcall %}
{% endmacro %}