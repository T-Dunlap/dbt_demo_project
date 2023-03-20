{# A basic example for a project-wide macro to cast a column uniformly #}

{% macro cents_to_dollars(column_name, precision=4) -%}
    ({{ column_name }} / 1000)::numeric(16, {{ precision }})
{%- endmacro %}
