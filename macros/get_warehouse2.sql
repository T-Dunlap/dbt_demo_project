{% macro get_warehouse2() %}
{% if target.name in ('ci') %}
    'transforming'
{% else %}
    'xyz'
{% endif %}
{% endmacro %}