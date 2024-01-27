{% macro get_warehouse2(transforming) %}
{% if target.name in ('ci') %}
    xyz
{% else %}
    transforming
{% endif %}
{% endmacro %}