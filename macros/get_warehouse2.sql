{%- macro get_warehouse2() -%}

    {# Warehouse naming convention by environment #}
    {%- if target.name in ['ci'] -%}
        {%- set warehouse_name = 'ci_warehouse' -%}
    {%- else -%}
        {%- set warehouse_name = 'transforming' -%}
    {%- endif -%}

    {{ return(warehouse_name) }}


{%- endmacro -%}