{%- macro set_warehouse_config(warehouse_size) -%}
    {# This is a mechanism to manually override for full refreshes #}
    {%- set warehouse_override = var('warehouse_override', none) -%}
    {%- if warehouse_override is not none -%}
        {%- set warehouse_size = warehouse_override -%}
        {{ log('Overriding warehouse_size of ' ~ warehouse_size ~ ' to ' ~ warehouse_override, info = True) }}
    {%- endif -%}

    {# Sanitize args #}
    {%- set warehouse_size = warehouse_size | lower -%}

    {# Confirm args are valid #}
    {%- if warehouse_size not in var('snowflake_warehouse_sizes') -%}
        {{ exceptions.raise_compiler_error('warehouse_size value must be ' + valid_warehouse_sizes | join(', ')) }}
    {%- endif -%}

    {# Warehouse naming convention by environment (CI and dev share larger sizes) #}
    {%- if target.user in ['dbt_cloud_user', 'ci_user'] -%}
        {%- set warehouse_prefix = 'transforming_prod' -%}
    {%- else -%}
        {%- if warehouse_size in ('3xlarge', '4xlarge') -%}
            {{ exceptions.raise_compiler_error('This warehouse configuration is only available in a production environment.') }}
        {%- endif -%}
        {%- set warehouse_prefix = 'transforming' -%}
    {%- endif -%}

    {# Apply warehouse naming conventions #}
    {%- if warehouse_size == 'medium' -%}
        {{ return(warehouse_prefix) }}
    {%- else -%}
        {{ return(warehouse_prefix + '_' + warehouse_size) }}
    {%- endif -%}

{%- endmacro -%}