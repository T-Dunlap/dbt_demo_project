
{% macro update_valid_snapshot_records(from_relation='', unique_key='', prefix='', update_cols=[], only_update_most_recent_record=true) %}

    {% call statement() %}
    create or replace view {{ this }}__dbt_tmp as
        select 
            {{ unique_key }}, 
            {%- for column in update_cols %}{{ column }}{% if not loop.last %}, {% endif %}{% endfor %}
        from {{ from_relation }}
    ;
    {% endcall %}

    {% call statement() %}
    merge into {{ this }} as DBT_INTERNAL_DEST
        using {{ this }}__dbt_tmp as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {%- if only_update_most_recent_record %}
        and DBT_INTERNAL_DEST.dbt_valid_to is null 
        {%- endif %}
    when matched then update set
        {%- for column in update_cols %}
        DBT_INTERNAL_DEST.{{ prefix}}{{ column }} = DBT_INTERNAL_SOURCE.{{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    ;
    {% endcall %}

    
{% endmacro %}