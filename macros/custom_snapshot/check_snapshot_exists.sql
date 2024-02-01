{% macro check_snapshot_exists(snapshot) %}
    
    {% if execute %}
        {% set result = False %}

        {% set check_table_names_in_target_schema_sql %}
           show terse objects like '{{snapshot.name}}' in schema {{snapshot.database}}.{{snapshot.schema}} limit 1;
        {% endset %}
        {% set table_name_count = run_query(check_table_names_in_target_schema_sql).rows | length %}
    
        {% set log_message %}
           'table_name_count: ' ~ {{table_name_count}}
        {% endset %}
    
        {{log(log_message, info=true)}}

        {% if table_name_count == 1 %}
          {% set result = True %}
        {% endif %}

        {{return(result)}}

    {% endif %}

{% endmacro %}