
{% macro override_timestamp(timestamp_column) -%}
    {% set query %}

        select '2999-01-01 00:00:00.001'

    {% endset %}

    {% set results = run_query(query) %}
    {# execute is a Jinja variable that returns True when dbt is in "execute" mode i.e. True when running dbt run but False during dbt compile. #}

    {% if execute %}
    {% set results_list = results.columns[0].values() %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}
{% endmacro %}