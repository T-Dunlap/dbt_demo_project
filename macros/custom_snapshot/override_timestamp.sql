
{% macro override_timestamp(timestamp_column) -%}

    {% set query %}

        select max('timestamp_column') from {{this}}

    {% endset %}

  {% if execute == True or flags.WHICH == 'snapshot'  %}
    {% set delete_override_timestamp = run_query(query).columns[0][0] %}
  {% else %}  
    {% set delete_override_timestamp = -1 %}
  {% endif %}

  {% do return(delete_override_timestamp) %}

{% endmacro %}