{% macro get_warehouse(transforming, wh2) %}  
  {% set wh = transforming %}
{# The if test below defaults to False if use_wh2 is undefined #}
  {% if var('use_wh2', False) %}
     {% set wh = wh2 %}
  {% endif %}

  {{return(wh)}}  
{% endmacro %}
 