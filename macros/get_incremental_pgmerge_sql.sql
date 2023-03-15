{% macro get_incremental_pgmerge_sql(arg_dict) %}

  {% do return(get_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"], arg_dict["unique_key"], arg_dict["dest_columns"])) %}

{% endmacro %}