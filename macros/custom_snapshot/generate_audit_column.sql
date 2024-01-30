{% macro generate_audit_column(unique_key) %}
    
      {% set sql %}
      update {{this}} 
      set {{this.identifier}}.status_audit = tmp_data.status_audit
      from (
          select dbt_scd_id,
          row_number() over (partition by {{unique_key}} order by dbt_updated_at) as _id_seq,  -- not used but helpful for visualization
          lead(dbt_valid_from, 1, null) over (partition by {{unique_key}} order by dbt_updated_at) as _next_valid_from,
          case
            -- currently active row is not deleted
            when dbt_valid_to is null 
                then 'Active'
            -- middle sequence record valid_to matches next_valid_from means consecutively updated
            when (dbt_valid_to = _next_valid_from) 
                then 'Updated'
            -- last sequenece was to delete the record
            when dbt_valid_to is not null and _next_valid_from is null 
                then 'Deleted'
            -- middle sequence record deleted, but then undeleted
            when (dbt_valid_to != _next_valid_from)
                then 'Deleted and Undeleted'
            else null
        end as status_audit
        from {{this}}
        ) tmp_data
        where {{this.identifier}}.dbt_scd_id = tmp_data.dbt_scd_id
        
        {% endset %}

    {{return(sql)}}

{% endmacro %}
