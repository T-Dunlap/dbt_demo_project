{% macro sproc__pi_multiplier() %}

    {% set stored_proc_name %}
        {{target.database}}.{{target.schema}}.sproc_pi_multiplier
    {% endset %}

    {% set table_name %}
        {{target.database}}.{{target.schema}}.sproc_pi_example
    {% endset %}
    
    {% set stored_proc_ddl_query %}
        create or replace procedure {{stored_proc_name}} (multiplier integer)
        returns varchar not null
        language sql
        as
        $$
        begin
            create table if not exists {{table_name}} as select cast(3.1415 as float) as pi_multiple, 'Seed Row' as action_type, current_timestamp() as inserted_timestamp;
        insert into {{table_name}} 
        select  cast(3.1415 * :multiplier as float) , 'Insert', current_timestamp();
        return 'Rows inserted: ' || sqlrowcount;
        end;
        $$
        ; 


    {% endset %}

    {% do run_query(stored_proc_ddl_query) %}
    {% do log('Created stored proc:\t' ~ stored_proc_name, True) %}

    {# return the name of the stored procedure #}
    {%- do return(stored_proc_name) %}
    
{% endmacro %}