{% snapshot scd_type6 %}
    {{ config(
            target_schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols=['name', 'phone'],
            post_hook=["{{ update_valid_snapshot_records(
                from_relation=ref('scd_source'), 
                unique_key='id', 
                update_cols=['name','phone'],
                only_update_most_recent_record=false
            ) }}"] 
    ) }}
    

    select * 
        ,null as current_name
        ,null as current_phone
    from {{ ref('scd_source') }} 

    
{% endsnapshot %}