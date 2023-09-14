{% snapshot scd_test %}
    {{ config(
            target_schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols='all'
    ) }}
    

    select * 
    from {{ ref('scd_source') }} 

    
{% endsnapshot %}