{% snapshot scd_salesforce_snapshot %}

{{
        config(
          target_schema='SALES_OPS_HC_ODS',
          strategy='check',
          unique_key="workday_unique_sk",
          updated_at='SYSTEMMODTIMESTAMP',
          check_cols=["CHECKSUM_COLUMN"],
          invalidate_hard_deletes=True
        )
}}

select * from {{ ref('scd_salesforce_source') }}

{% endsnapshot %}