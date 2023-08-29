--command: dbt snapshot -s scd_type6

--source: 
SELECT * FROM TDUNLAP_SANDBOX_DEV.DBT_TDUNLAP.scd_source

--type 6 dimension: 
SELECT * FROM TDUNLAP_SANDBOX_DEV.snapshots.scd_type6 order by id, dbt_valid_from

