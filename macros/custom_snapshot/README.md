

### How to run:

#### Refreshing state:

There is a macro to remove the target snapshot table when you want: `dbt run-operation drop_snapshot_table --args '{"schema":"dbt_mwinkler", "table":"custom_snapshot"}'`

#### Running the snapshot process:

1) `dbt build -s custom_snapshot`
2) `dbt build -s custom_snapshot --vars 'initial_delete: True'` # simulates the drop of the record
3) `dbt build -s custom_snapshot --vars 'returned_record: True'` # when the zombie record returns