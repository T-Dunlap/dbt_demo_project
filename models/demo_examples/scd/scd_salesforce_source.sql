with source_data as (
select 'PID-2015' as workday_unique_sk, 1  as CHECKSUM_COLUMN, '2024-01-19 05:25:47.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2016' as workday_unique_sk, 2  as CHECKSUM_COLUMN, '2024-01-19 13:52:21.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2017' as workday_unique_sk, 3  as CHECKSUM_COLUMN, '2024-01-19 17:00:45.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2018' as workday_unique_sk, 4  as CHECKSUM_COLUMN, '2024-01-19 14:35:10.000' as SYSTEMMODTIMESTAMP union all
--select 'PID-2019' as workday_unique_sk, 5  as CHECKSUM_COLUMN, '2024-01-19 03:33:51.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2020' as workday_unique_sk, 6  as CHECKSUM_COLUMN, '2024-01-19 00:36:35.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2021' as workday_unique_sk, 7  as CHECKSUM_COLUMN, '2024-01-19 09:13:21.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2022' as workday_unique_sk, 8  as CHECKSUM_COLUMN, '2024-01-19 10:07:04.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2023' as workday_unique_sk, 9  as CHECKSUM_COLUMN, '2024-01-19 05:11:22.000' as SYSTEMMODTIMESTAMP union all
select 'PID-2024' as workday_unique_sk, 10 as CHECKSUM_COLUMN, '2024-01-19 19:02:00.000' as SYSTEMMODTIMESTAMP 
)

select * from source_data 