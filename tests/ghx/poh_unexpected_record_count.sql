{{
    config(
        warn_if = '<2' 
        ,error_if = '>2' 
    )
}}

with poh as (
    select * from {{ ref('stg_poh') }}
) 

,files as (
    select file_name, count(*) rowcounts
    from poh
    group by file_name
    having count(*) < 2 --could insert a query that shows the expected number of rows
)

select file_name, rowcounts from files