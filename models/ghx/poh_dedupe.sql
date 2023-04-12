with poh as (
    select *,
    {{ dbt_utils.surrogate_key('cdp_unique_key','patient_name','quantity_of_eaches','unit_price','extended_price','order_quantity','po_date','file_name','inserted_timestamp') }} as recordhash
    from {{ ref('stg_poh') }}
)

,dupes as (
    select recordhash
    from poh
    group by 1 
    having count(*) > 1
)

,dupe_flag as (
    select 
        poh.*
        ,case when dupes.recordhash is not null then 1 else 0 end as dupe_flag
    from poh 
    left join dupes on dupes.recordhash = poh.recordhash
)

,order_dupes as (
    select *
        ,row_number() over ( partition by recordhash order by recordhash ) as ranking
    from dupe_flag
)

select * from order_dupes where ranking = 1

