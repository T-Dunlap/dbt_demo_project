with source_data as (
            select 1 as id, 'Thomas T Shelby' as name,  8797898999 as phone,    'seattle' as city,  dateadd(day,-2,current_timestamp()) as source_timestamp 
union all   select 2 as id, 'Arthur Shelby' as name,    7647575657 as phone,    'chicago' as city,  dateadd(day,-2,current_timestamp()) as source_timestamp 
union all   select 3 as id, 'John S Shelby' as name,    7786876876 as phone,    'austin' as city,   dateadd(day,-2,current_timestamp()) as source_timestamp 
--second load below
union all   select 4 as id, 'Pollie Gray' as name,      1000979888 as phone,    'chicago' as city,  dateadd(day,-1,current_timestamp()) as source_timestamp 
union all   select 5 as id, 'Michael Gray' as name,     8098080808 as phone,    'seattle' as city,  dateadd(day,-1,current_timestamp()) as source_timestamp 
--third load below
--union all   select 6 as id, 'Alfie Solomon' as name,    8080809800 as phone,    'austin' as city,   dateadd(day,0,current_timestamp()) as source_timestamp 

)
select * from source_data