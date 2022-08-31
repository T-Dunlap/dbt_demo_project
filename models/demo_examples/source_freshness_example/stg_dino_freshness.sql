{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('freshness', 'freshness_dinosaurs') }}

),

renamed as (

    select
    
        rownumber as rownumber,
        dinosaur as dinosaur,
        insert_timestamp as source_insert_timestamp

    from source

)

select * from renamed


/* --INSTRUCTIONS BELOW 

--STEP 1: Create source table if it doesn't exist 
create or replace transient table tdunlap_sandbox_sources.examples.freshness_dinosaurs (
    rownumber integer,
    dinosaur varchar (100),
    insert_timestamp TIMESTAMP_TZ(9)
);

--STEP 2: Run the "Fresh Builds" Job to show that the dependencies are not built when no new data has been added to the table. 

--STEP 3: Insert new data into the table
insert into tdunlap_sandbox_sources.examples.freshness_dinosaurs
  select 
    row_number() over (order by count(*) desc) as rownumber,
    'triceratops' as dinosaur, --velociraptor, triceratops, t-rex, pterodactyl, stegosaurus, brontosaurus
    convert_timezone('UTC', current_timestamp()) as insert_timestamp;
  commit;
  
--STEP 4: Run the "Fresh Builds" Job again to show that stg_dino_freshness view and dino_freshness table are run when the new data is added. 

--HELPER QUERIES
--Dev-----
select * from tdunlap_sandbox_sources.examples.freshness_dinosaurs
select * from tdunlap_sandbox_dev.dbt_tdunlap.stg_dino_freshness --stg view
select * from tdunlap_sandbox_dev.dbt_tdunlap.dino_freshness --final table

--Prod-----
select * from tdunlap_sandbox_sources.examples.freshness_dinosaurs
select * from tdunlap_sandbox_prod.prod.stg_dino_freshness --stg view
select * from tdunlap_sandbox_prod.prod.dino_freshness --final table
*/ 