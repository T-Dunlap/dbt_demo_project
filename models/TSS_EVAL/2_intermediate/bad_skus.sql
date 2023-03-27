with customer_deployment as (
    select * from {{ source('opal_salesforce', 'CUSTOMER_DEPLOYMENT__C') }}
)

,customer_asset as (
    select * from {{ source('opal_salesforce', 'CUSTOMER_ASSET__C') }}
)

,product as (
    select * from {{ source('opal_salesforce', 'PRODUCT2') }}
)

select 
    cd.id as na61_deployid,
    boolor_agg(iff(p.ECOMM_ELIGIBLE__C = 'False', true, false)) as is_ineligible_sku
from customer_deployment cd
join customer_asset ca on ca.CUSTOMER_DEPLOYMENT_ID__C = cd.ID
join product p on ca.PRODUCT_ID__C = p.ID
where p.ECOMM_ELIGIBLE__C = 'False'
and p.ISACTIVE = true
group by cd.id