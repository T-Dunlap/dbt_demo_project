truncate table SALES_OPS_DSDE_ODS.W_DIM_ECOMM_ELIGIBILITY;
    insert into ${target_schema}.W_DIM_ECOMM_ELIGIBILITY
    with bad_skus as
                (
                select cd.id as na61_deployid,
                boolor_agg(iff(p.ECOMM_ELIGIBLE__C = 'False', true, false)) as is_ineligible_sku
                from OPAL.SALESFORCE.CUSTOMER_DEPLOYMENT__C cd
                join OPAL.SALESFORCE.CUSTOMER_ASSET__C ca on ca.CUSTOMER_DEPLOYMENT_ID__C = cd.ID
                join OPAL.SALESFORCE.PRODUCT2 p on ca.PRODUCT_ID__C = p.ID
                where p.ECOMM_ELIGIBLE__C = 'False'
                and p.ISACTIVE = true
                group by cd.id
            ),
            -- booland, will return true only if all rows are true for the group.
            bad_contract as
            (
                select cd.ID                                                                 as na61_deployid,
                booland_agg(iff(c.ENDDATE < current_date, true, false))               as is_contract_expired,
                booland_agg(iff(c.CHANNEL_SALES_PARTNER__C is not null, true, false)) as is_partner_contract,
                booland_agg(iff(c.CURRENCYISOCODE = 'BRL', true, false))              as is_brazil_contract
                from EDW_PRD_ORG62_OPAL.ODS.ORDERITEM_SV oi
                join EDW_PRD_ORG62_OPAL.ODS.TENANT_SV t on oi.PROVISIONINGTARGETID = t.ID
                join OPAL.SALESFORCE.CUSTOMER_DEPLOYMENT__C cd on t.EXTERNALID = cd.ID
                join EDW_PRD_ORG62_OPAL.ODS.ORDER_SV o on oi.ORDERID = o.ID
                join EDW_PRD_ORG62_OPAL.ODS.CONTRACT_SV c on o.CONTRACTID = c.id
                group by cd.id
            ),
            -- Grain is deployment id
            tmp as (
            select
            null as id,
            cd.id                                                                     as NA61_DEPLOYMENT_ID,
            cd.PORTAL_ACCOUNT_ID_C                                                   as NA61_PORTAL_ACCOUNT_ID,
            cd.IS_DELETED                                                            as NA61_DEPLOYMENT_ID_IS_DELETED,
            cd.SERVER_MGMT_ENABLED_C,
            cd.DATA_MNGMENT_ENABLED_C,
            ifnull(bad_skus.is_ineligible_sku, false)                                 as IS_INELIGIBLE_SKU,
            ifnull(bad_contract.is_contract_expired, false)                           as IS_CONTRACT_EXPIRED,
            ifnull(bad_contract.is_partner_contract, false)                           as IS_PARTNER_CONTRACT,
            ifnull(bad_contract.is_brazil_contract, false)                            as IS_BRAZIL_CONTRACT,
            sa.ACCOUNT_DEFAULTING_EXCEPTION__C,
            sa.CREDIT_CHECK__C,
            sha2(
                to_char(object_construct(
                'NA61_PORTAL_ACCOUNT_ID', ifnull(cd.PORTAL_ACCOUNT_ID_C,'No Portal Account'),
                'NA61_DEPLOYMENT_ID_IS_DELETED', ifnull(cd.IS_DELETED,false),
                'SERVER_MGMT_ENABLED__C', ifnull(cd.SERVER_MGMT_ENABLED_C,false),
                'DATA_MNGMENT_ENABLED__C', ifnull(cd.DATA_MNGMENT_ENABLED_C,false),
                'IS_INELIGIBLE_SKU', ifnull(bad_skus.is_ineligible_sku, false),
                'IS_CONTRACT_EXPIRED', ifnull(bad_contract.is_contract_expired, false),
                'IS_PARTNER_CONTRACT', ifnull(bad_contract.is_partner_contract, false),
                'IS_BRAZIL_CONTRACT', ifnull(bad_contract.is_brazil_contract, false),
                'ACCOUNT_DEFAULTING_EXCEPTION__C', ifnull(sa.ACCOUNT_DEFAULTING_EXCEPTION__C,''),
                'CREDIT_CHECK__C', ifnull(sa.CREDIT_CHECK__C,'')))
            ) as CHECKSUM,
            current_timestamp as REC_START_DTTM,
            null as REC_END_DTTM,
            true as CURR_REC_IND
            from OPAL.ODS_FT_SALESFORCE_P1_BASE.CUSTOMER_DEPLOYMENT_C cd
            left join OPAL.SALESFORCE.ACCOUNT pa on pa.id = cd.PORTAL_ACCOUNT_ID_C
            left join OPAL.SALESFORCE.ACCOUNT a on pa.PARENTID = a.ID
            left join EDW_PRD_ORG62_OPAL.ODS.ACCOUNT_SV sa on a.ORG62_ACCOUNT_ID__C = sa.ID
            left join bad_skus on cd.id = bad_skus.na61_deployid
            left join bad_contract on cd.id = bad_contract.na61_deployid
            )
            SELECT tmp.*,
            CASE WHEN tmp.CHECKSUM != ifnull(tgt.CHECKSUM,'No Checksum') and tgt.CHECKSUM is not null then 'Update'
                 WHEN tmp.CHECKSUM != ifnull(tgt.CHECKSUM, 'No Checksum') and tgt.CHECKSUM is null then 'Insert'
                 ELSE 'No Change'
                 END as MERGE_ACTION
            FROM tmp
            LEFT JOIN SALES_OPS_DSDE_CDL.DIM_ECOMM_ELIGIBILITY tgt on tmp.NA61_DEPLOYMENT_ID = tgt.NA61_DEPLOYMENT_ID  and tgt.CURR_REC_IND = true;