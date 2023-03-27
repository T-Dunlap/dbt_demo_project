set effective_date = current_timestamp;
  merge into ${target_schema}.DIM_ECOMM_ELIGIBILITY as tgt using (
  with base_cte as (select id,
                      na61_deployment_id,
                      na61_portal_account_id,
                      na61_deployment_id_is_deleted,
                      server_mgmt_enabled__c,
                      data_mngment_enabled__c,
                      is_ineligible_sku,
                      is_contract_expired,
                      is_partner_contract,
                      is_brazil_contract,
                      account_defaulting_exception,
                      credit_check__c,
                      checksum,
                      rec_start_dttm,
                      rec_end_dttm,
                      curr_rec_ind,
                      merge_action --VAR_INSERT_COL_LIST
                      from SALES_OPS_DSDE_ODS.W_DIM_ECOMM_ELIGIBILITY
                      where merge_action in ('Update', 'Insert')
                      ),
  insert_cte as (select *, false as update_flag
                from base_cte),
  update_cte as (select *, true as update_flag
                        from base_cte
                        where base_cte.merge_action = 'Update'
                        )
  select *
  from insert_cte
  union all
  select *
  from update_cte
  ) as src
  on src.NA61_DEPLOYMENT_ID = tgt.NA61_DEPLOYMENT_ID and src.update_flag = true and tgt.curr_rec_ind = true
  WHEN MATCHED THEN UPDATE SET REC_END_DTTM = $effective_date, CURR_REC_IND = false
  WHEN NOT MATCHED THEN INSERT (
  id,
  na61_deployment_id,
  na61_portal_account_id,
  na61_deployment_id_is_deleted,
  server_mgmt_enabled__c,
  data_mngment_enabled__c,
  is_ineligible_sku,
  is_contract_expired,
  is_partner_contract,
  is_brazil_contract,
  account_defaulting_exception,
  credit_check__c,
  checksum,
  rec_start_dttm,
  rec_end_dttm,
  curr_rec_ind --exclude change flag
  )
  values (
  SALES_OPS_DSDE_CDL.SEQ_DIM_ECOMM_ELIGIBILITY.nextval,
  src.na61_deployment_id,
  src.na61_portal_account_id,
  src.na61_deployment_id_is_deleted,
  src.server_mgmt_enabled__c,
  src.data_mngment_enabled__c,
  src.is_ineligible_sku,
  src.is_contract_expired,
  src.is_partner_contract,
  src.is_brazil_contract,
  src.account_defaulting_exception,
  src.credit_check__c,
  src.checksum,
  $effective_date,
  to_timestamp_ltz('9999-12-31 00:00:00.000000000'),
  true
  );