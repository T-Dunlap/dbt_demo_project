CREATE OR REPLACE TABLE ${target_schema}.ORG62_TAB_ACCOUNT_OWNERSHIP
        (
            ID,
            TAB_AE_TERRITORY_ID,
            TAB_AE_TERRITORY_NAME,
            TAB_AE_USER_ID,
            TAB_AE_USER_NAME,
            TAB_AE_TEAM_ROLE_NAME,
            TAB_AE_MANAGER_TERRITORY_ID,
            TAB_AE_MANAGER_TERRITORY_NAME,
            TAB_AE_MANAGER_USER_ID,
            TAB_AE_MANAGER_USER_NAME,
            TAB_AE_MANAGER2_TERRITORY_ID,
            TAB_AE_MANAGER2_TERRITORY_NAME,
            TAB_AE_MANAGER2_USER_ID,
            TAB_AE_MANAGER2_USER_NAME,
            TAB_AE_MANAGER3_TERRITORY_ID,
            TAB_AE_MANAGER3_TERRITORY_NAME,
            TAB_AE_MANAGER3_USER_ID,
            TAB_AE_MANAGER3_USER_NAME,
            TAB_AE_MANAGER4_TERRITORY_ID,
            TAB_AE_MANAGER4_TERRITORY_NAME,
            TAB_AE_MANAGER4_USER_ID,
            TAB_AE_MANAGER4_USER_NAME,
            TAB_AE_MANAGER5_TERRITORY_ID,
            TAB_AE_MANAGER5_TERRITORY_NAME,
            TAB_AE_MANAGER5_USER_ID,
            TAB_AE_MANAGER5_USER_NAME,
            TAB_ECS_TERRITORY_ID,
            TAB_ECS_TERRITORY_NAME,
            TAB_ECS_USER_ID,
            TAB_ECS_USER_NAME,
            TAB_ECS_TEAM_ROLE_NAME,
            TAB_CRM_TERRITORY_ID,
            TAB_CRM_TERRITORY_NAME,
            TAB_CRM_USER_ID,
            TAB_CRM_USER_NAME,
            TAB_CRM_TEAM_ROLE_NAME,
            TAB_GCM_TERRITORY_ID,
            TAB_GCM_TERRITORY_NAME,
            TAB_GCM_USER_ID,
            TAB_GCM_USER_NAME,
            TAB_GCM_TEAM_ROLE_NAME,
            TAB_DMSM_TERRITORY_ID,
            TAB_DMSM_TERRITORY_NAME,
            TAB_DMSM_USER_ID,
            TAB_DMSM_USER_NAME,
            TAB_DMSM_TEAM_ROLE_NAME,
            TAB_EMBED_TERRITORY_ID,
            TAB_EMBED_TERRITORY_NAME,
            TAB_EMBED_USER_ID,
            TAB_EMBED_USER_NAME,
            TAB_EMBED_TEAM_ROLE_NAME,
            TAB_ONLINE_TERRITORY_ID,
            TAB_ONLINE_TERRITORY_NAME,
            TAB_ONLINE_USER_ID,
            TAB_ONLINE_USER_NAME,
            TAB_ONLINE_TEAM_ROLE_NAME,
            TAB_BDR_TERRITORY_ID,
            TAB_BDR_TERRITORY_NAME,
            TAB_BDR_USER_ID,
            TAB_BDR_USER_NAME,
            TAB_BDR_TEAM_ROLE_NAME,
            TAB_CSG_TERRITORY_ID,
            TAB_CSG_TERRITORY_NAME,
            TAB_CSG_USER_ID,
            TAB_CSG_USER_NAME,
            TAB_CSG_TEAM_ROLE_NAME,
            TAB_CSG_BS_TERRITORY_ID,
            TAB_CSG_BS_TERRITORY_NAME,
            TAB_CSG_BS_USER_ID,
            TAB_CSG_BS_USER_NAME,
            TAB_CSG_BS_TEAM_ROLE_NAME,
            TAB_CSG_RM_TERRITORY_ID,
            TAB_CSG_RM_TERRITORY_NAME,
            TAB_CSG_RM_USER_ID,
            TAB_CSG_RM_USER_NAME,
            TAB_CSG_RM_TEAM_ROLE_NAME,
            TAB_CSG_SM_TERRITORY_ID,
            TAB_CSG_SM_TERRITORY_NAME,
            TAB_CSG_SM_USER_ID,
            TAB_CSG_SM_USER_NAME,
            TAB_CSG_SM_TEAM_ROLE_NAME,
            CORE_TERRITORY_ID,
            CORE_TERRITORY_NAME,
            CORE_USER_ID,
            CORE_USER_NAME,
            CORE_TEAM_ROLE_NAME
        )
        as
        (
        with salesforce_team
        as
            (
                select sft.team_role_name__c,
                    sft.SFBASE__ACCOUNT__C,
                    sft.TERRITORY__C,
            --         6/29/2022 implementing partition in cte as logic to select correct core ae must be implemented on the
            --         joined object. if account territory = sf team territory than that takes precedence for core
            --         over other sf team record, otherwise take earliest sf team record -tl
                    row_number()
                        over (PARTITION by sft.SFBASE__ACCOUNT__C, sft.TEAM_ROLE_NAME__C
                            ORDER BY iff(a.TEAM_TERRITORY__C = sft.TERRITORY__C,1,2), sft.CREATEDDATE desc) as sft_rank
                    --amt.lastmodtimestamp
                from EDW_PRD_ORG62_OPAL.ODS.SFBASE__SALESFORCETEAM__C_SV sft
                left join EDW_PRD_ORG62_OPAL.ODS.ACCOUNT_SV a on sft.SFBASE__ACCOUNT__C = a.ID
                    -- left join EDW_PRD_ORG62_OPAL.ODS.TERRITORY__C_SV t
                    -- on t.ID = amt.TERRITORY__C
                where
                    -- using team role name instead of operating unit because operating unit does not appear in the ods object
                    sft.TEAM_ROLE_NAME__C IN (  'Tableau AE',
                                                'Tableau ECS',
                                                'Tableau CRM',
                                                'Tableau Global Client Director',
                                                'Tableau DM-SM AE',
                                                'Tableau Embedded AE',
                                                'Tableau Online AE',
                                                'Tableau BDR',
                                                'Tableau CSG Account Partner',
                                                'Tableau CSG Business Mgr',
                                                'Tableau CSG Renewals Mgr',
                                                'Tableau CSG Success Mgr',
                                                'Salesperson')
                    and sft.isdeleted = 'N'
                    and sft.SFBASE__STARTDATE__C <= current_date
                    and (sft.SFBASE__ENDDATE__C >= current_date or sft.SFBASE__ENDDATE__C IS NULL)
                    and sft.territory__c is not null -- Added on 6/27 - to avoid duplication
        ),
        territory
        as
        (
            select
                t.ID as sfid,
                t.NAME as name,
                t.REGION__C,
                t.PARENT_TERRITORY__C,
                tr.territory_userid__c,
                tr.TERRITORY_USER_NAME__C
                --            territory_userid__c,
                --            territory_user_name__c
            from
                EDW_PRD_ORG62_OPAL.ODS.TERRITORY__C_SV t
                left join EDW_PRD_ORG62_OPAL.ODS.TERRITORY_RESOURCES__C_SV tr on tr.TERRITORY__C = t.ID
                and tr. ISDELETED = 'N' 
                and tr.START_DATE__C <= CURRENT_DATE
                and (
                    tr.END_DATE__C >= current_date
                    or tr.END_DATE__C is null
                )
            where t.START_DATE__C <= current_date
            and (t.END_DATE__C >= current_date or t.END_DATE__C is null)
        ),
        main
        as
        (
            select
                --core routing data
                am.ID,
                -- TABLEAU OWNERSHIP
                tab_ae_t.sfid                       as tab_ae_territory_id,
                tab_ae_t.name                       as tab_ae_territory_name,
                tab_ae_t.territory_userid__c          as tab_ae_user_id,
                tab_ae_t.territory_user_name__c     as tab_ae_user_name,
                tab_ae.team_role_name__c            as tab_ae_team_role_name,


                tab_manager.sfid                    as tab_ae_manager_territory_id,
                tab_manager.name                    as tab_ae_manager_territory_name,
                tab_manager.territory_userid__c       as tab_ae_manager_user_id,
                tab_manager.territory_user_name__c  as tab_ae_manager_user_name,

                tab_manager2.sfid                   as tab_ae_manager2_territory_id,
                tab_manager2.name                   as tab_ae_manager2_territory_name,
                tab_manager2.territory_userid__c      as tab_ae_manager2_user_id,
                tab_manager2.territory_user_name__c as tab_ae_manager2_user_name,

                tab_manager3.sfid                   as tab_ae_manager3_territory_id,
                tab_manager3.name                   as tab_ae_manager3_territory_name,
                tab_manager3.territory_userid__c      as tab_ae_manager3_user_id,
                tab_manager3.territory_user_name__c as tab_ae_manager3_user_name,

                tab_manager4.sfid                   as tab_ae_manager4_territory_id,
                tab_manager4.name                   as tab_ae_manager4_territory_name,
                tab_manager4.territory_userid__c      as tab_ae_manager4_user_id,
                tab_manager4.territory_user_name__c as tab_ae_manager4_user_name,

                tab_manager5.sfid                   as tab_ae_manager5_territory_id,
                tab_manager5.name                   as tab_ae_manager5_territory_name,
                tab_manager5.territory_userid__c      as tab_ae_manager5_user_id,
                tab_manager5.territory_user_name__c as tab_ae_manager5_user_name,

                tab_ecs_t.sfid                      as tab_ecs_territory_id,
                tab_ecs_t.name                      as tab_ecs_territory_name,
                tab_ecs_t.territory_userid__c         as tab_ecs_user_id,
                tab_ecs_t.territory_user_name__c    as tab_ecs_user_name,
                tab_ecs.team_role_name__c           as tab_ecs_team_role_name,


                tab_crm_t.sfid                      as tab_crm_territory_id,
                tab_crm_t.name                      as tab_crm_territory_name,
                tab_crm_t.territory_userid__c         as tab_crm_user_id,
                tab_crm_t.territory_user_name__c    as tab_crm_user_name,
                tab_crm.team_role_name__c           as tab_crm_team_role_name,


                tab_gcm_t.sfid                      as tab_gcm_territory_id,
                tab_gcm_t.name                      as tab_gcm_territory_name,
                tab_gcm_t.territory_userid__c         as tab_gcm_user_id,
                tab_gcm_t.territory_user_name__c    as tab_gcm_user_name,
                tab_gcm.team_role_name__c           as tab_gcm_team_role_name,

                tab_dmsm_t.sfid                     as tab_dmsm_territory_id,
                tab_dmsm_t.name                     as tab_dmsm_territory_name,
                tab_dmsm_t.territory_userid__c        as tab_dmsm_user_id,
                tab_dmsm_t.territory_user_name__c   as tab_dmsm_user_name,
                tab_dmsm.team_role_name__c          as tab_dmsm_team_role_name,

                tab_embed_t.sfid                    as tab_embed_territory_id,
                tab_embed_t.name                    as tab_embed_territory_name,
                tab_embed_t.territory_userid__c       as tab_embed_user_id,
                tab_embed_t.territory_user_name__c  as tab_embed_user_name,
                tab_embed.team_role_name__c         as tab_embed_team_role_name,

                tab_online_t.sfid                   as tab_online_territory_id,
                tab_online_t.name                   as tab_online_territory_name,
                tab_online_t.territory_userid__c      as tab_online_user_id,
                tab_online_t.territory_user_name__c as tab_online_user_name,
                tab_online.team_role_name__c        as tab_online_team_role_name,

                tab_bdr_t.sfid                      as tab_bdr_territory_id,
                tab_bdr_t.name                      as tab_bdr_territory_name,
                tab_bdr_t.territory_userid__c         as tab_bdr_user_id,
                tab_bdr_t.territory_user_name__c    as tab_bdr_user_name,
                tab_bdr.team_role_name__c           as tab_bdr_team_role_name,

                tab_csg_ap_t.sfid                   as tab_csg_territory_id,
                tab_csg_ap_t.name                   as tab_csg_territory_name,
                tab_csg_ap_t.territory_userid__c      as tab_csg_user_id,
                tab_csg_ap_t.territory_user_name__c as tab_csg_user_name,
                tab_csg_ap.team_role_name__c        as tab_csg_ap_team_role_name,

                tab_csg_bs_t.sfid                   as tab_csg_bs_territory_id,
                tab_csg_bs_t.name                   as tab_csg_bs_territory_name,
                tab_csg_bs_t.territory_userid__c      as tab_csg_bs_user_id,
                tab_csg_bs_t.territory_user_name__c as tab_csg_bs_user_name,
                tab_csg_bs.team_role_name__c        as tab_csg_bs_team_role_name,

                tab_csg_rm_t.sfid                   as tab_csg_rm_territory_id,
                tab_csg_rm_t.name                   as tab_csg_rm_territory_name,
                tab_csg_rm_t.territory_userid__c      as tab_csg_rm_user_id,
                tab_csg_rm_t.territory_user_name__c as tab_csg_rm_user_name,
                tab_csg_rm.team_role_name__c        as tab_csg_rm_team_role_name,

                tab_csg_sm_t.sfid                   as tab_csg_sm_territory_id,
                tab_csg_sm_t.name                   as tab_csg_sm_territory_name,
                tab_csg_sm_t.territory_userid__c      as tab_csg_sm_user_id,
                tab_csg_sm_t.territory_user_name__c as tab_csg_sm_user_name,
                tab_csg_sm.team_role_name__c           as tab_csg_sm_team_role_name,

                core_t.sfid                         as core_territory_id,
                core_t.name                         as core_territory_name,
                core_t.territory_userid__c            as core_user_id,
                core_t.TERRITORY_USER_NAME__C       as core_user_name,
                core.team_role_name__c              as core_team_role_name,

                COUNT(*) over (PARTITION by am.ID ORDER BY am.ID) as occurence_count


            from EDW_PRD_ORG62_OPAL.ODS.ACCOUNT_SV am
                left join EDW_PRD_ORG62_OPAL.ODS.RECORDTYPE_SV rt on am.RECORDTYPEID = rt.ID
                left join salesforce_team tab_ae
                        on am.ID = tab_ae.SFBASE__ACCOUNT__C and tab_ae.team_role_name__c = 'Tableau AE'
                        and tab_ae.sft_rank = 1
                left join territory tab_ae_t on tab_ae.TERRITORY__C = tab_ae_t.sfid
                left join territory tab_manager on tab_ae_t.parent_territory__c = tab_manager.sfid
                left join territory tab_manager2 on tab_manager.parent_territory__c = tab_manager2.sfid
                left join territory tab_manager3 on tab_manager2.parent_territory__c = tab_manager3.sfid
                left join territory tab_manager4 on tab_manager3.parent_territory__c = tab_manager4.sfid
                left join territory tab_manager5 on tab_manager4.parent_territory__c = tab_manager5.sfid

                left join salesforce_team tab_ecs
                        on am.ID = tab_ecs.SFBASE__ACCOUNT__C and tab_ecs.team_role_name__c = 'Tableau ECS'
                        and tab_ecs.sft_rank = 1
                left join territory tab_ecs_t on tab_ecs.TERRITORY__C = tab_ecs_t.sfid

                left join salesforce_team tab_crm
                        on am.ID = tab_crm.SFBASE__ACCOUNT__C and tab_crm.team_role_name__c = 'Tableau CRM'
                        and tab_crm.sft_rank = 1
                left join territory tab_crm_t on tab_crm.TERRITORY__C = tab_crm_t.sfid

                left join salesforce_team tab_gcm on am.ID = tab_gcm.SFBASE__ACCOUNT__C and
                                                        tab_gcm.team_role_name__c = 'Tableau Global Client Director'
                                                        and tab_gcm.sft_rank = 1
                left join territory tab_gcm_t on tab_gcm.TERRITORY__C = tab_gcm_t.sfid

                left join salesforce_team tab_dmsm
                            on am.ID = tab_dmsm.SFBASE__ACCOUNT__C and tab_dmsm.team_role_name__c = 'Tableau DM-SM AE'
                                    and tab_dmsm.sft_rank =1
                left join territory tab_dmsm_t on tab_dmsm.TERRITORY__C = tab_dmsm_t.sfid

                left join salesforce_team tab_embed
                            on am.ID = tab_embed.SFBASE__ACCOUNT__C and tab_embed.team_role_name__c = 'Tableau Embedded AE'
                                    and tab_embed.sft_rank = 1
                left join territory tab_embed_t on tab_embed.TERRITORY__C = tab_embed_t.sfid

                left join salesforce_team tab_online
                            on am.ID = tab_online.SFBASE__ACCOUNT__C and tab_online.team_role_name__c = 'Tableau Online AE'
                                    and tab_online.sft_rank = 1
                left join territory tab_online_t on tab_online.TERRITORY__C = tab_online_t.sfid

                left join salesforce_team tab_bdr
                            on am.ID = tab_bdr.SFBASE__ACCOUNT__C and tab_bdr.team_role_name__c = 'Tableau BDR'
                                    and tab_bdr.sft_rank = 1
                left join territory tab_bdr_t on tab_bdr.TERRITORY__C = tab_bdr_t.sfid

                left join salesforce_team tab_csg_ap on am.ID = tab_csg_ap.SFBASE__ACCOUNT__C and
                                                            tab_csg_ap.team_role_name__c = 'Tableau CSG Account Partner'
                                                            and tab_csg_ap.sft_rank = 1
                left join territory tab_csg_ap_t on tab_csg_ap.TERRITORY__C = tab_csg_ap_t.sfid

                left join salesforce_team tab_csg_bs on am.ID = tab_csg_bs.SFBASE__ACCOUNT__C and
                                                            tab_csg_bs.team_role_name__c = 'Tableau CSG Business Mgr'
                                                            and tab_csg_bs.sft_rank = 1
                left join territory tab_csg_bs_t on tab_csg_bs.TERRITORY__C = tab_csg_bs_t.sfid

                left join salesforce_team tab_csg_rm ON am.ID = tab_csg_rm.SFBASE__ACCOUNT__C and
                                                        tab_csg_rm.team_role_name__c = 'Tableau CSG Renewals Mgr'
                                                            and tab_csg_rm.sft_rank = 1
                left join territory tab_csg_rm_t on tab_csg_rm.TERRITORY__C = tab_csg_rm_t.sfid

                left join salesforce_team tab_csg_sm on am.ID = tab_csg_sm.SFBASE__ACCOUNT__C and
                                                        tab_csg_sm.team_role_name__c = 'Tableau CSG Success Mgr'
                                                            and tab_csg_sm.sft_rank = 1
                left join territory tab_csg_sm_t on tab_csg_sm.TERRITORY__C = tab_csg_sm_t.sfid

                left join salesforce_team core
                        on am.ID = core.SFBASE__ACCOUNT__C and core.TEAM_ROLE_NAME__C = 'Salesperson'
                            and core.sft_rank = 1
                left join territory core_t on core.TERRITORY__C = core_t.sfid
            where rt.NAME = 'Sales'
                and am.ISDELETED = 'N'
        )
        SELECT
            ID,
            -- TABLEAU OWNERSHIP
            tab_ae_territory_id,
            tab_ae_territory_name,
            tab_ae_user_id,
            tab_ae_user_name,
            tab_ae_team_role_name,

            tab_ae_manager_territory_id,
            tab_ae_manager_territory_name,
            tab_ae_manager_user_id,
            tab_ae_manager_user_name,

            tab_ae_manager2_territory_id,
            tab_ae_manager2_territory_name,
            tab_ae_manager2_user_id,
            tab_ae_manager2_user_name,

            tab_ae_manager3_territory_id,
            tab_ae_manager3_territory_name,
            tab_ae_manager3_user_id,
            tab_ae_manager3_user_name,

            tab_ae_manager4_territory_id,
            tab_ae_manager4_territory_name,
            tab_ae_manager4_user_id,
            tab_ae_manager4_user_name,

            tab_ae_manager5_territory_id,
            tab_ae_manager5_territory_name,
            tab_ae_manager5_user_id,
            tab_ae_manager5_user_name,

            tab_ecs_territory_id,
            tab_ecs_territory_name,
            tab_ecs_user_id,
            tab_ecs_user_name,
            tab_ecs_team_role_name,

            tab_crm_territory_id,
            tab_crm_territory_name,
            tab_crm_user_id,
            tab_crm_user_name,
            tab_crm_team_role_name,

            tab_gcm_territory_id,
            tab_gcm_territory_name,
            tab_gcm_user_id,
            tab_gcm_user_name,
            tab_gcm_team_role_name,

            tab_dmsm_territory_id,
            tab_dmsm_territory_name,
            tab_dmsm_user_id,
            tab_dmsm_user_name,
            tab_dmsm_team_role_name,

            tab_embed_territory_id,
            tab_embed_territory_name,
            tab_embed_user_id,
            tab_embed_user_name,
            tab_embed_team_role_name,

            tab_online_territory_id,
            tab_online_territory_name,
            tab_online_user_id,
            tab_online_user_name,
            tab_online_team_role_name,

            tab_bdr_territory_id,
            tab_bdr_territory_name,
            tab_bdr_user_id,
            tab_bdr_user_name,
            tab_bdr_team_role_name,

            tab_csg_territory_id,
            tab_csg_territory_name,
            tab_csg_user_id,
            tab_csg_user_name,
            tab_csg_ap_team_role_name,

            tab_csg_bs_territory_id,
            tab_csg_bs_territory_name,
            tab_csg_bs_user_id,
            tab_csg_bs_user_name,
            tab_csg_bs_team_role_name,

            tab_csg_rm_territory_id,
            tab_csg_rm_territory_name,
            tab_csg_rm_user_id,
            tab_csg_rm_user_name,
            tab_csg_rm_team_role_name,

            tab_csg_sm_territory_id,
            tab_csg_sm_territory_name,
            tab_csg_sm_user_id,
            tab_csg_sm_user_name,
            tab_csg_sm_team_role_name,

            core_territory_id,
            core_territory_name,
            core_user_id,
            core_user_name,
            core_team_role_name
        from main
        where occurence_count = 1
        );