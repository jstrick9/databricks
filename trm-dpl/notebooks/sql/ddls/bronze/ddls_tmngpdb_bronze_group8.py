# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_registration_statement (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_reg_stmnt_type_cd string, 
# MAGIC sequence_no int, 
# MAGIC statement_year_no int, 
# MAGIC statement_month_no int, 
# MAGIC statement_day_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_registration_statement'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_registration_statement_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_reg_stmnt_type_cd string, 
# MAGIC sequence_no int, 
# MAGIC statement_year_no int, 
# MAGIC statement_month_no int, 
# MAGIC statement_day_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string, 
# MAGIC statement_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_registration_statement_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_relationship_h (
# MAGIC fk_parent_trademark_gid string, 
# MAGIC fk_related_trademark_gid string, 
# MAGIC fk_relationship_type_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_relationship_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_renewal (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC renewal_filed_dt timestamp, 
# MAGIC renewal_begin_effective_dt timestamp, 
# MAGIC renewal_end_effective_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_renewal'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_renewal_h (
# MAGIC fk_trademark_gid string, 
# MAGIC sequence_no int, 
# MAGIC renewal_filed_dt timestamp, 
# MAGIC renewal_begin_effective_dt timestamp, 
# MAGIC renewal_end_effective_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_renewal_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_telecom_addr (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_telecom_address_gid string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_telecom_addr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_telecom_addr_h (
# MAGIC fk_tm_party_role_id int, 
# MAGIC fk_telecom_address_gid string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC action_ct string, 
# MAGIC primary_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_telecom_addr_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.trademark_h (
# MAGIC trademark_gid string, 
# MAGIC fk_mark_drawing_type_cd string, 
# MAGIC fk_fee_process_type_cd string, 
# MAGIC serial_num_tx string, 
# MAGIC registration_num int, 
# MAGIC filing_dt timestamp, 
# MAGIC registry_ct string, 
# MAGIC standard_character_tx string, 
# MAGIC mark_description_tx string, 
# MAGIC preferred_contact_method_ct string, 
# MAGIC effective_filing_dt timestamp, 
# MAGIC collective_in string, 
# MAGIC legacy_status_cd int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC status_dt timestamp, 
# MAGIC last_action_dt timestamp, 
# MAGIC action_ct string, 
# MAGIC available_for_sou_in string, 
# MAGIC external_reference_tx string,
# MAGIC last_event_type_cd	string,
# MAGIC uspto_generated_image_in	string,
# MAGIC fk_filed_fee_process_type_cd	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/trademark_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_am (
# MAGIC am_addr_1 string, 
# MAGIC am_addr_2 string, 
# MAGIC am_addr_3 string, 
# MAGIC am_addr_4 string, 
# MAGIC am_addr_5 string, 
# MAGIC am_btch_num string, 
# MAGIC am_cncl_cd string, 
# MAGIC am_chrg_to int, 
# MAGIC am_chrg_to_loc string, 
# MAGIC am_cls_ct_actv int, 
# MAGIC am_dt_aban int, 
# MAGIC am_dt_amnd_reg int, 
# MAGIC am_dt_cncl int, 
# MAGIC am_dt_fil int, 
# MAGIC am_dt_pub int, 
# MAGIC am_dt_pub_12c int, 
# MAGIC am_dt_reg int, 
# MAGIC am_dt_rnwl int, 
# MAGIC am_lo_asgn string, 
# MAGIC am_exmr_num int, 
# MAGIC am_ldgr_qty int, 
# MAGIC am_loc string, 
# MAGIC am_mark_1_lin string, 
# MAGIC am_mark_dwg_cd string, 
# MAGIC am_para_lgl_num int, 
# MAGIC am_reg_num int, 
# MAGIC am_ser_num int, 
# MAGIC am_ste_cd_appl string, 
# MAGIC am_ste_cd_reg string, 
# MAGIC am_stat int, 
# MAGIC am_tot_case_act int, 
# MAGIC am_tot_para_act int, 
# MAGIC am_flg_add_case int, 
# MAGIC am_flg_amnd_appl int, 
# MAGIC am_flg_amnd_prin int, 
# MAGIC am_flg_amnd_supl int, 
# MAGIC am_flg_and_oth_cd int, 
# MAGIC am_flg_appeal int, 
# MAGIC am_flg_asgt_rcd int, 
# MAGIC am_flg_chng_reg int, 
# MAGIC am_flg_cncl_pend int, 
# MAGIC am_flg_cncr int, 
# MAGIC am_flg_cncr_pend int, 
# MAGIC am_flg_dspl int, 
# MAGIC am_flg_divd_chld int, 
# MAGIC am_flg_divd_prnt int, 
# MAGIC am_flg_1_act_ct int, 
# MAGIC am_flg_1_act_mlg int, 
# MAGIC am_flg_1_act_para int, 
# MAGIC am_flg_frgn_data int, 
# MAGIC am_flg_frpr_clmd int, 
# MAGIC am_flg_inactv int, 
# MAGIC am_flg_intf_pend int, 
# MAGIC am_flg_mark_oflw int, 
# MAGIC am_flg_tm int, 
# MAGIC am_flg_coll_tm int, 
# MAGIC am_flg_sm int, 
# MAGIC am_flg_coll_sm int, 
# MAGIC am_flg_coll_mm int, 
# MAGIC am_flg_cm int, 
# MAGIC am_flg_no_aced int, 
# MAGIC am_flg_offl_srch int, 
# MAGIC am_flg_opc_prcs int, 
# MAGIC am_flg_opps_pend int, 
# MAGIC am_flg_ord_whs int, 
# MAGIC am_flg_hear_ttab int, 
# MAGIC am_flg_spec_grnt int, 
# MAGIC am_flg_post_prin int, 
# MAGIC am_flg_post_supl int, 
# MAGIC am_flg_pre_exm int, 
# MAGIC am_flg_pub_cncr int, 
# MAGIC am_flg_pub_intf int, 
# MAGIC am_flg_ref_1 int, 
# MAGIC am_flg_rnwl_fil int, 
# MAGIC am_flg_rpb_sct_12 int, 
# MAGIC am_flg_sct_2f int, 
# MAGIC am_flg_sct_2f_pt int, 
# MAGIC am_flg_sct_8_acpt int, 
# MAGIC am_flg_sct_8_fil int, 
# MAGIC am_flg_sct_8_p_a int, 
# MAGIC am_flg_sct_15_fil int, 
# MAGIC am_flg_sct_15_ack int, 
# MAGIC am_flg_sn_verify int, 
# MAGIC am_flg_supl_reg int, 
# MAGIC am_flg_ttab_dcsn int, 
# MAGIC am_flg_un_ans_pet int, 
# MAGIC am_flg_paper_rcv int, 
# MAGIC am_flg_action int, 
# MAGIC am_flg_btch_rpt int, 
# MAGIC am_flg_del_sn_rec int, 
# MAGIC am_flg_nw_iss_cs int, 
# MAGIC am_flg_los_case int, 
# MAGIC am_flg_cert_re int, 
# MAGIC am_flg_db_dump int, 
# MAGIC am_flg_itu_ct int, 
# MAGIC am_flg_itu_dspl int, 
# MAGIC am_flg_prt_noa int, 
# MAGIC am_flg_fil_rcpt_c int, 
# MAGIC am_flg_fil_rcpt_d int, 
# MAGIC am_flg_fil_rcpt_o int, 
# MAGIC am_flg_fnd_case int, 
# MAGIC am_flg_ful_fil_pr int, 
# MAGIC am_flg_jkt_lbl int, 
# MAGIC am_flg_nop int, 
# MAGIC am_flg_nop_crct int, 
# MAGIC am_flg_nor_supl int, 
# MAGIC am_flg_nor_crct int, 
# MAGIC am_flg_not_aban int, 
# MAGIC am_flg_og int, 
# MAGIC am_flg_og_amnd int, 
# MAGIC am_flg_og_cncl int, 
# MAGIC am_flg_og_coc int, 
# MAGIC am_flg_og_nw_cert int, 
# MAGIC am_flg_og_ord int, 
# MAGIC am_flg_og_pub_ext int, 
# MAGIC am_flg_og_reg_ext int, 
# MAGIC am_flg_og_rnwl int, 
# MAGIC am_flg_og_rpub int, 
# MAGIC am_flg_prol int, 
# MAGIC am_flg_prtd_bvr int, 
# MAGIC am_flg_px_btch int, 
# MAGIC am_flg_srch_rpt int, 
# MAGIC am_flg_prnt_tf int, 
# MAGIC am_flg_qr_case int, 
# MAGIC am_flg_itu_ana int, 
# MAGIC am_flg_itu_cur int, 
# MAGIC am_flg_itu_fil int, 
# MAGIC am_flg_itu_pubo int, 
# MAGIC am_atty_dkt_num string, 
# MAGIC am_dt_phyc_fil int, 
# MAGIC am_noa_cd string, 
# MAGIC am_flg_ext_ltr int, 
# MAGIC am_flg_niar_lbl int, 
# MAGIC am_flg_1 int, 
# MAGIC am_flg_2 int, 
# MAGIC am_flg_tm_lbl int, 
# MAGIC am_flg_use_ltr int, 
# MAGIC am_flg_itu_af int, 
# MAGIC am_flg_itu_dlp int, 
# MAGIC am_flg_itu_ef int, 
# MAGIC am_flg_itu_ilm int, 
# MAGIC am_flg_itu_irr int, 
# MAGIC am_flg_itu_pc int, 
# MAGIC am_flg_itu_util1 int, 
# MAGIC am_flg_itu_util2 int, 
# MAGIC am_flg_final int, 
# MAGIC am_flg_aff_reg int, 
# MAGIC am_flg_final_itu int, 
# MAGIC am_flg_aff_itu int, 
# MAGIC am_flg_misc_1 int, 
# MAGIC am_flg_reclassify int, 
# MAGIC am_flg_lop int, 
# MAGIC am_flg_ptogen_img int, 
# MAGIC am_in_loc_dt int, 
# MAGIC am_los_dt int, 
# MAGIC am_1_actn_ct_dt int, 
# MAGIC am_last_actn_dt int, 
# MAGIC am_last_rsp_dt int, 
# MAGIC am_stat_dt int, 
# MAGIC am_1_para_ct_dt int, 
# MAGIC am_opps_cmplt_dt int, 
# MAGIC am_flg_use_fil int, 
# MAGIC am_flg_use_amed int, 
# MAGIC am_flg_use_cur int, 
# MAGIC am_flg_44d_fil int, 
# MAGIC am_flg_44d_amed int, 
# MAGIC am_flg_44d_cur int, 
# MAGIC am_flg_44e_fil int, 
# MAGIC am_flg_44e_amed int, 
# MAGIC am_flg_44e_cur int, 
# MAGIC am_flg_no_bas_fil int, 
# MAGIC am_flg_no_bas_cur int, 
# MAGIC am_flg_frp_dt_fil int, 
# MAGIC am_flg_fr_crt_fil int, 
# MAGIC am_flg_spec_fil int, 
# MAGIC am_flg_use_dt_fil int, 
# MAGIC am_flg_c_drw_fil int, 
# MAGIC am_flg_c_drw_cur int, 
# MAGIC am_flg_3d_drw_fil int, 
# MAGIC am_flg_3d_drw_cur int, 
# MAGIC am_flg_b_spec_fil int, 
# MAGIC am_flg_b_spec_cur int, 
# MAGIC am_flg_2_act_ct int, 
# MAGIC am_flg_itu_amed int, 
# MAGIC am_flg_dkt_card_t int, 
# MAGIC am_flg_in_ticrs int, 
# MAGIC am_flg_ttab_rqst int, 
# MAGIC am_dt_dock int, 
# MAGIC am_flg_66a_cur int, 
# MAGIC am_flg_66a_fil int, 
# MAGIC am_flg_std_char int, 
# MAGIC am_flg_teaspl_fil int, 
# MAGIC am_flg_teaspl_cur int, 
# MAGIC am_lie_num int, 
# MAGIC am_phyc_loc string, 
# MAGIC am_phyc_loc_dt int, 
# MAGIC am_phyc_loc_ti int, 
# MAGIC am_itu_empe int, 
# MAGIC am_pet_empe int, 
# MAGIC am_lie_asgn_dt int, 
# MAGIC am_flg_incom_corr int, 
# MAGIC am_flg_fa_pub int, 
# MAGIC am_dt_incom_corr int, 
# MAGIC am_dt_susp_check int, 
# MAGIC am_lie_unit string, 
# MAGIC am_last_event string, 
# MAGIC am_flg_noam int, 
# MAGIC am_dt_incom_c_itu int, 
# MAGIC am_dt_asgn_itu int, 
# MAGIC am_flg_jnote int, 
# MAGIC am_dt_potl_aban int, 
# MAGIC am_dt_asgn_pet int, 
# MAGIC am_flg_pet int, 
# MAGIC am_flg_nwap_off int, 
# MAGIC am_flg_email_auth int, 
# MAGIC am_flg_accel_cur int, 
# MAGIC am_flg_accel_fil int, 
# MAGIC am_flg_na_xsearch int, 
# MAGIC am_flg_new_appl int, 
# MAGIC am_dt_incom_c_nr int, 
# MAGIC am_dt_incom_c_pr int, 
# MAGIC am_dt_asgn_pr int, 
# MAGIC am_flg_sct_71_fil int, 
# MAGIC am_flg_sct_71_ax int, 
# MAGIC am_flg_sct_71_p_a int, 
# MAGIC am_flg_prlm_amnd int, 
# MAGIC am_dt_incom_c_tqr int, 
# MAGIC am_tqr_empe int, 
# MAGIC am_dt_asgn_tqr int, 
# MAGIC am_hold_empe int, 
# MAGIC am_hold_dt int, 
# MAGIC am_deadq_stat int, 
# MAGIC am_deadq_dt_incom int, 
# MAGIC am_deadq_emp_num int, 
# MAGIC am_flg_protest_ltr int, 
# MAGIC am_flg_teasrf_fil int, 
# MAGIC am_flg_teasrf_cur int, 
# MAGIC am_abanq_stat int, 
# MAGIC am_abanq_dt_incom int, 
# MAGIC am_abanq_emp_num int, 
# MAGIC am_rsn int, 
# MAGIC am_flg_mark_desc int, 
# MAGIC am_flg_not_elec int, 
# MAGIC am_bar_mem_yr int, 
# MAGIC am_bar_mem_mm int, 
# MAGIC am_bar_mem_state string, 
# MAGIC am_bar_mem string, 
# MAGIC am_flg_pr_audit_cur int, 
# MAGIC am_pr_audit_empe int, 
# MAGIC am_pr_audit_beg_dt int, 
# MAGIC am_trmntd_dt int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_am'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_amq (
# MAGIC amq_dept  string,
# MAGIC amq_ser_num  int,
# MAGIC amq_empe_num  int,
# MAGIC amq_lead_num  int,
# MAGIC amq_mgr_num  int,
# MAGIC amq_fy_pp  int,
# MAGIC amq_lead_asgn_dt  int,
# MAGIC amq_lead_asgn_ti  int,
# MAGIC amq_mgr_asgn_dt  int,
# MAGIC amq_mgr_asgn_ti  int,
# MAGIC amq_dt_create  int,
# MAGIC amq_ti_create  int,
# MAGIC amq_random_num  int,
# MAGIC amq_review_stat  int,
# MAGIC amq_upload_cnt  int,
# MAGIC amq_dt_upload  int,
# MAGIC amq_ti_upload  int,
# MAGIC amq_appeal_flag  int,
# MAGIC amq_cop_flag  int,
# MAGIC amq_rsn decimal(22,0) 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_amq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_amqe (
# MAGIC amqe_dept string,
# MAGIC amqe_ser_num int,
# MAGIC amqe_empe_num int,
# MAGIC amqe_empe_rev int,
# MAGIC amqe_fld_num int,
# MAGIC amqe_fld_cd string,
# MAGIC amqe_error_exp string,
# MAGIC amqe_create_dt int,
# MAGIC amqe_create_ti int,
# MAGIC amqe_cmplt_dt int,
# MAGIC amqe_cmplt_ti	int,
# MAGIC amqe_fy_pp	int,
# MAGIC amqe_review_stat	int,
# MAGIC amqe_review_level	int,
# MAGIC amqe_entry_num	int,
# MAGIC amqe_rsn	decimal(22,0) 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_amqe'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ath (
# MAGIC ath_rsn decimal(22,0),
# MAGIC ath_ser_num int,
# MAGIC ath_create_dt int,
# MAGIC ath_create_ti int,
# MAGIC ath_emp_num int,
# MAGIC ath_last_upd_dt int,
# MAGIC ath_last_upd_ti int,
# MAGIC ath_last_emp_num int,
# MAGIC ath_hold_status int,
# MAGIC ath_active_status int,
# MAGIC ath_hold_docket	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ath'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cac (
# MAGIC cac_ser_num  int,
# MAGIC cac_dt_rplcd  int,
# MAGIC cac_time_rplcd  int,
# MAGIC cac_addr_1  string,
# MAGIC cac_addr_2  string,
# MAGIC cac_addr_3  string,
# MAGIC cac_addr_4  string,
# MAGIC cac_addr_5  string,
# MAGIC cac_chng_src  string,
# MAGIC cac_chng_orig  string,
# MAGIC cac_empe_num    int,
# MAGIC cac_dt_rcvd   int,
# MAGIC cac_addr_line1_tx  string,
# MAGIC cac_addr_line2_tx  string,
# MAGIC cac_city_nm  string,
# MAGIC cac_ctry_cd  string,
# MAGIC cac_ctry_nm  string,
# MAGIC cac_firm_nm  string,
# MAGIC cac_geo_rgn_cd  string,
# MAGIC cac_geo_rgn_nm  string,
# MAGIC cac_indvl_full_nm  string,
# MAGIC cac_pstl_cd  string,
# MAGIC cac_rsn   decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cac'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cad (
# MAGIC cad_ser_num  int,
# MAGIC cad_indvl_full_nm  string,
# MAGIC cad_firm_nm  string,
# MAGIC cad_addr_line1_tx  string,
# MAGIC cad_addr_line2_tx  string,
# MAGIC cad_city_nm  string,
# MAGIC cad_geo_rgn_cd  string,
# MAGIC cad_geo_rgn_nm  string,
# MAGIC cad_pstl_cd  string,
# MAGIC cad_ctry_cd  string,
# MAGIC cad_ctry_nm  string,
# MAGIC cad_lst_mod_dt  int,
# MAGIC cad_lst_mod_tm  int,
# MAGIC cad_lst_mod_usrid  string,
# MAGIC cad_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cad'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cb (
# MAGIC cb_ser_num int,
# MAGIC cb_class string,
# MAGIC cb_flg_1a int,
# MAGIC cb_flg_1b int,
# MAGIC cb_flg_44d int,
# MAGIC cb_flg_44e int,
# MAGIC cb_create_dt int,
# MAGIC cb_create_ti int,
# MAGIC cb_last_updt_dt int,
# MAGIC cb_last_updt_ti int,
# MAGIC cb_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cb'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cdh (
# MAGIC cdh_ser_num  int,
# MAGIC cdh_incom_corr_dt  int,
# MAGIC cdh_incom_corr_ti  int,
# MAGIC cdh_event  string,
# MAGIC cdh_stat  int,
# MAGIC cdh_bus_unit  string,
# MAGIC cdh_empe_asgn  int,
# MAGIC cdh_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cdh'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cl (
# MAGIC cl_cls_intl_ct  int,
# MAGIC cl_cls_us_ct  int,
# MAGIC cl_cls_intl_1  string,
# MAGIC cl_cls_intl_2  string,
# MAGIC cl_cls_intl_3  string,
# MAGIC cl_cls_intl_4  string,
# MAGIC cl_cls_intl_5  string,
# MAGIC cl_cls_intl_6  string,
# MAGIC cl_cls_intl_7  string,
# MAGIC cl_cls_intl_8  string,
# MAGIC cl_cls_intl_9  string,
# MAGIC cl_cls_intl_10  string,
# MAGIC cl_cls_intl_11  string,
# MAGIC cl_cls_intl_12  string,
# MAGIC cl_cls_intl_13  string,
# MAGIC cl_cls_intl_14  string,
# MAGIC cl_cls_intl_15  string,
# MAGIC cl_cls_intl_16  string,
# MAGIC cl_cls_intl_17  string,
# MAGIC cl_cls_intl_18  string,
# MAGIC cl_cls_intl_19  string,
# MAGIC cl_cls_intl_20  string,
# MAGIC cl_cls_intl_21  string,
# MAGIC cl_cls_intl_22  string,
# MAGIC cl_cls_intl_23  string,
# MAGIC cl_cls_intl_24  string,
# MAGIC cl_cls_intl_25  string,
# MAGIC cl_cls_intl_26  string,
# MAGIC cl_cls_intl_27  string,
# MAGIC cl_cls_intl_28  string,
# MAGIC cl_cls_intl_29  string,
# MAGIC cl_cls_intl_30  string,
# MAGIC cl_cls_intl_31  string,
# MAGIC cl_cls_intl_32  string,
# MAGIC cl_cls_intl_33  string,
# MAGIC cl_cls_intl_34  string,
# MAGIC cl_cls_intl_35  string,
# MAGIC cl_cls_intl_36  string,
# MAGIC cl_cls_intl_37  string,
# MAGIC cl_cls_intl_38  string,
# MAGIC cl_cls_intl_39  string,
# MAGIC cl_cls_intl_40  string,
# MAGIC cl_cls_intl_41  string,
# MAGIC cl_cls_intl_42  string,
# MAGIC cl_cls_intl_43  string,
# MAGIC cl_cls_intl_44  string,
# MAGIC cl_cls_intl_45  string,
# MAGIC cl_cls_intl_46  string,
# MAGIC cl_cls_intl_47  string,
# MAGIC cl_cls_intl_48  string,
# MAGIC cl_cls_intl_49  string,
# MAGIC cl_cls_intl_50  string,
# MAGIC cl_cls_intl_51  string,
# MAGIC cl_cls_intl_52  string,
# MAGIC cl_cls_intl_53  string,
# MAGIC cl_cls_intl_54  string,
# MAGIC cl_cls_intl_55  string,
# MAGIC cl_cls_intl_56  string,
# MAGIC cl_cls_intl_57  string,
# MAGIC cl_cls_intl_58  string,
# MAGIC cl_cls_intl_59  string,
# MAGIC cl_cls_intl_60  string,
# MAGIC cl_cls_intl_61  string,
# MAGIC cl_cls_intl_62  string,
# MAGIC cl_cls_intl_63  string,
# MAGIC cl_cls_intl_64  string,
# MAGIC cl_cls_intl_65  string,
# MAGIC cl_cls_intl_66  string,
# MAGIC cl_cls_intl_67  string,
# MAGIC cl_cls_intl_68  string,
# MAGIC cl_cls_intl_69  string,
# MAGIC cl_cls_intl_70  string,
# MAGIC cl_cls_intl_71  string,
# MAGIC cl_cls_intl_72  string,
# MAGIC cl_cls_intl_73  string,
# MAGIC cl_cls_intl_74  string,
# MAGIC cl_cls_intl_75  string,
# MAGIC cl_cls_intl_76  string,
# MAGIC cl_cls_intl_77  string,
# MAGIC cl_cls_intl_78  string,
# MAGIC cl_cls_intl_79  string,
# MAGIC cl_cls_intl_80  string,
# MAGIC cl_cls_intl_81  string,
# MAGIC cl_cls_intl_82  string,
# MAGIC cl_cls_intl_83  string,
# MAGIC cl_cls_intl_84  string,
# MAGIC cl_cls_intl_85  string,
# MAGIC cl_cls_intl_86  string,
# MAGIC cl_cls_intl_87  string,
# MAGIC cl_cls_intl_88  string,
# MAGIC cl_cls_intl_89  string,
# MAGIC cl_cls_intl_90  string,
# MAGIC cl_cls_intl_91  string,
# MAGIC cl_cls_intl_92  string,
# MAGIC cl_cls_intl_93  string,
# MAGIC cl_cls_intl_94  string,
# MAGIC cl_cls_intl_95  string,
# MAGIC cl_cls_intl_96  string,
# MAGIC cl_cls_intl_97  string,
# MAGIC cl_cls_intl_98  string,
# MAGIC cl_cls_intl_99  string,
# MAGIC cl_cls_stat     string,
# MAGIC cl_cls_us_1     string,
# MAGIC cl_cls_us_2     string,
# MAGIC cl_cls_us_3     string,
# MAGIC cl_cls_us_4     string,
# MAGIC cl_cls_us_5     string,
# MAGIC cl_cls_us_6     string,
# MAGIC cl_cls_us_7     string,
# MAGIC cl_cls_us_8     string,
# MAGIC cl_cls_us_9     string,
# MAGIC cl_cls_us_10    string,
# MAGIC cl_cls_us_11    string,
# MAGIC cl_cls_us_12    string,
# MAGIC cl_cls_us_13    string,
# MAGIC cl_cls_us_14    string,
# MAGIC cl_cls_us_15    string,
# MAGIC cl_cls_us_16    string,
# MAGIC cl_cls_us_17    string,
# MAGIC cl_cls_us_18    string,
# MAGIC cl_cls_us_19    string,
# MAGIC cl_cls_us_20    string,
# MAGIC cl_cls_us_21    string,
# MAGIC cl_cls_us_22    string,
# MAGIC cl_cls_us_23    string,
# MAGIC cl_cls_us_24    string,
# MAGIC cl_cls_us_25    string,
# MAGIC cl_cls_us_26    string,
# MAGIC cl_cls_us_27    string,
# MAGIC cl_cls_us_28    string,
# MAGIC cl_cls_us_29    string,
# MAGIC cl_cls_us_30    string,
# MAGIC cl_cls_us_31    string,
# MAGIC cl_cls_us_32    string,
# MAGIC cl_cls_us_33    string,
# MAGIC cl_cls_us_34    string,
# MAGIC cl_cls_us_35    string,
# MAGIC cl_cls_us_36    string,
# MAGIC cl_cls_us_37    string,
# MAGIC cl_cls_us_38    string,
# MAGIC cl_cls_us_39    string,
# MAGIC cl_cls_us_40    string,
# MAGIC cl_cls_us_41    string,
# MAGIC cl_cls_us_42    string,
# MAGIC cl_cls_us_43    string,
# MAGIC cl_cls_us_44    string,
# MAGIC cl_cls_us_45    string,
# MAGIC cl_cls_us_46    string,
# MAGIC cl_cls_us_47    string,
# MAGIC cl_cls_us_48    string,
# MAGIC cl_cls_us_49    string,
# MAGIC cl_cls_us_50    string,
# MAGIC cl_cls_us_51    string,
# MAGIC cl_cls_us_52    string,
# MAGIC cl_cls_us_53    string,
# MAGIC cl_cls_us_54    string,
# MAGIC cl_cls_us_55    string,
# MAGIC cl_cls_us_56    string,
# MAGIC cl_cls_us_57    string,
# MAGIC cl_cls_us_58    string,
# MAGIC cl_cls_us_59    string,
# MAGIC cl_cls_us_60    string,
# MAGIC cl_cls_us_61    string,
# MAGIC cl_cls_us_62    string,
# MAGIC cl_cls_us_63    string,
# MAGIC cl_cls_us_64    string,
# MAGIC cl_cls_us_65    string,
# MAGIC cl_cls_us_66    string,
# MAGIC cl_cls_us_67    string,
# MAGIC cl_cls_us_68    string,
# MAGIC cl_cls_us_69    string,
# MAGIC cl_cls_us_70    string,
# MAGIC cl_cls_us_71    string,
# MAGIC cl_cls_us_72    string,
# MAGIC cl_cls_us_73    string,
# MAGIC cl_cls_us_74    string,
# MAGIC cl_cls_us_75    string,
# MAGIC cl_cls_us_76    string,
# MAGIC cl_cls_us_77    string,
# MAGIC cl_cls_us_78    string,
# MAGIC cl_cls_us_79    string,
# MAGIC cl_cls_us_80    string,
# MAGIC cl_cls_us_81    string,
# MAGIC cl_cls_us_82    string,
# MAGIC cl_cls_us_83    string,
# MAGIC cl_cls_us_84    string,
# MAGIC cl_cls_us_85    string,
# MAGIC cl_cls_us_86    string,
# MAGIC cl_cls_us_87    string,
# MAGIC cl_cls_us_88    string,
# MAGIC cl_cls_us_89    string,
# MAGIC cl_cls_us_90    string,
# MAGIC cl_cls_us_91    string,
# MAGIC cl_cls_us_92    string,
# MAGIC cl_cls_us_93    string,
# MAGIC cl_cls_us_94    string,
# MAGIC cl_cls_us_95    string,
# MAGIC cl_cls_us_96    string,
# MAGIC cl_cls_us_97    string,
# MAGIC cl_cls_us_98    string,
# MAGIC cl_cls_us_99    string,
# MAGIC cl_dt_stat      int,
# MAGIC cl_dt_1_use     int,
# MAGIC cl_dt_1_use_comm  int,
# MAGIC cl_flg_anoth_form int,
# MAGIC cl_prime_cls      string,
# MAGIC cl_ser_num        int,
# MAGIC cl_rsn            decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cl'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cm (
# MAGIC cm_ent_cd string, 
# MAGIC cm_ent_num int, 
# MAGIC cm_ent_type string, 
# MAGIC cm_prcd_num int, 
# MAGIC cm_ser_num int, 
# MAGIC cm_ent_dt int, 
# MAGIC cm_sys_dt int, 
# MAGIC cm_sys_ti int, 
# MAGIC cm_doc_id string, 
# MAGIC cm_flg_paper int, 
# MAGIC cm_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cop (
# MAGIC cop_ser_num int,
# MAGIC cop_nam     string,
# MAGIC cop_stat    int,
# MAGIC cop_rsn     decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cop'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_crq (
# MAGIC crq_exmr_num  int,
# MAGIC crq_cases_requested  int,
# MAGIC crq_cases_returned  int,
# MAGIC crq_request_dt  int,
# MAGIC crq_request_ti  int,
# MAGIC crq_fy_pp  int,
# MAGIC crq_exmr_lo  string,
# MAGIC crq_flg_6037 int,
# MAGIC crq_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_crq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ct (
# MAGIC CT_RSN decimal(22,0), 
# MAGIC CT_PROG string, 
# MAGIC CT_SEQ int, 
# MAGIC CT_BEGIN_SEQNO_1 int, 
# MAGIC CT_BEGIN_SEQNO_2 int, 
# MAGIC CT_BEGIN_SEQNO_3 int, 
# MAGIC CT_BEGIN_SEQNO_4 int, 
# MAGIC CT_BEGIN_SEQNO_5 int, 
# MAGIC CT_BEGIN_SEQNO_6 int, 
# MAGIC CT_BEGIN_SEQNO_7 int, 
# MAGIC CT_BEGIN_SEQNO_8 int, 
# MAGIC CT_BEGIN_SEQNO_9 int, 
# MAGIC CT_BEGIN_SEQNO_10 int, 
# MAGIC CT_BEGIN_SEQNO_11 int, 
# MAGIC CT_BEGIN_SEQNO_12 int, 
# MAGIC CT_END_SEQNO_1 int, 
# MAGIC CT_END_SEQNO_2 int, 
# MAGIC CT_END_SEQNO_3 int, 
# MAGIC CT_END_SEQNO_4 int, 
# MAGIC CT_END_SEQNO_5 int, 
# MAGIC CT_END_SEQNO_6 int, 
# MAGIC CT_END_SEQNO_7 int, 
# MAGIC CT_END_SEQNO_8 int, 
# MAGIC CT_END_SEQNO_9 int, 
# MAGIC CT_END_SEQNO_10 int, 
# MAGIC CT_END_SEQNO_11 int, 
# MAGIC CT_END_SEQNO_12 int, 
# MAGIC CT_REST_1 int, 
# MAGIC CT_REST_2 int, 
# MAGIC CT_REST_3 int, 
# MAGIC CT_REST_4 int, 
# MAGIC CT_REST_5 int, 
# MAGIC CT_REST_6 int, 
# MAGIC CT_REST_7 int, 
# MAGIC CT_REST_8 int, 
# MAGIC CT_REST_9 int, 
# MAGIC CT_REST_10 int, 
# MAGIC CT_REST_11 int,
# MAGIC CT_REST_12 int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ct'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_des (
# MAGIC des_key string,
# MAGIC des_cd string,
# MAGIC des_ent_num int,
# MAGIC des_text_1 string,
# MAGIC des_text_2 string,
# MAGIC des_text_3 string,
# MAGIC des_text_4 string,
# MAGIC des_text_5 string,
# MAGIC des_text_6 string,
# MAGIC des_text_7 string,
# MAGIC des_text_8	string,
# MAGIC des_text_9	string,
# MAGIC des_text_10	string,
# MAGIC des_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_des'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_dsc (
# MAGIC dsc_dsgn_srch_cd string,
# MAGIC dsc_text_1 string,
# MAGIC dsc_last_updt_dt int,
# MAGIC dsc_last_updt_ti int,
# MAGIC dsc_empe_num_updt int,
# MAGIC dsc_ent_num int,
# MAGIC dsc_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dsc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_dv (
# MAGIC dv_chld_ser_num int,
# MAGIC dv_dt_chld_rqst int,
# MAGIC dv_dt_chld_cloned int,
# MAGIC dv_dt_decl_infr int,
# MAGIC dv_dt_infr_ltr int,
# MAGIC dv_dt_prt_rqst int,
# MAGIC dv_dt_prcs_cmplt int,
# MAGIC dv_dt_rcv_mlrm int,
# MAGIC dv_dt_rcv_unit int,
# MAGIC dv_dt_chld_aban int,
# MAGIC dv_dt_stat	int,
# MAGIC dv_stat	string,
# MAGIC dv_prnt_ser_num	int,
# MAGIC dv_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dv'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_dvc (
# MAGIC dvc_ser_num int,
# MAGIC dvc_empe_num int,
# MAGIC dvc_create_dt int,
# MAGIC dvc_create_ti int,
# MAGIC dvc_stat int,
# MAGIC dvc_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dvc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ecr (
# MAGIC ECR_NUM int, 
# MAGIC ECR_DT_CREATED int, 
# MAGIC ECR_WBS_CD string, 
# MAGIC ECR_DT_RCVD int, 
# MAGIC ECR_FROM string, 
# MAGIC ECR_SUBJECT string, 
# MAGIC ECR_DT_ASGN int, 
# MAGIC ECR_DT_COMPLTD int, 
# MAGIC ECR_ASGN string, 
# MAGIC ECR_PROGRAMMER string, 
# MAGIC ECR_STATUS int, 
# MAGIC ECR_DT_DUE int, 
# MAGIC ECR_DOCS_CNT int, 
# MAGIC ECR_SUP_DOC_TITLE_1 string, 
# MAGIC ECR_SUP_DOC_TITLE_2 string, 
# MAGIC ECR_SUP_DOC_TITLE_3 string, 
# MAGIC ECR_SUP_DOC_TITLE_4 string, 
# MAGIC ECR_SUP_DOC_TITLE_5 string, 
# MAGIC ECR_SUP_DOC_TITLE_6 string, 
# MAGIC ECR_SUP_DOC_TITLE_7 string, 
# MAGIC ECR_SUP_DOC_TITLE_8 string, 
# MAGIC ECR_SUP_DOC_TITLE_9 string, 
# MAGIC ECR_SUP_DOC_TITLE_10 string, 
# MAGIC ECR_SUP_DOC_DESC_1 string, 
# MAGIC ECR_SUP_DOC_DESC_2 string, 
# MAGIC ECR_SUP_DOC_DESC_3 string, 
# MAGIC ECR_SUP_DOC_DESC_4 string, 
# MAGIC ECR_SUP_DOC_DESC_5 string, 
# MAGIC ECR_SUP_DOC_DESC_6 string, 
# MAGIC ECR_SUP_DOC_DESC_7 string, 
# MAGIC ECR_SUP_DOC_DESC_8 string, 
# MAGIC ECR_SUP_DOC_DESC_9 string, 
# MAGIC ECR_SUP_DOC_DESC_10 string, 
# MAGIC ECR_HRS_WORK int 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ecr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ee (
# MAGIC ee_empe_nam string,
# MAGIC ee_empe_num int,
# MAGIC ee_empe_lo string,
# MAGIC ee_email_addr string,
# MAGIC ee_last_updt_dt int,
# MAGIC ee_empe_stat int,
# MAGIC ee_empe_stat_dt int,
# MAGIC ee_empe_type string,
# MAGIC ee_sig_auth string,
# MAGIC ee_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ee'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_em (
# MAGIC em_empe_nam string,
# MAGIC em_empe_num int,
# MAGIC em_empe_stat int,
# MAGIC em_flg_spe int,
# MAGIC em_org1 string,
# MAGIC em_gau int,
# MAGIC em_grd int,
# MAGIC em_step int,
# MAGIC em_ptonet_id string,
# MAGIC em_grd_dt int,
# MAGIC em_step_dt	int,
# MAGIC em_email_addr	string,
# MAGIC em_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_em'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ema (
# MAGIC ema_ser_num  int,
# MAGIC ema_primary_email  string,
# MAGIC ema_2nd_email_1  string,
# MAGIC ema_2nd_email_2  string,
# MAGIC ema_2nd_email_3  string,
# MAGIC ema_2nd_email_4  string,
# MAGIC ema_type  string,
# MAGIC ema_party_type  int,
# MAGIC ema_ent_num  int,
# MAGIC ema_flg_auth int,
# MAGIC ema_create_dt int,
# MAGIC ema_create_ti int,
# MAGIC ema_last_updt_dt int,
# MAGIC ema_last_updt_ti int,
# MAGIC ema_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ema'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_eme (
# MAGIC eme_ser_num int,
# MAGIC eme_type string,
# MAGIC eme_error string,
# MAGIC eme_error_dt int,
# MAGIC eme_error_ti int,
# MAGIC eme_fixed_dt int,
# MAGIC eme_fixed_ti int,
# MAGIC eme_last_updt_dt int,
# MAGIC eme_last_updt_ti int,
# MAGIC eme_fixed_by_num int,
# MAGIC eme_send_paper	int,
# MAGIC eme_paper_sent	int,
# MAGIC eme_send_notice	int,
# MAGIC eme_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_eme'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_eml (
# MAGIC eml_ser_num  int,
# MAGIC eml_type  string,
# MAGIC eml_create_dt  int,
# MAGIC eml_create_ti  int,
# MAGIC eml_dt_to_be_sent  int,
# MAGIC eml_email_addr  string,
# MAGIC eml_sent_dt  int,
# MAGIC eml_sent_ti  int,
# MAGIC eml_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_eml'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ep (
# MAGIC ep_actn_credit int, 
# MAGIC ep_actn_num int, 
# MAGIC ep_exmr_lo string, 
# MAGIC ep_exmr_num int, 
# MAGIC ep_fy_pp int, 
# MAGIC ep_ser_num int, 
# MAGIC ep_tran_cd int, 
# MAGIC ep_tran_ind int, 
# MAGIC ep_flg_para_lgl int, 
# MAGIC ep_dir string, 
# MAGIC ep_actn_ct_dt int, 
# MAGIC ep_flg_priority int, 
# MAGIC ep_flg_fast_trans int, 
# MAGIC ep_work_unit string, 
# MAGIC ep_tran_cat string, 
# MAGIC ep_empe_num int, 
# MAGIC ep_sub_tran_cd int, 
# MAGIC ep_flg_exmr_tran int, 
# MAGIC ep_sys_dt int, 
# MAGIC ep_sys_ti int, 
# MAGIC ep_empe_type string, 
# MAGIC ep_flg_remail int, 
# MAGIC ep_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ep'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_fn (
# MAGIC fn_dt_frgn_fil int,
# MAGIC fn_dt_frgn_reg int,
# MAGIC fn_dt_frgn_exp int,
# MAGIC fn_dt_rnwl_reg int,
# MAGIC fn_dt_rnwl_exp int,
# MAGIC fn_ent_num int,
# MAGIC fn_frgn_appl_num string,
# MAGIC fn_frgn_ctry_cd string,
# MAGIC fn_frgn_reg_num string,
# MAGIC fn_rnwl_reg_num string,
# MAGIC fn_ser_num	int,
# MAGIC fn_flg_frpr_clmd	int,
# MAGIC fn_priority_type string,
# MAGIC fn_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_fn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_fpr (
# MAGIC fpr_print_q string,
# MAGIC fpr_empe_num int,
# MAGIC fpr_ser_num int,
# MAGIC fpr_submit_dt int,
# MAGIC fpr_submit_ti int,
# MAGIC fpr_seq_num int,
# MAGIC fpr_cmplt_dt int,
# MAGIC fpr_cmplt_ti int,
# MAGIC fpr_details string,
# MAGIC fpr_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_fpr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ft (
# MAGIC ft_ctrl_fld  string,
# MAGIC ft_dt_og int,
# MAGIC ft_ent_num int,
# MAGIC ft_form_text_ct  int,
# MAGIC ft_form_text string,
# MAGIC ft_seq_cd int,
# MAGIC ft_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ft'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_gs (
# MAGIC GS_SER_NUM int, 
# MAGIC GS_CLS string, 
# MAGIC GS_CLS_STAT string, 
# MAGIC GS_TEXT string, 
# MAGIC GS_BASIS_IND string, 
# MAGIC GS_ENT_NUM int, 
# MAGIC GS_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_gs'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_iu (
# MAGIC iu_dt_exp_cause1 int, 
# MAGIC iu_dt_exp_cause2 int, 
# MAGIC iu_dt_exp_cause3 int, 
# MAGIC iu_dt_exp_cause4 int, 
# MAGIC iu_dt_exp_pro_f int, 
# MAGIC iu_dt_last_ext int, 
# MAGIC iu_dt_noa int, 
# MAGIC iu_dt_potl_aban int, 
# MAGIC iu_dt_use_stmt int, 
# MAGIC iu_ser_num int, 
# MAGIC iu_flg_cl1 int, 
# MAGIC iu_flg_cl2 int, 
# MAGIC iu_flg_cl3 int, 
# MAGIC iu_flg_cl4 int, 
# MAGIC iu_flg_cl5 int, 
# MAGIC iu_flg_cl6 int, 
# MAGIC iu_flg_cl7 int, 
# MAGIC iu_flg_cl8 int, 
# MAGIC iu_flg_cl9 int, 
# MAGIC iu_flg_cl10 int, 
# MAGIC iu_flg_cl11 int, 
# MAGIC iu_flg_cl12 int, 
# MAGIC iu_flg_cl13 int, 
# MAGIC iu_flg_cl14 int, 
# MAGIC iu_flg_cl15 int, 
# MAGIC iu_flg_cl16 int, 
# MAGIC iu_flg_cl17 int, 
# MAGIC iu_flg_cl18 int, 
# MAGIC iu_flg_cl19 int, 
# MAGIC iu_flg_cl20 int, 
# MAGIC iu_flg_cl21 int, 
# MAGIC iu_flg_cl22 int, 
# MAGIC iu_flg_cl23 int, 
# MAGIC iu_flg_cl24 int, 
# MAGIC iu_flg_cl25 int, 
# MAGIC iu_flg_cl26 int, 
# MAGIC iu_flg_cl27 int, 
# MAGIC iu_flg_cl28 int, 
# MAGIC iu_flg_cl29 int, 
# MAGIC iu_flg_cl30 int, 
# MAGIC iu_flg_cl31 int, 
# MAGIC iu_flg_cl32 int, 
# MAGIC iu_flg_cl33 int, 
# MAGIC iu_flg_cl34 int, 
# MAGIC iu_flg_cl35 int, 
# MAGIC iu_flg_cl36 int, 
# MAGIC iu_flg_cl37 int, 
# MAGIC iu_flg_cl38 int, 
# MAGIC iu_flg_cl39 int, 
# MAGIC iu_flg_cl40 int, 
# MAGIC iu_flg_cl41 int, 
# MAGIC iu_flg_cl42 int, 
# MAGIC iu_flg_cl43 int, 
# MAGIC iu_flg_cl44 int, 
# MAGIC iu_flg_cl45 int, 
# MAGIC iu_flg_cla int, 
# MAGIC iu_flg_clb int, 
# MAGIC iu_flg_cl200 int, 
# MAGIC iu_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_iu'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ix (
# MAGIC ix_srch_nam  string,
# MAGIC ix_data_type  string,
# MAGIC ix_ref_num  int,
# MAGIC ix_ent_num  int,
# MAGIC ix_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ix'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_jn (
# MAGIC jn_ser_num int,
# MAGIC jn_ent_num int,
# MAGIC jn_created_empe int,
# MAGIC jn_deleted_empe int,
# MAGIC jn_created_dt int,
# MAGIC jn_created_ti int,
# MAGIC jn_deleted_dt int,
# MAGIC jn_deleted_ti int,
# MAGIC jn_title string,
# MAGIC jn_text_ct int,
# MAGIC jn_text	string,
# MAGIC jn_completed_empe	int,
# MAGIC jn_completed_dt	int,
# MAGIC jn_completed_ti	int,
# MAGIC jn_empe_review	int,
# MAGIC jn_review_dt	int,
# MAGIC jn_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_jn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mad (
# MAGIC mad_ctl_num  string,
# MAGIC mad_intl_reg_num string,
# MAGIC mad_ent_num  int,
# MAGIC mad_addr_type string,
# MAGIC mad_num_nam  int,
# MAGIC mad_name_1  string,
# MAGIC mad_name_2  string,
# MAGIC mad_name_3  string,
# MAGIC mad_name_4  string,
# MAGIC mad_name_5  string,
# MAGIC mad_name_6	string,
# MAGIC mad_name_7	string,
# MAGIC mad_name_8	string,
# MAGIC mad_name_9	string,
# MAGIC mad_name_10	string,
# MAGIC mad_name_11	string,
# MAGIC mad_name_12	string,
# MAGIC mad_name_13	string,
# MAGIC mad_name_14	string,
# MAGIC mad_num_addr	int,
# MAGIC mad_addr_1	string,
# MAGIC mad_addr_2	string,
# MAGIC mad_addr_3	string,
# MAGIC mad_addr_4	string,
# MAGIC mad_addr_5	string,
# MAGIC mad_addr_6	string,
# MAGIC mad_ctry_cd	string,
# MAGIC mad_fax		string,
# MAGIC mad_zip_cd		string,
# MAGIC mad_nationality		string,
# MAGIC mad_lgl_nature		string,
# MAGIC mad_place_org		string,
# MAGIC mad_email		string,
# MAGIC mad_entit_cd		string,
# MAGIC mad_entit_ctry_cd		string,
# MAGIC mad_entit_zip_cd		string,
# MAGIC mad_num_enti_addr		int,
# MAGIC mad_enti_addr_1		string,
# MAGIC mad_enti_addr_2		string,
# MAGIC mad_enti_addr_3		string,
# MAGIC mad_enti_addr_4		string,
# MAGIC mad_enti_addr_5		string,
# MAGIC mad_enti_addr_6		string,
# MAGIC mad_rsn		decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mad'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mas (
# MAGIC mas_ctl_num string,
# MAGIC mas_intl_reg_num string,
# MAGIC mas_ser_num int,
# MAGIC mas_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mas'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mc (
# MAGIC mc_ctl_num  string,
# MAGIC mc_dt_mega  int,
# MAGIC mc_reel  int,
# MAGIC mc_stat  int,
# MAGIC mc_key  int,
# MAGIC mc_cls_cnt  int,
# MAGIC mc_cat  string,
# MAGIC mc_prime_ind  string,
# MAGIC mc_cls_1  int,
# MAGIC mc_cls_2  int,
# MAGIC mc_cls_3  int,
# MAGIC mc_cls_4  int,
# MAGIC mc_cls_5  int,
# MAGIC mc_cls_6  int,
# MAGIC mc_cls_7  int,
# MAGIC mc_cls_8  int,
# MAGIC mc_cls_9  int,
# MAGIC mc_cls_10  int,
# MAGIC mc_cls_11  int,
# MAGIC mc_cls_12  int,
# MAGIC mc_cls_13  int,
# MAGIC mc_cls_14  int,
# MAGIC mc_cls_15  int,
# MAGIC mc_cls_16  int,
# MAGIC mc_cls_17  int,
# MAGIC mc_cls_18  int,
# MAGIC mc_cls_19  int,
# MAGIC mc_cls_20  int,
# MAGIC mc_cls_21  int,
# MAGIC mc_cls_22  int,
# MAGIC mc_cls_23  int,
# MAGIC mc_cls_24  int,
# MAGIC mc_cls_25  int,
# MAGIC mc_cls_26  int,
# MAGIC mc_cls_27  int,
# MAGIC mc_cls_28  int,
# MAGIC mc_cls_29  int,
# MAGIC mc_cls_30  int,
# MAGIC mc_cls_31  int,
# MAGIC mc_cls_32  int,
# MAGIC mc_cls_33  int,
# MAGIC mc_cls_34  int,
# MAGIC mc_cls_35  int,
# MAGIC mc_cls_36  int,
# MAGIC mc_cls_37  int,
# MAGIC mc_cls_38  int,
# MAGIC mc_cls_39  int,
# MAGIC mc_cls_40  int,
# MAGIC mc_cls_41  int,
# MAGIC mc_cls_42  int,
# MAGIC mc_cls_43  int,
# MAGIC mc_cls_44  int,
# MAGIC mc_cls_45  int,
# MAGIC mc_cls_46  int,
# MAGIC mc_cls_47  int,
# MAGIC mc_cls_48  int,
# MAGIC mc_cls_49  int,
# MAGIC mc_cls_50  int,
# MAGIC mc_cls_51  int,
# MAGIC mc_cls_52  int,
# MAGIC mc_cls_53  int,
# MAGIC mc_cls_54  int,
# MAGIC mc_cls_55  int,
# MAGIC mc_cls_56  int,
# MAGIC mc_cls_57  int,
# MAGIC mc_cls_58  int,
# MAGIC mc_cls_59  int,
# MAGIC mc_cls_60  int,
# MAGIC mc_cls_61  int,
# MAGIC mc_cls_62  int,
# MAGIC mc_cls_63  int,
# MAGIC mc_cls_64  int,
# MAGIC mc_cls_65  int,
# MAGIC mc_cls_stat_1  string,
# MAGIC mc_cls_stat_2  string,
# MAGIC mc_cls_stat_3  string,
# MAGIC mc_cls_stat_4  string,
# MAGIC mc_cls_stat_5  string,
# MAGIC mc_cls_stat_6  string,
# MAGIC mc_cls_stat_7  string,
# MAGIC mc_cls_stat_8  string,
# MAGIC mc_cls_stat_9  string,
# MAGIC mc_cls_stat_10  string,
# MAGIC mc_cls_stat_11  string,
# MAGIC mc_cls_stat_12  string,
# MAGIC mc_cls_stat_13  string,
# MAGIC mc_cls_stat_14  string,
# MAGIC mc_cls_stat_15  string,
# MAGIC mc_cls_stat_16  string,
# MAGIC mc_cls_stat_17  string,
# MAGIC mc_cls_stat_18  string,
# MAGIC mc_cls_stat_19  string,
# MAGIC mc_cls_stat_20  string,
# MAGIC mc_cls_stat_21  string,
# MAGIC mc_cls_stat_22  string,
# MAGIC mc_cls_stat_23  string,
# MAGIC mc_cls_stat_24  string,
# MAGIC mc_cls_stat_25  string,
# MAGIC mc_cls_stat_26  string,
# MAGIC mc_cls_stat_27  string,
# MAGIC mc_cls_stat_28  string,
# MAGIC mc_cls_stat_29  string,
# MAGIC mc_cls_stat_30  string,
# MAGIC mc_cls_stat_31  string,
# MAGIC mc_cls_stat_32  string,
# MAGIC mc_cls_stat_33  string,
# MAGIC mc_cls_stat_34  string,
# MAGIC mc_cls_stat_35  string,
# MAGIC mc_cls_stat_36  string,
# MAGIC mc_cls_stat_37  string,
# MAGIC mc_cls_stat_38  string,
# MAGIC mc_cls_stat_39  string,
# MAGIC mc_cls_stat_40  string,
# MAGIC mc_cls_stat_41  string,
# MAGIC mc_cls_stat_42  string,
# MAGIC mc_cls_stat_43  string,
# MAGIC mc_cls_stat_44  string,
# MAGIC mc_cls_stat_45  string,
# MAGIC mc_cls_stat_46  string,
# MAGIC mc_cls_stat_47  string,
# MAGIC mc_cls_stat_48  string,
# MAGIC mc_cls_stat_49  string,
# MAGIC mc_cls_stat_50  string,
# MAGIC mc_cls_stat_51  string,
# MAGIC mc_cls_stat_52  string,
# MAGIC mc_cls_stat_53  string,
# MAGIC mc_cls_stat_54  string,
# MAGIC mc_cls_stat_55  string,
# MAGIC mc_cls_stat_56  string,
# MAGIC mc_cls_stat_57  string,
# MAGIC mc_cls_stat_58  string,
# MAGIC mc_cls_stat_59  string,
# MAGIC mc_cls_stat_60  string,
# MAGIC mc_cls_stat_61  string,
# MAGIC mc_cls_stat_62  string,
# MAGIC mc_cls_stat_63  string,
# MAGIC mc_cls_stat_64  string,
# MAGIC mc_cls_stat_65  string,
# MAGIC mc_iss_dt       int,
# MAGIC mc_rsn          decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_md (
# MAGIC md_flg_ci_stat  int,
# MAGIC md_flg_dsc_stat  int,
# MAGIC md_flg_mdc_stat  int,
# MAGIC md_new_mdc  string,
# MAGIC md_old_mdc  string,
# MAGIC md_ser_num  int,
# MAGIC md_source  string,
# MAGIC md_type_updt  string,
# MAGIC md_extr_dt  int,
# MAGIC md_tran_dt  int,
# MAGIC md_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_md'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mhi (
# MAGIC mhi_ctl_num       string,
# MAGIC mhi_intl_reg_num  string,
# MAGIC mhi_action       string,
# MAGIC mhi_ent_dt       int,
# MAGIC mhi_time       int,
# MAGIC mhi_empe_num   int,
# MAGIC mhi_doc_id       string,
# MAGIC mhi_rcordl_dt   int,
# MAGIC mhi_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mhi'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mif (
# MAGIC mif_ctl_num string,
# MAGIC mif_intl_reg_num string,
# MAGIC mif_intl_reg_dt int,
# MAGIC mif_orig_fil_dt int,
# MAGIC mif_stat        int,
# MAGIC mif_stat_dt     int,
# MAGIC mif_reply_by_dt int,
# MAGIC mif_rnwl_dt     int,
# MAGIC mif_ram_sale    int,
# MAGIC mif_type_pay    string,
# MAGIC mif_flg_auto_cert	int,
# MAGIC mif_email	string,
# MAGIC mif_ib_pub_dt	int,
# MAGIC mif_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mif'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mn (
# MAGIC mn_intl_reg_num string,
# MAGIC mn_ser_num int,
# MAGIC mn_notice_type string,
# MAGIC mn_create_dt int,
# MAGIC mn_process_dt int,
# MAGIC mn_source string,
# MAGIC mn_ctl_num string,
# MAGIC mn_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ni (
# MAGIC ni_catg  int,
# MAGIC ni_cty  string,
# MAGIC ni_dt_iss  int,
# MAGIC ni_ent_num  int,
# MAGIC ni_nam  string,
# MAGIC ni_nam_oflw_ct  int,
# MAGIC ni_nam_oflw  string,
# MAGIC ni_nam_type  int,
# MAGIC ni_reg_num  int,
# MAGIC ni_ser_num  int,
# MAGIC ni_ste_ctry_cd  string,
# MAGIC ni_flg_accd  int,
# MAGIC ni_flg_aka  int,
# MAGIC ni_flg_altd  int,
# MAGIC ni_flg_dba  int,
# MAGIC ni_flg_prime_us  int,
# MAGIC ni_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ni'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_og (
# MAGIC og_catg    int,
# MAGIC og_dt_actn int,
# MAGIC og_dt_iss  int,
# MAGIC og_dt_nop  int,
# MAGIC og_reg_num int,
# MAGIC og_ser_num int,
# MAGIC og_stat string,
# MAGIC og_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_og'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ogh (
# MAGIC ogh_catg  int,
# MAGIC ogh_dt_actn  int,
# MAGIC ogh_dt_iss  int,
# MAGIC ogh_dt_nop  int,
# MAGIC ogh_reg_num  int,
# MAGIC ogh_ser_num  int,
# MAGIC ogh_stat  string,
# MAGIC ogh_create_dt  int,
# MAGIC ogh_create_ti  int,
# MAGIC ogh_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ogh'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_oi (
# MAGIC oi_dt_new_ofc int,
# MAGIC oi_dt_old_ofc int,
# MAGIC oi_memb_ttl_ct int,
# MAGIC oi_memb_ttl_1 string,
# MAGIC oi_memb_ttl_2 string,
# MAGIC oi_memb_ttl_3 string,
# MAGIC oi_memb_ttl_4 string,
# MAGIC oi_memb_ttl_5 string,
# MAGIC oi_ofc_cls_ct int,
# MAGIC oi_ofc_cls_1 string,
# MAGIC oi_ofc_cls_2	string,
# MAGIC oi_ofc_cls_3	string,
# MAGIC oi_ofc_cls_4	string,
# MAGIC oi_ofc_cls_5	string,
# MAGIC oi_ofc_cls_ttl_1	string,
# MAGIC oi_ofc_cls_ttl_2	string,
# MAGIC oi_ofc_cls_ttl_3	string,
# MAGIC oi_ofc_cls_ttl_4	string,
# MAGIC oi_ofc_cls_ttl_5	string,
# MAGIC oi_ofc_ind	int,
# MAGIC oi_ofc_memb_1	string,
# MAGIC oi_ofc_memb_2	string,
# MAGIC oi_ofc_memb_3	string,
# MAGIC oi_ofc_memb_4	string,
# MAGIC oi_ofc_memb_5	string,
# MAGIC oi_ofc_phone	decimal(22,0),
# MAGIC oi_ofc_ttl	string,
# MAGIC oi_new_ofc_dt	int,
# MAGIC oi_old_ofc_dt	int,
# MAGIC oi_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_oi'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tmcom_batch_ingest_control (
# MAGIC SERIAL_NUM	int,
# MAGIC BATCH_NM	string,
# MAGIC TARGET_ENDPOINT	string,
# MAGIC ENDPOINT_TYPE	string,
# MAGIC TARGET_ERROR_CODE	int,
# MAGIC TARGET_ERROR_MSG	string,
# MAGIC COMPLETED_TS	TIMESTAMP,
# MAGIC STATUS_CT	string,
# MAGIC CREATE_USER_ID	string,
# MAGIC CREATE_TS	TIMESTAMP,
# MAGIC LAST_MOD_USER_ID	string,
# MAGIC LAST_MOD_TS	TIMESTAMP,
# MAGIC BATCH_DT_NO	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tmcom_batch_ingest_control'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);
