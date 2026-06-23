# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_vt (
# MAGIC vt_ent_num  int,
# MAGIC vt_ser_num  int,
# MAGIC vt_text  string,
# MAGIC vt_text_type  string,
# MAGIC vt_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_vt'
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
# MAGIC cm_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cm'
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
# MAGIC mc_rsn          int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_py (
# MAGIC py_addr_1       string,
# MAGIC py_addr_2       string,
# MAGIC py_citizenship  string,
# MAGIC py_city         string,
# MAGIC py_entity_type  int,
# MAGIC py_ent_num      int,
# MAGIC py_flg_addr_1   int,
# MAGIC py_flg_addr_2   int,
# MAGIC py_flg_asgt_nam int,
# MAGIC py_flg_cmp_stmt int,
# MAGIC py_flg_dba_aka  int,
# MAGIC py_flg_entity   int,
# MAGIC py_flg_nam_text int,
# MAGIC py_nam_1        string,
# MAGIC py_nam_2        string,
# MAGIC py_nam_3        string,
# MAGIC py_party_type   int,
# MAGIC py_ser_num      int,
# MAGIC py_ste_ctry_cd  string,
# MAGIC py_zip_cd       string,
# MAGIC py_reel_frame   string,
# MAGIC py_dt_execute   int,
# MAGIC py_rsn          int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_py'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_plh (
# MAGIC plh_ser_num  int,
# MAGIC plh_phyc_loc  string,
# MAGIC plh_phyc_loc_dt  int,
# MAGIC plh_phyc_loc_ti  int,
# MAGIC plh_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_plh'
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
# MAGIC ep_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ep'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_vh (
# MAGIC vh_ser_num  int,
# MAGIC vh_dt_rplcd  int,
# MAGIC vh_time_rplcd  int,
# MAGIC vh_text_type  string,
# MAGIC vh_ent_num  int,
# MAGIC vh_text  string,
# MAGIC vh_chng_src  string,
# MAGIC vh_chng_orig  string,
# MAGIC vh_empe_num  int,
# MAGIC vh_dt_rcvd  int,
# MAGIC vh_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_vh'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ix (
# MAGIC ix_srch_nam  string,
# MAGIC ix_data_type  string,
# MAGIC ix_ref_num  int,
# MAGIC ix_ent_num  int,
# MAGIC ix_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ix'
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
# MAGIC eml_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_eml'
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
# MAGIC cdh_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cdh'
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
# MAGIC cl_rsn            int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cl'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pas (
# MAGIC pas_ser_num  int,
# MAGIC pas_ent_key  string,
# MAGIC pas_ent_cd  int,
# MAGIC pas_pr_stat  int,
# MAGIC pas_pr_dt_stat  int,
# MAGIC pas_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pas'
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
# MAGIC cac_rsn   int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cac'
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
# MAGIC ogh_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ogh'
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
# MAGIC cad_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cad'
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
# MAGIC ni_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ni'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pi (
# MAGIC pi_catg  int,
# MAGIC pi_cls_intl_ct  int,
# MAGIC pi_cls_intl_1  string,
# MAGIC pi_cls_intl_2  string,
# MAGIC pi_cls_intl_3  string,
# MAGIC pi_cls_intl_4  string,
# MAGIC pi_cls_intl_5  string,
# MAGIC pi_cls_intl_6  string,
# MAGIC pi_cls_intl_7  string,
# MAGIC pi_cls_intl_8  string,
# MAGIC pi_cls_intl_9  string,
# MAGIC pi_cls_intl_10  string,
# MAGIC pi_cls_intl_11  string,
# MAGIC pi_cls_intl_12  string,
# MAGIC pi_cls_intl_13  string,
# MAGIC pi_cls_intl_14  string,
# MAGIC pi_cls_intl_15  string,
# MAGIC pi_cls_intl_16  string,
# MAGIC pi_cls_intl_17  string,
# MAGIC pi_cls_intl_18  string,
# MAGIC pi_cls_intl_19  string,
# MAGIC pi_cls_intl_20  string,
# MAGIC pi_cls_intl_21  string,
# MAGIC pi_cls_intl_22  string,
# MAGIC pi_cls_intl_23  string,
# MAGIC pi_cls_intl_24  string,
# MAGIC pi_cls_intl_25  string,
# MAGIC pi_cls_intl_26  string,
# MAGIC pi_cls_intl_27  string,
# MAGIC pi_cls_intl_28  string,
# MAGIC pi_cls_intl_29  string,
# MAGIC pi_cls_intl_30  string,
# MAGIC pi_cls_us_ct    int,
# MAGIC pi_cls_us_1  string,
# MAGIC pi_cls_us_2  string,
# MAGIC pi_cls_us_3  string,
# MAGIC pi_cls_us_4  string,
# MAGIC pi_cls_us_5  string,
# MAGIC pi_cls_us_6  string,
# MAGIC pi_cls_us_7  string,
# MAGIC pi_cls_us_8  string,
# MAGIC pi_cls_us_9  string,
# MAGIC pi_cls_us_10  string,
# MAGIC pi_cls_us_11  string,
# MAGIC pi_cls_us_12  string,
# MAGIC pi_cls_us_13  string,
# MAGIC pi_cls_us_14  string,
# MAGIC pi_cls_us_15  string,
# MAGIC pi_cls_us_16  string,
# MAGIC pi_cls_us_17  string,
# MAGIC pi_cls_us_18  string,
# MAGIC pi_cls_us_19  string,
# MAGIC pi_cls_us_20  string,
# MAGIC pi_cls_us_21  string,
# MAGIC pi_cls_us_22  string,
# MAGIC pi_cls_us_23  string,
# MAGIC pi_cls_us_24  string,
# MAGIC pi_cls_us_25  string,
# MAGIC pi_cls_us_26  string,
# MAGIC pi_cls_us_27  string,
# MAGIC pi_cls_us_28  string,
# MAGIC pi_cls_us_29  string,
# MAGIC pi_cls_us_30  string,
# MAGIC pi_dt_iss     int,
# MAGIC pi_dt_pub     int,
# MAGIC pi_reg_num     int,
# MAGIC pi_ser_num     int,
# MAGIC pi_flg_cncl_pt     int,
# MAGIC pi_flg_cncr     int,
# MAGIC pi_flg_prime_us     int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pi'
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
# MAGIC ema_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ema'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_wp (
# MAGIC wp_ser_num  int,
# MAGIC wp_wipo_cd  string,
# MAGIC wp_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_wp'
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
# MAGIC md_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_md'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_th (
# MAGIC th_ent_cd  string,
# MAGIC th_ent_num  int,
# MAGIC th_ent_type  string,
# MAGIC th_prcdng_num  int,
# MAGIC th_ent_dt  int,
# MAGIC th_last_updt_dt  int,
# MAGIC th_due_dt  int,
# MAGIC th_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_th'
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
# MAGIC amq_rsn int 
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_amq'
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
# MAGIC iu_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_iu'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pxc (
# MAGIC pxc_ser_num  int,
# MAGIC pxc_empe_num  int,
# MAGIC pxc_bch_ser_num  int,
# MAGIC pxc_create_dt  int,
# MAGIC pxc_create_ti  int,
# MAGIC pxc_stat  int,
# MAGIC pxc_fy_pp  int,
# MAGIC pxc_upload_dt  int,
# MAGIC pxc_upload_ti  int,
# MAGIC pxc_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pxc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tqr (
# MAGIC tqr_ser_num  int,
# MAGIC tqr_reg_num  int,
# MAGIC tqr_dt_period  int,
# MAGIC tqr_queue_type  string,
# MAGIC tqr_queue  string,
# MAGIC tqr_tran_cd  int,
# MAGIC tqr_dt_tran  int,
# MAGIC tqr_cm_cd_type  string,
# MAGIC tqr_empe_num  int,
# MAGIC tqr_asgn_empe_num  int,
# MAGIC tqr_fy_pp  int,
# MAGIC tqr_dt_asgn  int,
# MAGIC tqr_dt_cmpltd  int,
# MAGIC tqr_qpa_done_cnt  int,
# MAGIC tqr_qpa_done_1  int,
# MAGIC tqr_qpa_done_2  int,
# MAGIC tqr_qpa_done_3  int,
# MAGIC tqr_qpa_done_4  int,
# MAGIC tqr_qpa_done_5  int,
# MAGIC tqr_qpa_done_6  int,
# MAGIC tqr_qpa_done_7  int,
# MAGIC tqr_qpa_done_8  int,
# MAGIC tqr_qpa_done_9  int,
# MAGIC tqr_qpa_done_10  int,
# MAGIC tqr_qpa_done_11  int,
# MAGIC tqr_qpa_done_12  int,
# MAGIC tqr_qpa_done_13  int,
# MAGIC tqr_qpa_done_14  int,
# MAGIC tqr_qpa_done_15  int,
# MAGIC tqr_qpa_done_16  int,
# MAGIC tqr_qpa_done_17  int,
# MAGIC tqr_qpa_done_18  int,
# MAGIC tqr_qpa_done_19  int,
# MAGIC tqr_qpa_done_20  int,
# MAGIC tqr_qpa_done_21  int,
# MAGIC tqr_qpa_done_22  int,
# MAGIC tqr_qpa_done_23  int,
# MAGIC tqr_qpa_done_24  int,
# MAGIC tqr_qpa_done_25  int,
# MAGIC tqr_qpa_done_26  int,
# MAGIC tqr_qpa_done_27  int,
# MAGIC tqr_qpa_done_28  int,
# MAGIC tqr_qpa_done_29  int,
# MAGIC tqr_qpa_done_30  int,
# MAGIC tqr_qpa_done_31  int,
# MAGIC tqr_qpa_done_32  int,
# MAGIC tqr_qpa_done_33  int,
# MAGIC tqr_qpa_done_34  int,
# MAGIC tqr_qpa_done_35  int,
# MAGIC tqr_qpa_done_36  int,
# MAGIC tqr_qpa_done_37  int,
# MAGIC tqr_qpa_done_38  int,
# MAGIC tqr_qpa_done_39  int,
# MAGIC tqr_qpa_done_40  int,
# MAGIC tqr_qpa_done_41  int,
# MAGIC tqr_qpa_done_42  int,
# MAGIC tqr_qpa_done_43  int,
# MAGIC tqr_qpa_done_44  int,
# MAGIC tqr_qpa_done_45  int,
# MAGIC tqr_qpa_done_46  int,
# MAGIC tqr_qpa_done_47  int,
# MAGIC tqr_qpa_done_48  int,
# MAGIC tqr_qpa_done_49  int,
# MAGIC tqr_qpa_done_50  int,
# MAGIC tqr_qpa_dt_cnt  int,
# MAGIC tqr_qpa_dt_1  int,
# MAGIC tqr_qpa_dt_2  int,
# MAGIC tqr_qpa_dt_3  int,
# MAGIC tqr_qpa_dt_4  int,
# MAGIC tqr_qpa_dt_5  int,
# MAGIC tqr_qpa_dt_6  int,
# MAGIC tqr_qpa_dt_7  int,
# MAGIC tqr_qpa_dt_8  int,
# MAGIC tqr_qpa_dt_9  int,
# MAGIC tqr_qpa_dt_10  int,
# MAGIC tqr_qpa_dt_11  int,
# MAGIC tqr_qpa_dt_12  int,
# MAGIC tqr_qpa_dt_13  int,
# MAGIC tqr_qpa_dt_14  int,
# MAGIC tqr_qpa_dt_15  int,
# MAGIC tqr_qpa_dt_16  int,
# MAGIC tqr_qpa_dt_17  int,
# MAGIC tqr_qpa_dt_18  int,
# MAGIC tqr_qpa_dt_19  int,
# MAGIC tqr_qpa_dt_20  int,
# MAGIC tqr_qpa_dt_21  int,
# MAGIC tqr_qpa_dt_22  int,
# MAGIC tqr_qpa_dt_23  int,
# MAGIC tqr_qpa_dt_24  int,
# MAGIC tqr_qpa_dt_25  int,
# MAGIC tqr_qpa_dt_26  int,
# MAGIC tqr_qpa_dt_27  int,
# MAGIC tqr_qpa_dt_28  int,
# MAGIC tqr_qpa_dt_29  int,
# MAGIC tqr_qpa_dt_30  int,
# MAGIC tqr_qpa_dt_31  int,
# MAGIC tqr_qpa_dt_32  int,
# MAGIC tqr_qpa_dt_33  int,
# MAGIC tqr_qpa_dt_34  int,
# MAGIC tqr_qpa_dt_35  int,
# MAGIC tqr_qpa_dt_36  int,
# MAGIC tqr_qpa_dt_37  int,
# MAGIC tqr_qpa_dt_38  int,
# MAGIC tqr_qpa_dt_39  int,
# MAGIC tqr_qpa_dt_40  int,
# MAGIC tqr_qpa_dt_41  int,
# MAGIC tqr_qpa_dt_42  int,
# MAGIC tqr_qpa_dt_43  int,
# MAGIC tqr_qpa_dt_44  int,
# MAGIC tqr_qpa_dt_45  int,
# MAGIC tqr_qpa_dt_46  int,
# MAGIC tqr_qpa_dt_47  int,
# MAGIC tqr_qpa_dt_48  int,
# MAGIC tqr_qpa_dt_49  int,
# MAGIC tqr_qpa_dt_50  int,
# MAGIC tqr_cm_ent_num  string,
# MAGIC tqr_cm_stat  int,
# MAGIC tqr_tran_actn_num  string,
# MAGIC tqr_tran_stat  int,
# MAGIC tqr_dt_select  int,
# MAGIC tqr_dt_create  int,
# MAGIC tqr_dt_export  int,
# MAGIC tqr_tran_ind  int,
# MAGIC tqr_sub_tran_cd  int,
# MAGIC tqr_random_num  int,
# MAGIC tqr_rview_type  string,
# MAGIC tqr_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tqr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pr (
# MAGIC pr_rcd_type  int,
# MAGIC pr_rel_id_num  string,
# MAGIC pr_ser_num  int,
# MAGIC pr_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pr'
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
# MAGIC ft_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ft'
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
# MAGIC crq_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_crq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ssr (
# MAGIC ssr_ser_num  int,
# MAGIC ssr_ent_num  int,
# MAGIC ssr_type  string,
# MAGIC ssr_notif_email  string,
# MAGIC ssr_asgn_empe  int,
# MAGIC ssr_asgn_dt  int,
# MAGIC ssr_asgn_ti  int,
# MAGIC ssr_create_empe  int,
# MAGIC ssr_create_dt  int,
# MAGIC ssr_create_ti  int,
# MAGIC ssr_cmplt_dt	int,
# MAGIC ssr_cmplt_ti	int,
# MAGIC ssr_cmplt_empe	int,
# MAGIC ssr_lo_asgn	string,
# MAGIC ssr_priority_cd	int,
# MAGIC ssr_exmr_num	int,
# MAGIC ssr_request_email	string,
# MAGIC ssr_request_dt	int,
# MAGIC ssr_request_ti	int,
# MAGIC ssr_lie_unit	string,
# MAGIC ssr_flg_delete	int,
# MAGIC ssr_spec_credit	int,
# MAGIC ssr_office	string,
# MAGIC ssr_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ssr'
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
# MAGIC mad_rsn		int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mad'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_tram_trm_obj_id_mapping (
# MAGIC rsn		int,
# MAGIC legacy_dataset		string,
# MAGIC class_name		string,
# MAGIC serial_num		int,
# MAGIC trademark_id		int,
# MAGIC gid		string,
# MAGIC row_id_key		int,
# MAGIC row_gid_key		string,
# MAGIC obj_creator		string,
# MAGIC row_cd		string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_tram_trm_obj_id_mapping'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_ep (
# MAGIC prodvty_cd  int,
# MAGIC prodvty_ind  int,
# MAGIC exam_no  int,
# MAGIC reason_tx  string,
# MAGIC fk_work_item_code  string,
# MAGIC fk_credit_tran_rsn_type_cd  string,
# MAGIC reason_ct  string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_ep'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_trm (
# MAGIC trm_terminal_id  string,
# MAGIC trm_tran_cd  string,
# MAGIC trm_loc   string,
# MAGIC trm_type int,
# MAGIC trm_rsn  int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_trm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_upd (
# MAGIC upd_ser_num  int,
# MAGIC upd_updt_dt  int,
# MAGIC upd_updt_ti  int,
# MAGIC upd_prog_id  string,
# MAGIC upd_tran_cd  string,
# MAGIC upd_ent_num  int,
# MAGIC upd_set_array string,
# MAGIC upd_rsn  int,
# MAGIC upd_client_id int,
# MAGIC upd_terminal_id  string,
# MAGIC upd_msg_data	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_upd'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.mv_myuspto_trm_search (
# MAGIC SERIAL_NUM int,
# MAGIC REGISTRATION_NUM int,
# MAGIC FILING_DT timestamp,
# MAGIC REGISTRATION_DT timestamp,
# MAGIC MARK_DESCRIPTION_TX string,
# MAGIC OWNER_ID string,
# MAGIC OWNER_NM string,
# MAGIC ATTORNEY_ID string,
# MAGIC ATTORNEY_NM string,
# MAGIC DEAD_MARK_IN string,
# MAGIC MARK_DRAWING_CD string,
# MAGIC SEARCH_MARK_TX string,
# MAGIC SEARCH_OWNER_NM string,
# MAGIC SEARCH_ATTORNEY_NM string,
# MAGIC PROCEEDING_NUM_LIST string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/mv_myuspto_trm_search'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
# MAGIC
