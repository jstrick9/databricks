# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pq (
# MAGIC pq_ser_num  int,
# MAGIC pq_ent_num  int,
# MAGIC pq_empe_num  int,
# MAGIC pq_queue_dt  int,
# MAGIC pq_queue_ti  int,
# MAGIC pq_asgn_empe     int,
# MAGIC pq_queue  string,
# MAGIC pq_flg_cur  int,
# MAGIC pq_doc_type  string,
# MAGIC pq_cmp_type  string,
# MAGIC pq_doc_rcvd_dt	int,
# MAGIC pq_lop_reason	int,
# MAGIC pq_lop_lo_asgn		string,
# MAGIC pq_rsn		int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pq'
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
# MAGIC mn_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tp (
# MAGIC tp_cls_intl_ct int,
# MAGIC tp_cls_intl_1 string,
# MAGIC tp_cls_intl_2 string,
# MAGIC tp_cls_intl_3 string,
# MAGIC tp_cls_intl_4 string,
# MAGIC tp_cls_intl_5 string,
# MAGIC tp_cls_intl_6 string,
# MAGIC tp_cls_intl_7 string,
# MAGIC tp_cls_intl_8 string,
# MAGIC tp_cls_intl_9 string,
# MAGIC tp_cls_intl_10	string,
# MAGIC tp_cls_intl_11	string,
# MAGIC tp_cls_intl_12	string,
# MAGIC tp_cls_intl_13	string,
# MAGIC tp_cls_intl_14	string,
# MAGIC tp_cls_intl_15	string,
# MAGIC tp_cls_intl_16	string,
# MAGIC tp_cls_intl_17	string,
# MAGIC tp_cls_intl_18	string,
# MAGIC tp_cls_intl_19	string,
# MAGIC tp_cls_intl_20	string,
# MAGIC tp_cls_intl_21	string,
# MAGIC tp_cls_intl_22	string,
# MAGIC tp_cls_intl_23	string,
# MAGIC tp_cls_intl_24	string,
# MAGIC tp_cls_intl_25	string,
# MAGIC tp_cls_intl_26	string,
# MAGIC tp_cls_intl_27	string,
# MAGIC tp_cls_intl_28	string,
# MAGIC tp_cls_intl_29	string,
# MAGIC tp_cls_intl_30	string,
# MAGIC tp_cls_intl_31	string,
# MAGIC tp_cls_intl_32	string,
# MAGIC tp_cls_intl_33	string,
# MAGIC tp_cls_intl_34	string,
# MAGIC tp_cls_intl_35	string,
# MAGIC tp_cls_intl_36	string,
# MAGIC tp_cls_intl_37	string,
# MAGIC tp_cls_intl_38	string,
# MAGIC tp_cls_intl_39	string,
# MAGIC tp_cls_intl_40	string,
# MAGIC tp_cls_intl_41	string,
# MAGIC tp_cls_intl_42	string,
# MAGIC tp_cls_intl_43	string,
# MAGIC tp_cls_intl_44	string,
# MAGIC tp_cls_intl_45	string,
# MAGIC tp_cls_intl_46	string,
# MAGIC tp_cls_intl_47	string,
# MAGIC tp_cls_intl_48	string,
# MAGIC tp_pi_ent_num	int,
# MAGIC tp_prcdng_num	int,
# MAGIC tp_reg_num	int,
# MAGIC tp_relshp_cd	string,
# MAGIC tp_ser_num	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tp'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_cop (
# MAGIC cop_ser_num int,
# MAGIC cop_nam     string,
# MAGIC cop_stat    int,
# MAGIC cop_rsn     int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cop'
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
# MAGIC mhi_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mhi'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_psl (
# MAGIC psl_dsgn_srch_cd string,
# MAGIC psl_ser_num         int,
# MAGIC psl_dsgn_srch_val string,
# MAGIC psl_rsn             int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_psl'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ot (
# MAGIC ot_ent_num     int,
# MAGIC ot_prcdng_num int,
# MAGIC ot_text         string,
# MAGIC ot_text_type string,
# MAGIC ot_last_updt_dt int,
# MAGIC ot_rsn         int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ot'
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
# MAGIC jn_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_jn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ty (
# MAGIC ty_addr_1 string,
# MAGIC ty_addr_2 string,
# MAGIC ty_cty         string,
# MAGIC ty_corr_addr_ct int,
# MAGIC ty_corr_addr_1 string,
# MAGIC ty_corr_addr_2 string,
# MAGIC ty_corr_addr_3 string,
# MAGIC ty_corr_addr_4 string,
# MAGIC ty_corr_addr_5 string,
# MAGIC ty_ent_num int,
# MAGIC ty_nam_1	string,
# MAGIC ty_nam_2	string,
# MAGIC ty_nam_3	string,
# MAGIC ty_prcdng_num	int,
# MAGIC ty_ste_ctry_cd	string,
# MAGIC ty_zip_cd	string,
# MAGIC ty_flg_addr_1	int,
# MAGIC ty_flg_addr_2	int,
# MAGIC ty_flg_cmp_stmt	int,
# MAGIC ty_flg_dba_aka	int,
# MAGIC ty_flg_nam_text	int,
# MAGIC ty_last_updt_dt	int,
# MAGIC ty_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ty'
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
# MAGIC fn_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_fn'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tm (
# MAGIC tm_chrg_to int,
# MAGIC tm_chrg_to_loc string,
# MAGIC tm_dt_fil int,
# MAGIC tm_int_atty_num int,
# MAGIC tm_loc         string,
# MAGIC tm_memb_dcsn int,
# MAGIC tm_prcdng_num int,
# MAGIC tm_ttab_stat string,
# MAGIC tm_flg_pn_verify int,
# MAGIC tm_flg_proofed int,
# MAGIC tm_in_loc_dt	int,
# MAGIC tm_last_updt_dt	int,
# MAGIC tm_ttab_stat_dt	int,
# MAGIC tm_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ri (
# MAGIC ri_ser_num int, 
# MAGIC ri_intl_reg_num string, 
# MAGIC ri_intl_reg_dt int, 
# MAGIC ri_flg_prior_clmd int, 
# MAGIC ri_prior_clmd_dt int, 
# MAGIC ri_death_dt int, 
# MAGIC ri_stat int, 
# MAGIC ri_stat_dt int, 
# MAGIC ri_rnwl_dt int, 
# MAGIC ri_auto_protec_dt int, 
# MAGIC ri_ib_pub_dt int, 
# MAGIC ri_flg_1st_ref int, 
# MAGIC ri_notif_dt int, 
# MAGIC ri_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ri'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_te (
# MAGIC te_empe_num int, 
# MAGIC te_fy_pp int, 
# MAGIC te_hrs_ot int, 
# MAGIC te_hrs_reg int, 
# MAGIC te_lo_asgn string, 
# MAGIC te_dir string, 
# MAGIC te_last_updt_dt int, 
# MAGIC te_last_updt_ti int, 
# MAGIC te_lie_unit string, 
# MAGIC te_task_cd string, 
# MAGIC te_ent_dt int, 
# MAGIC te_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_te'
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
# MAGIC og_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_og'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tt (
# MAGIC tt_ent_cd   string,
# MAGIC tt_ent_key  string,
# MAGIC tt_ent_num   int,
# MAGIC tt_text_1   string,
# MAGIC tt_text_2   string,
# MAGIC tt_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pxq (
# MAGIC pxq_ser_num int,
# MAGIC pxq_cases   int,
# MAGIC pxq_empe_num int,
# MAGIC pxq_fy_pp int,
# MAGIC pxq_create_dt int,
# MAGIC pxq_create_ti int,
# MAGIC pxq_received_dt int,
# MAGIC pxq_stat int,
# MAGIC pxq_upload_dt int,
# MAGIC pxq_upload_ti int,
# MAGIC pxq_note	string,
# MAGIC pxq_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pxq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_mas (
# MAGIC mas_ctl_num string,
# MAGIC mas_intl_reg_num string,
# MAGIC mas_ser_num int,
# MAGIC mas_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mas'
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
# MAGIC mif_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_mif'
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
# MAGIC dv_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dv'
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
# MAGIC cb_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_cb'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_rt (
# MAGIC rt_ctr_1 int,
# MAGIC rt_ctr_2 int,
# MAGIC rt_dtl_id string,
# MAGIC rt_rpt_id string,
# MAGIC rt_rpt_dt int,
# MAGIC rt_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_rt'
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
# MAGIC em_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_em'
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
# MAGIC eme_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_eme'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_px (
# MAGIC px_dt_created int,
# MAGIC px_dt_ext int,
# MAGIC px_ent_num int,
# MAGIC px_form_type string,
# MAGIC px_prgrph_type string,
# MAGIC px_print_line string,
# MAGIC px_reg_num int,
# MAGIC px_ser_num int,
# MAGIC px_stat_cd string,
# MAGIC px_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_px'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_dvc (
# MAGIC dvc_ser_num int,
# MAGIC dvc_empe_num int,
# MAGIC dvc_create_dt int,
# MAGIC dvc_create_ti int,
# MAGIC dvc_stat int,
# MAGIC dvc_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dvc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ath (
# MAGIC ath_rsn int,
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pcm (
# MAGIC pcm_ser_num int,
# MAGIC pcm_seq_num int,
# MAGIC pcm_file_sufx string,
# MAGIC pcm_updt_dt int,
# MAGIC pcm_updt_ti int,
# MAGIC pcm_orig_fil_nam string,
# MAGIC pcm_file_size int,
# MAGIC pcm_updt_emp_num int,
# MAGIC pcm_rcpt_dt int,
# MAGIC pcm_rcpt_ti int,
# MAGIC pcm_rcpt_type	string,
# MAGIC pcm_replace_dt	int,
# MAGIC pcm_replace_ti	int,
# MAGIC pcm_rpby_seq_num	int,
# MAGIC pcm_flg_active	int,
# MAGIC pcm_flg_suprted	int,
# MAGIC pcm_flg_cnvrted	int,
# MAGIC pcm_note_num	int,
# MAGIC pcm_comment	string,
# MAGIC pcm_cont_cd	string,
# MAGIC pcm_codec_v	string,
# MAGIC pcm_codec_a	string,
# MAGIC pcm_duratn	int,
# MAGIC pcm_start	int,
# MAGIC pcm_desc	string,
# MAGIC pcm_media_type	string,
# MAGIC pcm_doc_type	string,
# MAGIC pcm_create_dt	int,
# MAGIC pcm_create_ti	int,
# MAGIC pcm_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pcm'
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
# MAGIC amqe_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_amqe'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_log (
# MAGIC id int,
# MAGIC createdate timestamp,
# MAGIC action string,
# MAGIC userid string,
# MAGIC note string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_log'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_exceptions (
# MAGIC insert_dt timestamp,
# MAGIC script_num int,
# MAGIC source_table string,
# MAGIC source_field string,
# MAGIC source_value string,
# MAGIC serial_num string,
# MAGIC object_gid string,
# MAGIC target_table string,
# MAGIC target_field string,
# MAGIC error_num int,
# MAGIC rule	int,
# MAGIC error_msg	string,
# MAGIC cleared_ind	string,
# MAGIC type_ct	string,
# MAGIC resolved_ts	timestamp,
# MAGIC severity_cd	string,
# MAGIC sync_exceptions_id	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_exceptions'
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
# MAGIC dsc_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_dsc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_emp_lo (
# MAGIC empe_num int,
# MAGIC empe_lo string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_emp_lo'
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
# MAGIC ee_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ee'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_migration_rules (
# MAGIC tram_full_name string,
# MAGIC dataset string,
# MAGIC cobol_field_name string,
# MAGIC tmng_mapping string,
# MAGIC tmng_transformation_rule string,
# MAGIC tmng_data_type_cleansing string,
# MAGIC target_table_name string,
# MAGIC target_column_name string,
# MAGIC updated_date string,
# MAGIC rule_num string,
# MAGIC approve_reject	string,
# MAGIC approve_reject_date	string,
# MAGIC approval_rejection_comments	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_migration_rules'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ts (
# MAGIC ts_empe_num int,
# MAGIC ts_cat string,
# MAGIC ts_group string,
# MAGIC ts_role string,
# MAGIC ts_law_office string,
# MAGIC ts_tradeups_id string,
# MAGIC ts_nt_userid string,
# MAGIC ts_log_in_ind string,
# MAGIC ts_active_ind string,
# MAGIC ts_dt_active int,
# MAGIC ts_dt_last_updt	int,
# MAGIC ts_2nd_role_cnt	int,
# MAGIC ts_2nd_role_1	string,
# MAGIC ts_2nd_role_2	string,
# MAGIC ts_2nd_role_3	string,
# MAGIC ts_2nd_role_4	string,
# MAGIC ts_2nd_role_5	string,
# MAGIC ts_2nd_role_6	string,
# MAGIC ts_2nd_role_7	string,
# MAGIC ts_2nd_role_8	string,
# MAGIC ts_2nd_role_9	string,
# MAGIC ts_2nd_role_10	string,
# MAGIC ts_lie_unit	string,
# MAGIC ts_pr_unit	string,
# MAGIC ts_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ts'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pqc (
# MAGIC pqc_rsn int,
# MAGIC pqc_ctl_num string,
# MAGIC pqc_ent_num int,
# MAGIC pqc_empe_num int,
# MAGIC pqc_queue_dt int,
# MAGIC pqc_queue_ti int,
# MAGIC pqc_asgn_empe int,
# MAGIC pqc_queue string,
# MAGIC pqc_flg_cur int,
# MAGIC pqc_doc_type string,
# MAGIC pqc_cmp_type	string,
# MAGIC pqc_doc_rcvd_dt	int,
# MAGIC pqc_lop_reason	int,
# MAGIC pqc_lop_lo_asgn	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pqc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_stc (
# MAGIC stc_ent_num int,
# MAGIC stc_iso_geo_rgncd string,
# MAGIC stc_iso_nm string,
# MAGIC stc_iso_ctry_cd string,
# MAGIC stc_iso_vld_dt int,
# MAGIC stc_iso_invld_dt int,
# MAGIC stc_wipo_vld_dt int,
# MAGIC stc_wipo_invld_dt int,
# MAGIC stc_sc_cd string,
# MAGIC stc_sc_nm string,
# MAGIC stc_type_cd	string,
# MAGIC stc_wipo_cd	string,
# MAGIC stc_wipo_nm	string,
# MAGIC stc_oga_cd	string,
# MAGIC stc_oga_nm	string,
# MAGIC stc_last_mod_dt	int,
# MAGIC stc_last_mod_uid	string,
# MAGIC stc_sc_flg_active	int,
# MAGIC stc_flg_priority	int,
# MAGIC stc_flg_ownr_addr	int,
# MAGIC stc_flg_citz	int,
# MAGIC stc_flg_lgl_ent	int,
# MAGIC stc_flg_for_reg	int,
# MAGIC stc_flg_eu	int,
# MAGIC stc_flg_1	int,
# MAGIC stc_flg_2	int,
# MAGIC stc_flg_3	int,
# MAGIC stc_flg_4	int,
# MAGIC stc_flg_rep_addr	int,
# MAGIC stc_flg_mdrid_dcp	int,
# MAGIC stc_dcp_vld_dt	int,
# MAGIC stc_dcp_invld_dt	int,
# MAGIC stc_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_stc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_geo (
# MAGIC legacy_cd string,
# MAGIC geo_unit_cd string,
# MAGIC geo_unit_nm string,
# MAGIC country_cd string,
# MAGIC country_nm string,
# MAGIC geo_type_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_geo'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_sc (
# MAGIC sc_sc_cd string,
# MAGIC sc_sc_nam string,
# MAGIC sc_home_ctry string,
# MAGIC sc_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_sc'
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
# MAGIC fpr_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_fpr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_psc (
# MAGIC psc_dsgn_srch_cd string,
# MAGIC psc_text_1 string,
# MAGIC psc_last_updt_dt int,
# MAGIC psc_last_updt_ti int,
# MAGIC psc_flg_deleted int,
# MAGIC psc_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_psc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_stnd_am_stat (
# MAGIC am_stat int,
# MAGIC description string,
# MAGIC control_num string,
# MAGIC tram_state string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_stnd_am_stat'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_checkpoint (
# MAGIC script_nm string,
# MAGIC start_ts timestamp,
# MAGIC commit_count int,
# MAGIC records_commited int,
# MAGIC last_commit_ts timestamp,
# MAGIC commit_frequency string,
# MAGIC end_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_checkpoint'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_location (
# MAGIC law_office_cd string,
# MAGIC palm_short_cd string,
# MAGIC tt_text string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_location'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.stnd_myuspto_event (
# MAGIC event_cd string,
# MAGIC event_tx string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/stnd_myuspto_event'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_party_type (
# MAGIC legacy_party_type string,
# MAGIC milestone_cd string,
# MAGIC owner_type_cd string,
# MAGIC fk_owner_type_id int,
# MAGIC owner_type_sequence_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_party_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_migration_script (
# MAGIC script_num int,
# MAGIC script_seq int,
# MAGIC script_name string,
# MAGIC source_table string,
# MAGIC target_table string,
# MAGIC default_create_userid string,
# MAGIC default_last_userid string,
# MAGIC print_only string,
# MAGIC script_description string,
# MAGIC commit_count int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_migration_script'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_og_catg (
# MAGIC og_cat int,
# MAGIC pub_cat_cd string,
# MAGIC pub_cat_des string,
# MAGIC pub_sub_cd string,
# MAGIC pub_sub_des string,
# MAGIC lvl1 string,
# MAGIC lvl2 string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_og_catg'
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
# MAGIC des_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_des'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_assumed_name (
# MAGIC data_tx string,
# MAGIC conv_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_assumed_name'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_casestatus (
# MAGIC cs_serial_num int,
# MAGIC cs_uj_date int,
# MAGIC cs_uj_timer int,
# MAGIC cs_status string,
# MAGIC cs_lock string,
# MAGIC cs_timestamp timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_casestatus'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_petition_dockt (
# MAGIC doc_type_cd string,
# MAGIC description_tx string,
# MAGIC role_cd string,
# MAGIC docket_id int,
# MAGIC docket_tx string,
# MAGIC event_cd string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_petition_dockt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_exception_type (
# MAGIC error_tx string,
# MAGIC error_type string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_exception_type'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.trigger_exceptions (
# MAGIC insert_ts timestamp,
# MAGIC error_num int,
# MAGIC error_msg string,
# MAGIC backtrace string,
# MAGIC callstack string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/trigger_exceptions'
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
# MAGIC oi_ofc_phone	int,
# MAGIC oi_ofc_ttl	string,
# MAGIC oi_new_ofc_dt	int,
# MAGIC oi_old_ofc_dt	int,
# MAGIC oi_rsn	int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_oi'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_translate_work_item_cms (
# MAGIC work_item_type_cd string,
# MAGIC cms_doc_type string,
# MAGIC doc_description string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_translate_work_item_cms'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.sync_authuser (
# MAGIC id int,
# MAGIC userid string,
# MAGIC password string,
# MAGIC role string,
# MAGIC createdate timestamp,
# MAGIC lastupdated timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/sync_authuser'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tg (
# MAGIC tg_ent_num int,
# MAGIC tg_fy_pp int,
# MAGIC tg_pre_exm_btch string,
# MAGIC tg_pub_btch_num string,
# MAGIC tg_qr_aban_ct_1 int,
# MAGIC tg_qr_aban_ct_2 int,
# MAGIC tg_qr_aban_ct_3 int,
# MAGIC tg_qr_aban_ct_4 int,
# MAGIC tg_qr_aban_ct_5 int,
# MAGIC tg_qr_aban_ct_6 int,
# MAGIC tg_qr_aban_ct_7 int,
# MAGIC tg_qr_aban_ct_8 int,
# MAGIC tg_qr_invl_1 int,
# MAGIC tg_qr_invl_2 int,
# MAGIC tg_qr_invl_3 int,
# MAGIC tg_qr_invl_4 int,
# MAGIC tg_qr_invl_5 int,
# MAGIC tg_qr_invl_6 int,
# MAGIC tg_qr_invl_7 int,
# MAGIC tg_qr_invl_8 int,
# MAGIC tg_qr_pub_ct_1 int,
# MAGIC tg_qr_pub_ct_2 int,
# MAGIC tg_qr_pub_ct_3 int,
# MAGIC tg_qr_pub_ct_4 int,
# MAGIC tg_qr_pub_ct_5 int,
# MAGIC tg_qr_pub_ct_6 int,
# MAGIC tg_qr_pub_ct_7 int,
# MAGIC tg_qr_pub_ct_8 int,
# MAGIC tg_tf_stat int,
# MAGIC tg_pub_dt int,
# MAGIC tg_rsn int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tg'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tt1 (
# MAGIC TT_ENT_CD string,
# MAGIC TT_ENT_KEY string,
# MAGIC TT_ENT_NUM int,
# MAGIC TT_TEXT_1 string,
# MAGIC TT_TEXT_2 string,
# MAGIC TT_RSN int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tt1'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
