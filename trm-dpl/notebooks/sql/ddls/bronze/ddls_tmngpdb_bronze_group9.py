# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_ot (
# MAGIC ot_ent_num     int,
# MAGIC ot_prcdng_num int,
# MAGIC ot_text         string,
# MAGIC ot_text_type string,
# MAGIC ot_last_updt_dt int,
# MAGIC ot_rsn         decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ot'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pas (
# MAGIC pas_ser_num  int,
# MAGIC pas_ent_key  string,
# MAGIC pas_ent_cd  int,
# MAGIC pas_pr_stat  int,
# MAGIC pas_pr_dt_stat  int,
# MAGIC pas_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pas'
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
# MAGIC pcm_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pcm'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pd (
# MAGIC PD_ENT_NUM int, 
# MAGIC PD_EMPE_LO string, 
# MAGIC PD_EMPE_NUM int, 
# MAGIC PD_SER_NUM int, 
# MAGIC PD_TRAN_CD int, 
# MAGIC PD_OFFICE_TYPE string, 
# MAGIC PD_SUB_TYPE string, 
# MAGIC PD_START_EVENT string, 
# MAGIC PD_START_DT int, 
# MAGIC PD_START_TIME int, 
# MAGIC PD_START_FY_PP int, 
# MAGIC PD_FA_EVENT string, 
# MAGIC PD_FA_DT int, 
# MAGIC PD_FA_TIME int, 
# MAGIC PD_FA_FY_PP int, 
# MAGIC PD_END_EVENT string, 
# MAGIC PD_END_DT int, 
# MAGIC PD_END_TIME int, 
# MAGIC PD_END_FY_PP int, 
# MAGIC PD_PEND_DAYS int, 
# MAGIC PD_UPDATE_DT int, 
# MAGIC PD_UPDATE_TIME int, 
# MAGIC PD_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pd'
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
# MAGIC pi_flg_prime_us     int,
# MAGIC pi_flg_rstr     int,
# MAGIC pi_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pi'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_plh (
# MAGIC plh_ser_num  int,
# MAGIC plh_phyc_loc  string,
# MAGIC plh_phyc_loc_dt  int,
# MAGIC plh_phyc_loc_ti  int,
# MAGIC plh_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_plh'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

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
# MAGIC pq_rsn		decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pqc (
# MAGIC pqc_rsn decimal(22,0),
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
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_pr (
# MAGIC pr_rcd_type  int,
# MAGIC pr_rel_id_num  string,
# MAGIC pr_ser_num  int,
# MAGIC pr_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_psc (
# MAGIC psc_dsgn_srch_cd string,
# MAGIC psc_text_1 string,
# MAGIC psc_last_updt_dt int,
# MAGIC psc_last_updt_ti int,
# MAGIC psc_flg_deleted int,
# MAGIC psc_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_psc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_psl (
# MAGIC psl_dsgn_srch_cd string,
# MAGIC psl_ser_num         int,
# MAGIC psl_dsgn_srch_val string,
# MAGIC psl_rsn             decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_psl'
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
# MAGIC px_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_px'
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
# MAGIC pxc_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pxc'
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
# MAGIC pxq_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_pxq'
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
# MAGIC py_rsn          decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_py'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_qe (
# MAGIC QE_SER_NUM int, 
# MAGIC QE_QUEUE_TYPE string, 
# MAGIC QE_QUEUE string, 
# MAGIC QE_FLG_ASGN int, 
# MAGIC QE_EMPE_NUM int, 
# MAGIC QE_EMPE_ASGN_DT int, 
# MAGIC QE_EMPE_ASGN_TI int, 
# MAGIC QE_ENTER_DT int, 
# MAGIC QE_ENTER_TI int, 
# MAGIC QE_LEAVE_DT int, 
# MAGIC QE_LEAVE_TI int, 
# MAGIC QE_ENTER_EVENT string, 
# MAGIC QE_LEAVE_EVENT string, 
# MAGIC QE_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_qe'
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
# MAGIC ri_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ri'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_rq (
# MAGIC RQ_ACC_CD string, 
# MAGIC RQ_ACC_NUM string, 
# MAGIC RQ_EMP_NUM_1 int, 
# MAGIC RQ_EMP_NUM_2 int, 
# MAGIC RQ_EMP_NUM_3 int, 
# MAGIC RQ_LOC_1 string, 
# MAGIC RQ_LOC_2 string, 
# MAGIC RQ_LOC_3 string, 
# MAGIC RQ_COPIES int, 
# MAGIC RQ_CTRL_ID int,
# MAGIC RQ_RQST_EMP_NUM int, 
# MAGIC RQ_RQST_EMP_LOC string, 
# MAGIC RQ_MARK_STAT string, 
# MAGIC RQ_NUM_QC_RECS int, 
# MAGIC RQ_STAT string, 
# MAGIC RQ_TRAN_CD int, 
# MAGIC RQ_BEG_DT int, 
# MAGIC RQ_CMPLT_DT int, 
# MAGIC RQ_END_DT int, 
# MAGIC RQ_RQST_DT int, 
# MAGIC RQ_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_rq'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_rt (
# MAGIC rt_ctr_1 int,
# MAGIC rt_ctr_2 decimal(22,0),
# MAGIC rt_dtl_id string,
# MAGIC rt_rpt_id string,
# MAGIC rt_rpt_dt int,
# MAGIC rt_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_rt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_sc (
# MAGIC sc_sc_cd string,
# MAGIC sc_sc_nam string,
# MAGIC sc_home_ctry string,
# MAGIC sc_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_sc'
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
# MAGIC ssr_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ssr'
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
# MAGIC stc_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_stc'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_te (
# MAGIC te_empe_num int, 
# MAGIC te_fy_pp int, 
# MAGIC te_hrs_ot decimal(22,0), 
# MAGIC te_hrs_reg decimal(22,0), 
# MAGIC te_lo_asgn string, 
# MAGIC te_dir string, 
# MAGIC te_last_updt_dt int, 
# MAGIC te_last_updt_ti int, 
# MAGIC te_lie_unit string, 
# MAGIC te_task_cd string, 
# MAGIC te_ent_dt int, 
# MAGIC te_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_te'
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
# MAGIC tg_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tg'
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
# MAGIC th_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_th'
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
# MAGIC tm_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tm'
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
# MAGIC tp_ser_num	int,
# MAGIC tp_last_updt_dt int,
# MAGIC tp_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tp'
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
# MAGIC tqr_cm_ent_num  int,
# MAGIC tqr_cm_stat  string,
# MAGIC tqr_tran_actn_num  int,
# MAGIC tqr_tran_stat  string,
# MAGIC tqr_dt_select  int,
# MAGIC tqr_dt_create  int,
# MAGIC tqr_dt_export  int,
# MAGIC tqr_tran_ind  int,
# MAGIC tqr_sub_tran_cd  int,
# MAGIC tqr_random_num  int,
# MAGIC tqr_rview_type  string,
# MAGIC tqr_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tqr'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_trm (
# MAGIC trm_terminal_id  string,
# MAGIC trm_tran_cd  string,
# MAGIC trm_loc   string,
# MAGIC trm_type int,
# MAGIC trm_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_trm'
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
# MAGIC ts_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ts'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tt (
# MAGIC tt_ent_cd   string,
# MAGIC tt_ent_key  string,
# MAGIC tt_ent_num   int,
# MAGIC tt_text_1   string,
# MAGIC tt_text_2   string,
# MAGIC tt_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_tt1 (
# MAGIC TT_ENT_CD string,
# MAGIC TT_ENT_KEY string,
# MAGIC TT_ENT_NUM int,
# MAGIC TT_TEXT_1 string,
# MAGIC TT_TEXT_2 string,
# MAGIC TT_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_tt1'
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
# MAGIC ty_rsn	decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_ty'
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
# MAGIC upd_rsn  decimal(22,0),
# MAGIC upd_client_id decimal(22,0),
# MAGIC upd_terminal_id  string,
# MAGIC upd_msg_data	string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_upd'
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
# MAGIC vh_rsn decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_vh'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_vt (
# MAGIC vt_ent_num  int,
# MAGIC vt_ser_num  int,
# MAGIC vt_text  string,
# MAGIC vt_text_type  string,
# MAGIC vt_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_vt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_wp (
# MAGIC wp_ser_num  int,
# MAGIC wp_wipo_cd  string,
# MAGIC wp_rsn  decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_wp'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tram_wt (
# MAGIC WT_PROJECT_CD string, 
# MAGIC WT_EMPE_NUM int, 
# MAGIC WT_TYPE string, 
# MAGIC WT_HRS_REG int, 
# MAGIC WT_HRS_OT int, 
# MAGIC WT_FY_PP int, 
# MAGIC WT_LAST_UPDT_DT int,
# MAGIC WT_LAST_UPDT_TI int, 
# MAGIC WT_RSN decimal(22,0)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tram_wt'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.transaction_instance(
# MAGIC   fk_legacy_transaction_cd string, 
# MAGIC   cfk_employee_no string, 
# MAGIC   transaction_instance_gid string, 
# MAGIC   transaction_instance_id string, 
# MAGIC   effective_ts timestamp, 
# MAGIC   details_tx string, 
# MAGIC   terminated_in string, 
# MAGIC   origin_location_tx string, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/transaction_instance'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.trigger_exceptions (
# MAGIC insert_ts timestamp,
# MAGIC error_num decimal(22,0),
# MAGIC error_msg string,
# MAGIC backtrace string,
# MAGIC callstack string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/trigger_exceptions'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.use_in_another_form_h (
# MAGIC fk_trademark_gid string, 
# MAGIC fk_class_id int, 
# MAGIC fk_class_statement_type_cd string, 
# MAGIC preformatted_text_in string, 
# MAGIC first_use_month_no int, 
# MAGIC first_use_day_no int, 
# MAGIC first_use_year_no int, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp, 
# MAGIC statement_tx string, 
# MAGIC action_ct string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/use_in_another_form_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.user_para_form_para_ver (
# MAGIC FK_DOCUMENT_COMPONENT_ID int,  
# MAGIC CFK_FORM_PARAGRAPH_VERSION_GID string,  
# MAGIC LOCK_CONTROL_NO int,   
# MAGIC CREATE_TS timestamp,  
# MAGIC CREATE_USER_ID string,  
# MAGIC LAST_MOD_TS timestamp,  
# MAGIC LAST_MOD_USER_ID string 
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/user_para_form_para_ver'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.user_session (
# MAGIC cfk_empe_no string, 
# MAGIC user_session_gid string, 
# MAGIC status_ct string, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/user_session'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.work_item(
# MAGIC   work_item_gid string, 
# MAGIC   fk_work_item_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${conf.catalog}.${conf.database}.work_item_h(
# MAGIC   work_item_gid string, 
# MAGIC   fk_work_item_type_cd string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_object_h(
# MAGIC   fk_work_item_gid string, 
# MAGIC   fk_object_type_cd string, 
# MAGIC   cfk_object_gid string, 
# MAGIC   lock_control_no int, 
# MAGIC   create_ts timestamp, 
# MAGIC   create_user_id string, 
# MAGIC   last_mod_ts timestamp, 
# MAGIC   last_mod_user_id string, 
# MAGIC   cfk_transaction_instance_gid string, 
# MAGIC   begin_effective_ts timestamp, 
# MAGIC   end_effective_ts timestamp, 
# MAGIC   action_ct string)
# MAGIC using delta
# MAGIC location 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_object_h'
# MAGIC tblproperties ('databricks.delta.autocompact.enabled'= true,'delta.enablechangedatafeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_relationship (
# MAGIC fk_parent_work_item_gid string, 
# MAGIC fk_child_work_item_gid string, 
# MAGIC fk_work_item_relationship_cd string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_relationship'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_relationship_h (
# MAGIC fk_parent_work_item_gid string, 
# MAGIC fk_child_work_item_gid string, 
# MAGIC fk_work_item_relationship_cd string, 
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
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_relationship_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_request (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_work_item_request_cd string, 
# MAGIC cfk_sender_employee_no string, 
# MAGIC request_dt timestamp, 
# MAGIC request_statement_tx string, 
# MAGIC request_description_tx string, 
# MAGIC request_status_ct string, 
# MAGIC cfk_business_unit_cd string, 
# MAGIC business_unit_addr_tx string, 
# MAGIC notify_status_complete_in string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC sequence_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_request'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.work_item_request_employee (
# MAGIC fk_work_item_gid string, 
# MAGIC fk_sequence_no int, 
# MAGIC cfk_receiver_employee_no string, 
# MAGIC receiver_email_addr_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/work_item_request_employee'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.worker (
# MAGIC worker_gid string, 
# MAGIC worker_no string, 
# MAGIC grade_cd string, 
# MAGIC signatory_authority_ct string, 
# MAGIC brs_user_id string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/worker'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.worker_folder (
# MAGIC worker_folder_id int, 
# MAGIC fk_worker_gid string, 
# MAGIC fk_parent_worker_folder_id int, 
# MAGIC name_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC display_order_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/worker_folder'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.worker_folder_item (
# MAGIC cfk_item_object_id int, 
# MAGIC fk_worker_folder_id int, 
# MAGIC fk_object_type_cd string, 
# MAGIC name_tx string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC display_order_no int
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/worker_folder_item'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.worker_h (
# MAGIC worker_gid string, 
# MAGIC worker_no string, 
# MAGIC grade_cd string, 
# MAGIC signatory_authority_ct string, 
# MAGIC brs_user_id string, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string, 
# MAGIC action_ct string, 
# MAGIC cfk_transaction_instance_gid string, 
# MAGIC begin_effective_ts timestamp, 
# MAGIC end_effective_ts timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/worker_h'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.writing_review (
# MAGIC fk_writing_rvw_addl_actn_cd string, 
# MAGIC fk_work_item_gid string, 
# MAGIC fk_review_rating_cd string, 
# MAGIC cfk_reviewer_employee_no string, 
# MAGIC writing_review_id int, 
# MAGIC performance_procedure_error_qt int, 
# MAGIC substantive_error_qt int, 
# MAGIC correction_in string, 
# MAGIC comprehensively_excellent_in string, 
# MAGIC review_comment_tx string, 
# MAGIC review_complete_dt timestamp, 
# MAGIC lock_control_no int, 
# MAGIC create_ts timestamp, 
# MAGIC create_user_id string, 
# MAGIC last_mod_ts timestamp, 
# MAGIC last_mod_user_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/writing_review'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_states (
# MAGIC FK_TRADEMARK_GID	string,
# MAGIC AMENDED_TM_APPLICATION_IN	string,
# MAGIC CHILD_APPLICATION_IN	string,
# MAGIC PARENT_APPLICATION_IN	string,
# MAGIC ASSIGNMENT_RECORDED_IN	string,
# MAGIC COMPLETE_CASE_IN_TICRS_IN	string,
# MAGIC CONCURRENT_USE_IN	string,
# MAGIC CNCR_USE_PEND_TTAB_PRCDNG_IN	string,
# MAGIC CONCURRENT_USE_PUBLISHED_IN	string,
# MAGIC INACTIVE_IN	string,
# MAGIC INTF_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC INTERFERENCE_PUBLISHED_IN	string,
# MAGIC INTERNAL_NOTE_IN	string,
# MAGIC MISCELLANEOUS_1_IN	string,
# MAGIC NEW_TM_CASE_ADDED_IN	string,
# MAGIC OPPOSITION_PERIOD_ENDED_DT	date,
# MAGIC REGISTER_AMENDED_PRINCIPAL_IN	string,
# MAGIC REGISTER_AMENDED_SUPL_IN	string,
# MAGIC REGISTRATION_AMENDED_IN	string,
# MAGIC SERIAL_NUMBER_VERIFIED_IN	string,
# MAGIC IN_PUBLICATION_IN	string,
# MAGIC TTAB_ORAL_HEARING_REQUESTED_IN	string,
# MAGIC NO_ACED_IN	string,
# MAGIC OPPOSITION_PEND_TTAB_PRCDNG_IN	string,
# MAGIC EXPARTE_APPEAL_DECISION_IN	string,
# MAGIC LATEST_SUSPENSION_CHECK_DT	date,
# MAGIC REFUSAL_APPEALED_TO_TTAB_IN	string,
# MAGIC LOP_RECEIVED_IN	string,
# MAGIC ACTIVE_PETITION_IN	string,
# MAGIC UNANSWERED_PETITION_IN	string,
# MAGIC LOCK_CONTROL_NO	int,
# MAGIC CREATE_TS	timestamp,
# MAGIC CREATE_USER_ID	string,
# MAGIC LAST_MOD_TS	timestamp,
# MAGIC LAST_MOD_USER_ID	string,
# MAGIC CONCURRENT_USE_STATUS_CT	string,
# MAGIC NOT_ELECTRONIC_IN	string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_states'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ${conf.catalog}.${conf.database}.tm_appeals (
# MAGIC CFK_TRADEMARK_GID	string,
# MAGIC EXPARTE_APPEAL_DECISION_IN	string,
# MAGIC CONCURRENT_USE_IN	string,
# MAGIC CNCL_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC CNCR_USE_PEND_TTAB_PRCDNG_IN	string,
# MAGIC INTF_PENDING_TTAB_PRCDNG_IN	string,
# MAGIC INTERFERENCE_PUBLISHED_IN	string,
# MAGIC OPPOSITION_PEND_TTAB_PRCDNG_IN	string,
# MAGIC REFUSAL_APPEALED_TO_TTAB_IN	string,
# MAGIC TTAB_MISPLACED_APPL_REQ_IN	string,
# MAGIC TTAB_ORAL_HEARING_REQUESTED_IN	string,
# MAGIC LOCK_CONTROL_NO	INT,
# MAGIC CREATE_TS	TIMESTAMP,
# MAGIC CREATE_USER_ID	string,
# MAGIC LAST_MOD_TS	TIMESTAMP,
# MAGIC LAST_MOD_USER_ID	string
# MAGIC
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 's3://${conf.cdc_bucket}/eds/delta_tables/${conf.catalog}/bronze/tm_appeals'
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'= true,'delta.enableChangeDataFeed' = true)
