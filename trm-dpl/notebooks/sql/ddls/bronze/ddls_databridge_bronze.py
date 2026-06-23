# Databricks notebook source
# MAGIC %md
# MAGIC <pre>
# MAGIC Purpose: This ntbk executes DDL scripts to create TMRWORKER bronze layer tables
# MAGIC </pre>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file="../../../config/"+dbutils.widgets.get("dbx_env").rstrip()+"/databridge-conf.yaml"
print(f'{config_file=}')
if dbx_env == "qa":
    dbutils.widgets.text("env", "test")
    print(f'{dbx_env=}')
else:
    dbutils.widgets.text("env", dbx_env)
    print(f'{dbx_env=}')

# COMMAND ----------

# MAGIC %run ../../../python/shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

#schema variables
common_configs=read_yaml(config_file)
databridge_catalog = common_configs['schema']['databridge_catalog']
src_folder=common_configs['cdc']['src_csv_files']
src_database=common_configs['cdc']['src_database']
data_quality_catalog = common_configs['schema']['data_quality_catalog']
spark.conf.set('config.data_quality_db', data_quality_catalog.lower())
spark.conf.set('config.databridge_catalog', databridge_catalog.lower())
print(f'{databridge_catalog=},{src_folder=}, ,{src_database=}')

# COMMAND ----------

database = 'bronze'
control_table = 'cdc_batch_job_control'
job_history_table = 'cdc_batch_job_history'
spark.conf.set('conf.catalog', databridge_catalog)
spark.conf.set('conf.database', database)
spark.conf.set('conf.control_table', control_table)
spark.conf.set('conf.job_history_table', job_history_table)
spark.conf.set('conf.src_folder', src_folder)
spark.conf.set('conf.src_database', src_database)


# COMMAND ----------

# MAGIC %sql
# MAGIC create CATALOG if not exists  ${conf.catalog};
# MAGIC use catalog ${conf.catalog};
# MAGIC create schema if not exists  ${conf.database};
# MAGIC use ${conf.database};
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.cm(
# MAGIC absn int,
# MAGIC afn int,
# MAGIC cm_doc_id string,
# MAGIC cm_ent_cd string,
# MAGIC cm_ent_dt timestamp,
# MAGIC cm_ent_num int,
# MAGIC cm_ent_type string,
# MAGIC cm_flg_paper int,
# MAGIC cm_prcd_num int,
# MAGIC cm_rsn int,
# MAGIC cm_ser_num int,
# MAGIC cm_sys_dt timestamp,
# MAGIC cm_sys_ti int,
# MAGIC last_modified_date timestamp,
# MAGIC oracle_apply_time timestamp)
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.mhi(
# MAGIC absn int,
# MAGIC afn int,
# MAGIC last_modified_date timestamp,
# MAGIC mhi_action string,
# MAGIC mhi_ctl_num string,
# MAGIC mhi_doc_id string,
# MAGIC mhi_empe_num int,
# MAGIC mhi_ent_dt timestamp, 
# MAGIC mhi_intl_reg_num string, 
# MAGIC mhi_rcordl_dt timestamp, 
# MAGIC mhi_rsn int, 
# MAGIC mhi_time int, 
# MAGIC oracle_apply_time timestamp)
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.am(
# MAGIC absn              int,
# MAGIC afn                  int,
# MAGIC am_1_actn_ct_dt      timestamp,
# MAGIC am_1_para_ct_dt      timestamp,
# MAGIC am_atty_dkt_num      string,
# MAGIC am_btch_num          string,
# MAGIC am_chrg_to          int,
# MAGIC am_chrg_to_loc      string,
# MAGIC am_cls_ct_actv      int,
# MAGIC am_cncl_cd          string,
# MAGIC am_abanq_dt_incom   timestamp,
# MAGIC am_abanq_emp_num    int,
# MAGIC am_abanq_stat      int,
# MAGIC am_deadq_dt_incom   timestamp,
# MAGIC am_deadq_emp_num    int,
# MAGIC am_deadq_stat      int,
# MAGIC am_dt_aban          timestamp,
# MAGIC am_dt_amnd_reg      timestamp,
# MAGIC am_dt_asgn_itu      timestamp,
# MAGIC am_dt_asgn_pet      timestamp,
# MAGIC am_dt_asgn_pr      timestamp,
# MAGIC am_dt_asgn_tqr      timestamp,
# MAGIC am_dt_cncl          timestamp,
# MAGIC am_dt_dock          timestamp,
# MAGIC am_dt_fil          timestamp,
# MAGIC am_dt_incom_corr    timestamp,
# MAGIC am_dt_incom_c_itu   timestamp,
# MAGIC am_dt_incom_c_nr    timestamp,
# MAGIC am_dt_incom_c_pr    timestamp,
# MAGIC am_dt_incom_c_tqr   timestamp,
# MAGIC am_dt_phyc_fil      timestamp,
# MAGIC am_dt_potl_aban      timestamp,
# MAGIC am_dt_pub          timestamp,
# MAGIC am_dt_pub_12c      timestamp,
# MAGIC am_dt_reg          timestamp,
# MAGIC am_dt_rnwl          timestamp,
# MAGIC am_dt_susp_check    timestamp,
# MAGIC am_exmr_num          int, 
# MAGIC am_fill_rep_1      string,
# MAGIC am_flg_1          int,
# MAGIC am_flg_1_act_ct      int,
# MAGIC am_flg_1_act_mlg    int,
# MAGIC am_flg_1_act_para   int,
# MAGIC am_flg_2          int,
# MAGIC am_flg_2_act_ct      int,
# MAGIC am_flg_3d_drw_cur   int,
# MAGIC am_flg_3d_drw_fil   int,
# MAGIC am_flg_44d_amed      int,
# MAGIC am_flg_44d_cur      int,
# MAGIC am_flg_44d_fil      int,
# MAGIC am_flg_44e_amed      int,
# MAGIC am_flg_44e_cur      int,
# MAGIC am_flg_44e_fil      int,
# MAGIC am_flg_66a_cur      int,
# MAGIC am_flg_66a_fil      int,
# MAGIC am_flg_accel_cur    int,
# MAGIC am_flg_accel_fil    int,
# MAGIC am_flg_action      int,
# MAGIC am_flg_add_case      int,
# MAGIC am_flg_aff_itu      int,
# MAGIC am_flg_aff_reg      int,
# MAGIC am_flg_amnd_appl    int,
# MAGIC am_flg_amnd_prin    int,
# MAGIC am_flg_amnd_supl    int,
# MAGIC am_flg_and_oth_cd   int,
# MAGIC am_flg_appeal      int,
# MAGIC am_flg_asgt_rcd      int,
# MAGIC am_flg_btch_rpt      int,
# MAGIC am_flg_b_spec_cur   int,
# MAGIC am_flg_b_spec_fil   int,
# MAGIC am_flg_cert_re      int,
# MAGIC am_flg_chng_reg      int,
# MAGIC am_flg_cm          int,
# MAGIC am_flg_cncl_pend    int,
# MAGIC am_flg_cncr          int,
# MAGIC am_flg_cncr_pend    int,
# MAGIC am_flg_coll_mm      int,
# MAGIC am_flg_coll_sm      int,
# MAGIC am_flg_coll_tm      int,
# MAGIC am_flg_c_drw_cur    int,
# MAGIC am_flg_c_drw_fil    int,
# MAGIC am_flg_db_dump      int,
# MAGIC am_flg_del_sn_rec   int,
# MAGIC am_flg_divd_chld    int,
# MAGIC am_flg_divd_prnt    int,
# MAGIC am_flg_itu_ct      int,
# MAGIC am_flg_itu_dspl      int,
# MAGIC am_flg_prt_noa      int,
# MAGIC am_flg_dkt_card_t   int,
# MAGIC am_flg_dspl          int,
# MAGIC am_flg_email_auth   int,
# MAGIC am_flg_ext_ltr      int,
# MAGIC am_flg_fa_pub      int,
# MAGIC am_flg_fil_rcpt_c   int,
# MAGIC am_flg_fil_rcpt_d   int,
# MAGIC am_flg_fil_rcpt_o   int,
# MAGIC am_flg_final      int,
# MAGIC am_flg_final_itu    int,
# MAGIC am_flg_fnd_case      int,
# MAGIC am_flg_frgn_data    int,
# MAGIC am_flg_frpr_clmd    int,
# MAGIC am_flg_frp_dt_fil   int,
# MAGIC am_flg_fr_crt_fil   int,
# MAGIC am_flg_teaspl_cur   int,
# MAGIC am_flg_teaspl_fil   int,
# MAGIC am_flg_ful_fil_pr   int,
# MAGIC am_flg_hear_ttab    int,
# MAGIC am_flg_inactv      int,
# MAGIC am_flg_incom_corr   int,
# MAGIC am_flg_intf_pend    int,
# MAGIC am_flg_in_ticrs      int,
# MAGIC am_flg_itu_af      int,
# MAGIC am_flg_itu_amed      int,
# MAGIC am_flg_itu_ana      int,
# MAGIC am_flg_itu_cur      int,
# MAGIC am_flg_itu_dlp      int,
# MAGIC am_flg_itu_ef      int,
# MAGIC am_flg_itu_fil      int,
# MAGIC am_flg_itu_ilm      int,
# MAGIC am_flg_itu_irr      int,
# MAGIC am_flg_itu_pc      int,
# MAGIC am_flg_itu_pubo      int,
# MAGIC am_flg_itu_util1    int,
# MAGIC am_flg_itu_util2    int,
# MAGIC am_flg_jkt_lbl      int,
# MAGIC am_flg_jnote      int,
# MAGIC am_flg_los_case      int,
# MAGIC am_flg_mark_oflw    int,
# MAGIC am_flg_misc_1      int,
# MAGIC am_flg_reclassify   int,
# MAGIC am_flg_lop          int,
# MAGIC am_flg_ptogen_img   int,
# MAGIC am_flg_na_xsearch   int,
# MAGIC am_flg_new_appl      int,
# MAGIC am_flg_niar_lbl      int,
# MAGIC am_flg_noam          int,
# MAGIC am_flg_nop          int,
# MAGIC am_flg_nop_crct      int,
# MAGIC am_flg_nor_crct      int,
# MAGIC am_flg_nor_supl      int,
# MAGIC am_flg_not_aban      int,
# MAGIC am_flg_no_aced      int,
# MAGIC am_flg_no_bas_cur   int,
# MAGIC am_flg_no_bas_fil   int,
# MAGIC am_flg_nwap_off      int,
# MAGIC am_flg_nw_iss_cs    int,
# MAGIC am_flg_offl_srch    int,
# MAGIC am_flg_og          int,
# MAGIC am_flg_og_amnd      int,
# MAGIC am_flg_og_cncl      int,
# MAGIC am_flg_og_coc      int,
# MAGIC am_flg_og_nw_cert   int,
# MAGIC am_flg_og_ord      int,
# MAGIC am_flg_og_pub_ext   int,
# MAGIC am_flg_og_reg_ext   int,
# MAGIC am_flg_og_rnwl      int,
# MAGIC am_flg_og_rpub      int,
# MAGIC am_flg_opc_prcs      int,
# MAGIC am_flg_opps_pend    int,
# MAGIC am_flg_ord_whs      int,
# MAGIC am_flg_paper_rcv    int,
# MAGIC am_flg_pet          int,
# MAGIC am_flg_post_prin    int,
# MAGIC am_flg_post_supl    int,
# MAGIC am_flg_pre_exm      int,
# MAGIC am_flg_prlm_amnd    int,
# MAGIC am_flg_prnt_tf      int,
# MAGIC am_flg_prol          int,
# MAGIC am_flg_prtd_bvr      int,
# MAGIC am_flg_pub_cncr      int,
# MAGIC am_flg_pub_intf      int,
# MAGIC am_flg_px_btch      int,
# MAGIC am_flg_qr_case      int,
# MAGIC am_flg_ref_1      int,
# MAGIC am_flg_rnwl_fil      int,
# MAGIC am_flg_rpb_sct_12   int,
# MAGIC am_flg_sct_15_ack   int,
# MAGIC am_flg_sct_15_fil   int,
# MAGIC am_flg_sct_2f      int,
# MAGIC am_flg_sct_2f_pt    int,
# MAGIC am_flg_sct_71_ax    int,
# MAGIC am_flg_sct_71_fil   int,
# MAGIC am_flg_sct_71_p_a   int,
# MAGIC am_flg_sct_8_acpt   int,
# MAGIC am_flg_sct_8_fil    int,
# MAGIC am_flg_sct_8_p_a    int,
# MAGIC am_flg_sm           int,
# MAGIC am_flg_sn_verify    int,
# MAGIC am_flg_spec_fil      int,
# MAGIC am_flg_spec_grnt    int,
# MAGIC am_flg_srch_rpt      int,
# MAGIC am_flg_std_char      int,
# MAGIC am_flg_supl_reg      int,
# MAGIC am_flg_tm          int,
# MAGIC am_flg_tm_lbl      int,
# MAGIC am_flg_ttab_dcsn    int,
# MAGIC am_flg_ttab_rqst    int,
# MAGIC am_flg_un_ans_pet   int,
# MAGIC am_flg_use_amed      int,
# MAGIC am_flg_use_cur      int,
# MAGIC am_flg_use_dt_fil   int,
# MAGIC am_flg_use_fil      int,
# MAGIC am_flg_use_ltr      int,
# MAGIC am_hold_dt          timestamp,
# MAGIC am_hold_empe      int,
# MAGIC am_in_loc_dt        timestamp,
# MAGIC am_itu_empe          int,
# MAGIC am_last_actn_dt      timestamp,
# MAGIC am_last_event      string,
# MAGIC am_last_rsp_dt      timestamp,
# MAGIC am_ldgr_qty          int,
# MAGIC am_lie_asgn_dt      timestamp,
# MAGIC am_lie_num          int,
# MAGIC am_lie_unit          string,
# MAGIC am_loc              string,
# MAGIC am_los_dt          timestamp,
# MAGIC am_lo_asgn          string,
# MAGIC am_mark_1_lin      string,
# MAGIC am_mark_dwg_cd      string,
# MAGIC am_noa_cd          string,
# MAGIC am_opps_cmplt_dt    timestamp,
# MAGIC am_para_lgl_num      int,
# MAGIC am_pet_empe          int,
# MAGIC am_phyc_loc          string,
# MAGIC am_phyc_loc_dt      timestamp,
# MAGIC am_phyc_loc_ti      int,
# MAGIC am_reg_num          int,
# MAGIC am_rsn              double,
# MAGIC am_ser_num          int,
# MAGIC am_stat              int,
# MAGIC am_stat_dt          timestamp,
# MAGIC am_ste_cd_appl      string,
# MAGIC am_ste_cd_reg      string,
# MAGIC am_tot_case_act      int,
# MAGIC am_tot_para_act      int,
# MAGIC am_tqr_empe          int,
# MAGIC am_flg_protest_ltr  int,
# MAGIC am_flg_teasrf_fil   int,
# MAGIC am_flg_teasrf_cur   int,
# MAGIC am_flg_mark_desc    int,
# MAGIC am_flg_not_elec      int,
# MAGIC am_bar_mem_yr      int,
# MAGIC am_bar_mem_mm      int,
# MAGIC am_bar_mem_state    string,
# MAGIC am_bar_mem          string,
# MAGIC am_flg_pr_audit_cur int,
# MAGIC am_pr_audit_empe    int,
# MAGIC am_pr_audit_beg_dt  timestamp,
# MAGIC am_trmntd_dt      timestamp,
# MAGIC last_modified_date  timestamp,
# MAGIC oracle_apply_time   timestamp
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.em(
# MAGIC absn              int,
# MAGIC afn                  int,
# MAGIC em_email_addr      string,
# MAGIC em_empe_nam          string,
# MAGIC em_empe_num          int,
# MAGIC em_empe_stat      int,
# MAGIC em_flg_spe          int,
# MAGIC em_gau              int,
# MAGIC em_grd              int,
# MAGIC em_grd_dt          timestamp,
# MAGIC em_org1              string,
# MAGIC em_ptonet_id        string,
# MAGIC em_rsn              double,
# MAGIC em_ssn              int,
# MAGIC em_step              int,
# MAGIC em_step_dt          timestamp,
# MAGIC last_modified_date  timestamp,
# MAGIC oracle_apply_time   timestamp
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.mas(
# MAGIC absn  int,
# MAGIC afn                  int,
# MAGIC last_modified_date  timestamp,
# MAGIC mas_ctl_num          string,
# MAGIC mas_intl_reg_num    string,
# MAGIC mas_rsn              double,
# MAGIC mas_ser_num          int,
# MAGIC oracle_apply_time   timestamp
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.mif(
# MAGIC absn                 int,
# MAGIC afn                  int,
# MAGIC last_modified_date   string,
# MAGIC mif_ctl_num          string,
# MAGIC mif_email           string,
# MAGIC mif_flg_auto_cert   int,
# MAGIC mif_ib_pub_dt       string,
# MAGIC mif_intl_reg_dt       string,
# MAGIC mif_intl_reg_num   string,
# MAGIC mif_reply_by_dt       string,
# MAGIC mif_orig_fil_dt       string,
# MAGIC mif_ram_sale       int,
# MAGIC mif_rnwl_dt           string,
# MAGIC mif_rsn               double,
# MAGIC mif_stat           int,
# MAGIC mif_stat_dt           string,
# MAGIC mif_type_pay       string,
# MAGIC oracle_apply_time   string
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);
# MAGIC
# MAGIC CREATE TABLE  if not exists ${conf.catalog}.${conf.database}.tt(
# MAGIC absn              int,
# MAGIC afn                  int,
# MAGIC last_modified_date  timestamp,
# MAGIC oracle_apply_time   timestamp,
# MAGIC tt_ent_cd          string,
# MAGIC tt_ent_key          string,
# MAGIC tt_ent_num          int,
# MAGIC tt_rsn              double,
# MAGIC tt_text_1          string,
# MAGIC tt_text_2          string
# MAGIC )
# MAGIC USING delta
# MAGIC TBLPROPERTIES ('databricks.delta.autocompact.enabled'=true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.control_table};
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.control_table} (
# MAGIC   src_folder string,
# MAGIC   catalog_name string,
# MAGIC   database_name string, -- bronze
# MAGIC   table_name string,
# MAGIC   source_db_name string,
# MAGIC   source_table_name string,
# MAGIC   primary_keys string,
# MAGIC   initial_load_finished boolean
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-control table

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

table_schema = spark.table(f'{databridge_catalog}.{database}.{control_table}').schema

table_data = [
]
 
df = spark.createDataFrame(data=table_data,schema=table_schema)
display(df)
df.write.mode('overwrite').saveAsTable(f'{databridge_catalog}.{database}.{control_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC #Initialize the dms-cdc-batch-job-history table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists ${conf.catalog}.${conf.database}.${conf.job_history_table};
# MAGIC
# MAGIC create table if not exists ${conf.catalog}.${conf.database}.${conf.job_history_table} (
# MAGIC   cdc_file_path string,
# MAGIC   meta_src_time long,
# MAGIC   cdc_file_date date,
# MAGIC   processing_time TIMESTAMP
# MAGIC )
# MAGIC ;
