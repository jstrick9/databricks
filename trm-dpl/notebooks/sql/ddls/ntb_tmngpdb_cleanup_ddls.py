# Databricks notebook source
# MAGIC %sql
# MAGIC drop catalog trm_tmngpvtdb_test cascade

# COMMAND ----------

delete_file_list = [
    "tram_oi/",
    "trademark/",
    "tm_party_role_owner/",
    "tm_party_role/",
    "tm_itu/",
    "attorney_hold/",
    "tm_locations_h/",
    "tm_post_registration/",
    "sync_tm_com_exception/",
    "tram_mc/",
    "tram_cl/",
    "tram_cac/",
    "tram_cad/",
    "tram_ni/",
    "tram_pi/",
    "tram_ema/",
    "tram_md/",
    "tram_amq/",
    "tram_tqr/",
    "tram_mad/",
    "sync_tram_trm_obj_id_mapping/",
    "tram_upd/",
    "tram_pq/",
    "tram_tp/",
    "tram_jn/",
    "tram_tytram_fn/",
    "tram_tm/",
    "tram_pxq/",
    "tram_mif/",
    "tram_dv/",
    "tram_cb/",
    "tram_em/",
    "tram_eme/",
    "tram_ath/",
    "tram_pcm/",
    "tram_amqe/",
    "sync_exceptions/",
    "sync_migration_rules/",
    "tram_ts/",
    "tram_pqc/",
    "tram_stc/",
    "tram_des/",
    "tram_oi/",
    "trademark_h/",
    "tm_additional_statement/",
    "tm_additional_statement_h/",
    "interested_party_h/",
    "interested_party sync_tranlog/",
    "tm_addl_stmnt_prior_reg_h/",
]
cdc_bucket = "bdr-databricks-app-test/eds/delta_tables/trm_tmngpvtdb_test/bronze"

# COMMAND ----------

for file_prefix in delete_file_list:
    file_path = "s3://"+cdc_bucket+"/"+file_prefix
    try:
        print(f"deleting file {file_path}")
        dbutils.fs.rm(file_path, recurse=True)
    except:
        raise Exception("delete command failed")
