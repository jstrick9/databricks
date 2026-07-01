# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import (
    regexp_replace,
    upper,
    split,
    regexp_extract,
    regexp_extract_all,
    replace,
    col,
    explode_outer,
    trim,
    when,
    element_at,
)
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Get Configurations
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Common Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configurations
common_configs = read_yaml(config_file)
target_catalog = common_configs["schema"]["tm_practitioner_catalog"]
run_env = dbx_env
print(target_catalog, run_env)

# COMMAND ----------

# DBTITLE 1,Mask
if run_env in ("dev", "test"):
    print("""
    Masking the following columns in lower environments:
        user_acct_nm
        given_nm
        family_nm
        middle_nm
        nickname_nm
        emp_no
        patron_org_nm
        electronic_addr_locator_tx
    """)

    column_selection: str = """
    dim_patron_id,
    patron_id,
    md5(user_acct_nm) user_acct_nm,
    user_acct_status_tx,
    md5(given_nm) given_nm,
    md5(family_nm) family_nm,
    md5(middle_nm) middle_nm,
    md5(nickname_nm) nickname_nm,
    md5(emp_no) emp_no,
    emp_type_nm,
    md5(patron_org_nm) patron_org_nm,
    md5(electronic_addr_locator_tx) electronic_addr_locator_tx,
    acct_type_cd,
    src_create_ts,
    src_last_mod_ts,
    bgn_dt,
    end_dt,
    load_no,
    update_ts,
    source_nm,
    distinguished_nm
    """
else:
    column_selection: str = """
    dim_patron_id,
    patron_id,
    user_acct_nm,
    user_acct_status_tx,
    given_nm,
    family_nm,
    middle_nm,
    nickname_nm,
    emp_no,
    emp_type_nm,
    patron_org_nm,
    electronic_addr_locator_tx,
    acct_type_cd,
    src_create_ts,
    src_last_mod_ts,
    bgn_dt,
    end_dt,
    load_no,
    update_ts,
    source_nm,
    distinguished_nm
    """

# COMMAND ----------

# DBTITLE 1,Insert dim_patron
dim_patron = read_data_from_oracle_conn_dsu_cmn(
    sql_query="select * from DW.DIM_PATRON", scope_name="trm_edw_secret"
)
dim_patron.createOrReplaceTempView("dim_patron")
display(
    spark.sql(
        f"""
        insert overwrite
            {target_catalog}.bronze.dim_patron
        select
            {column_selection}
        from
            dim_patron
        """
    )
)

# COMMAND ----------

# DBTITLE 1,Sanity Check: Sample
display(spark.sql(f"select * from {target_catalog}.bronze.dim_patron"))
