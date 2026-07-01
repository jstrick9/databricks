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
trm_tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
run_env = dbx_env
print(target_catalog, run_env)

# COMMAND ----------

# DBTITLE 1,Practitioner
display(
    spark.sql(
        f"""
insert
  overwrite {target_catalog}.gold.practitioner
select
  dp.`name`,
  dp.role_type,
  dp.suffix,
  dp.professional_title,
  dp.bar_identity,
  dp.bar_state,
  dp.bar_identity_enforced,
  dt.telecom_number,
  dt.telecom_extension_number,
  dt.telecom_format_code,
  dt.telecom_type_code,
  dad.country_code,
  dad.state_code,
  dad.city_name,
  dad.postal_code,
  dad.street_line_one,
  dad.street_line_two,
  de.email,
  de.email_domain,
  de.email_code,
  da.account_id,
  da.account_patron_name,
  da.account_patron_nickname,
  da.account_status,
  da.account_email,
  da.account_creation_timestamp,
  da.account_created_before_verification_enforced,
  case
    when account_id is not null then true
    else false
  end has_link
from
  {target_catalog}.silver.dim_practitioner dp
  left join {target_catalog}.silver.dim_telecom dt on dp.practitioner_id = dt.fk_practitioner_id
  left join {target_catalog}.silver.dim_address dad on dp.practitioner_id = dad.fk_practitioner_id
  left join {target_catalog}.silver.dim_email de on dp.practitioner_id = de.fk_practitioner_id
  left join {target_catalog}.silver.dim_account da on dp.fk_account_id = da.account_id
union
select
  null as `name`,
  null as role_type,
  null as suffix,
  null as professional_title,
  null as bar_identity,
  null as bar_state,
  null as bar_identity_enforced,
  null as telecom_number,
  null as telecom_extension_number,
  null as telecom_format_code,
  null as telecom_type_code,
  null as country_code,
  null as state_code,
  null as city_name,
  null as postal_code,
  null as street_line_one,
  null as street_line_two,
  null as email,
  null as email_domain,
  null as email_code,
  da.account_id,
  da.account_patron_name,
  da.account_patron_nickname,
  da.account_status,
  da.account_email,
  da.account_creation_timestamp,
  da.account_created_before_verification_enforced,
  false has_link
from
  {target_catalog}.silver.dim_account da left anti
  join {target_catalog}.silver.dim_practitioner dp on da.account_id = dp.fk_account_id
"""
    )
)
display(spark.sql(f"select * from {target_catalog}.gold.practitioner").limit(5))
