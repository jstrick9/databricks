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
import pprint
from pyspark.sql.functions import *
import pytz
from pytz import timezone
import datetime

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

# DBTITLE 1,Helper for columns
identity_columns = ["id"]
scd_columns = [
    "last_modified_timestamp",
    "begin_effective_timestamp",
    "end_effective_timestamp",
]
common_columns = set(scd_columns + identity_columns)
columns = {
    "dim_account": [
        column
        for column in spark.sql(
            f"select * from {target_catalog}.silver.dim_account"
        ).columns
        if column not in common_columns
    ],
    "dim_address": [
        column
        for column in spark.sql(
            f"select * from {target_catalog}.silver.dim_address"
        ).columns
        if column not in common_columns
    ],
    "dim_email": [
        column
        for column in spark.sql(
            f"select * from {target_catalog}.silver.dim_email"
        ).columns
        if column not in common_columns
    ],
    "dim_practitioner": [
        column
        for column in spark.sql(
            f"select * from {target_catalog}.silver.dim_practitioner"
        ).columns
        if column not in common_columns
    ],
    "dim_telecom": [
        column
        for column in spark.sql(
            f"select * from {target_catalog}.silver.dim_telecom"
        ).columns
        if column not in common_columns
    ],
}
pprint.pprint(common_columns)
pprint.pprint(columns)

# COMMAND ----------

# DBTITLE 1,Base Query
spark.sql(f"""
select
  iff(
    ip.interested_party_gid is null,
    ip.interested_party_gid,
    md5(concat_ws("", tpr.tm_party_role_id, ip.interested_party_gid))
  ) practitioner_id,
  tpr.cfk_patron_id fk_account_id,
  iff(
    tmea.electronic_address_gid is null,
    tmea.electronic_address_gid,
    md5(concat_ws("", tpr.tm_party_role_id, tmea.electronic_address_gid))
  ) fk_email_id,
  iff(
    tmma.mailing_address_gid is null,
    tmma.mailing_address_gid,
    md5(concat_ws("", tpr.tm_party_role_id, tmma.mailing_address_gid))
  ) fk_address_id,
  iff(
    tmta.telecom_address_gid is null,
    tmta.telecom_address_gid,
    md5(concat_ws("", tpr.tm_party_role_id, tmta.telecom_address_gid))
  ) fk_telecom_id,
  ip.interested_party_nm `name`,
  tpr.bar_information_tx bar_identity,
  tpr.bar_membership_state_cd bar_state,
  iff(
    tpr.create_ts >= "2019-08-03"
    and tpr.fk_tm_party_role_cd = 'AT',
    'Y',
    'N'
  ) bar_identity_enforced,
  tpr.fk_tm_party_role_cd role_type,
  tmta.telecom_no telecom_number,
  tmta.extension_no telecom_extension_number,
  tmta.fk_telecom_format_cd telecom_format_code,
  tmta.fk_telecom_type_cd telecom_type_code,
  tmea.electronic_addr_locator_tx email,
  tmea.fk_electronic_addr_type_cd email_code,
  tmma.city_nm city_name,
  tmma.country_cd country_code,
  tmma.geographic_region_cd state_code,
  tmma.postal_cd postal_code,
  tmma.street_line_1_tx street_line_one,
  tmma.street_line_2_tx street_line_two
from
  {trm_tmngpdb_catalog}.bronze.tm_party_role tpr
    left join (
      select
        ip.interested_party_gid,
        ip.interested_party_nm
      from
        {trm_tmngpdb_catalog}.bronze.interested_party ip
    ) ip
      on tpr.fk_interested_party_gid = ip.interested_party_gid
    left join (
      select
        tmta.fk_tm_party_role_id,
        ta.telecom_address_gid,
        ta.telecom_no,
        ta.extension_no,
        ta.fk_telecom_format_cd,
        ta.fk_telecom_type_cd
      from
        {trm_tmngpdb_catalog}.bronze.tm_telecom_addr tmta
          join {trm_tmngpdb_catalog}.bronze.telecom_address ta
            on tmta.fk_telecom_address_gid = ta.telecom_address_gid
      where
        telecom_no is not null
    ) tmta
      on tpr.tm_party_role_id = tmta.fk_tm_party_role_id
    left join (
      select
        tmma.fk_tm_party_role_id,
        ma.mailing_address_gid,
        ma.city_nm,
        ma.country_cd,
        ma.geographic_region_cd,
        ma.postal_cd,
        ma.street_line_1_tx,
        ma.street_line_2_tx
      from
        {trm_tmngpdb_catalog}.bronze.tm_mailing_addr tmma
          join {trm_tmngpdb_catalog}.bronze.mailing_address ma
            on tmma.fk_mailing_address_gid = ma.mailing_address_gid
    ) tmma
      on tpr.tm_party_role_id = tmma.fk_tm_party_role_id
    left join (
      select
        tmea.fk_tm_party_role_id,
        ea.electronic_address_gid,
        ea.electronic_addr_locator_tx,
        ea.fk_electronic_addr_type_cd
      from
        {trm_tmngpdb_catalog}.bronze.tm_electronic_addr tmea
          join {trm_tmngpdb_catalog}.bronze.electronic_address ea
            on tmea.fk_electronic_address_gid = ea.electronic_address_gid
      where
        ea.electronic_addr_locator_tx is not null
    ) tmea
      on tpr.tm_party_role_id = tmea.fk_tm_party_role_id
where
  tpr.fk_tm_party_role_cd in ('AT', 'DR')
"""
).createOrReplaceTempView("base")
display(spark.sql("select * from base").limit(5))

# COMMAND ----------

# DBTITLE 1,dim_account
dim_account = spark.sql(
    f"""
select
  * except (rn)
from
  (
    select
      lower(pi.patron_id) account_id,
      initcap(concat_ws(' ', pi.given_nm, pi.middle_nm, pi.family_nm)) account_patron_name,
      nickname_nm account_patron_nickname,
      pi.user_acct_nm account_username,
      pi.electronic_addr_locator_tx account_email,
      user_acct_status_tx account_status,
      pi.src_create_ts < '2021-04-25' account_created_before_verification_enforced,
      iff(
        min(pi.src_create_ts) over (partition by pi.patron_id) < min(pi.bgn_dt) over (
            partition by pi.patron_id
          ),
        min(pi.src_create_ts) over (partition by pi.patron_id),
        min(pi.bgn_dt) over (partition by pi.patron_id)
      ) account_creation_timestamp,
      row_number() over (partition by patron_id order by dim_patron_id desc) rn
    from
      {target_catalog}.bronze.dim_patron pi
    where
      pi.acct_type_cd = 'X'
  )
where
  rn = 1
"""
)
display(dim_account.limit(5))

# COMMAND ----------

# DBTITLE 1,dim_practitioner
dim_practitioner = spark.sql(
    f"""
    select distinct
        practitioner_id, 
        fk_account_id,
        fk_telecom_id,
        fk_email_id,
        fk_address_id,
        role_type,
        `name`,
        bar_identity,
        bar_state,
        bar_identity_enforced
    from
        base
    where
        practitioner_id is not null
"""
)

# include names that have at least one alphabetic character
dim_practitioner = dim_practitioner.filter("regexp_extract(name, '[a-zA-Z]+', 0) != ''")

# replace backslash
dim_practitioner = dim_practitioner.withColumn(
    "name", regexp_replace("name", "[\\\\/]", " ")
)

# replace multi spaces with single space
dim_practitioner = dim_practitioner.withColumn(
    "name", regexp_replace("name", " +", " ")
)

# trim
dim_practitioner = dim_practitioner.withColumn("name", trim("name"))

# upper case
dim_practitioner = dim_practitioner.withColumn("name", upper("name"))

# Replace ESQUIRE
dim_practitioner = dim_practitioner.withColumn(
    "name",
    regexp_replace("name", "(?i)\\b(ESQUIRE)\\b", "ESQ"),
)

# Add periods for middle names
dim_practitioner = dim_practitioner.withColumn(
    "name",
    regexp_replace("name", "(\\b[A-Za-z]\\b)(?=\\s+[A-Za-z])", "$1."),
)

# remove names where contiguous characters are numeric
dim_practitioner = dim_practitioner.withColumn(
    "name",
    regexp_replace("name", "([0-9]+[A-Za-z]+|[A-Za-z]+[0-9]+)", ""),
)

# extract common name titles
dim_practitioner = dim_practitioner.withColumn(
    "suffix",
    trim(regexp_extract("name", "(?i)[\\s,](JR|SR|JR.|SR.|I|II|III|IV)\\s*$", 0)),
)

# extract common professional titles
dim_practitioner = dim_practitioner.withColumn(
    "professional_title",
    regexp_extract("name", "(?i)\\b(PHD|MSW|ESQ|PH.D|M.S.W.|ESQ)\\b", 0),
)

# extract common professional titles
dim_practitioner = dim_practitioner.withColumn(
    "name",
    regexp_replace("name", "[^a-zA-Z ]", ""),
)

# remove empty strings
dim_practitioner = dim_practitioner.withColumn(
    "suffix",
    trim(when(col("suffix") == "", None).otherwise(col("suffix"))),
)

# remove empty strings
dim_practitioner = dim_practitioner.withColumn(
    "professional_title",
    trim(when(col("professional_title") == "", None).otherwise(col("professional_title"))),
)

# ignore blank names produced by above
dim_practitioner = dim_practitioner.where("name != ''")

# remove reg no
dim_practitioner = dim_practitioner.withColumn(
    "name", regexp_replace(col("name"), "REG NO", "")
)

# remove BAR info
dim_practitioner = dim_practitioner.withColumn(
    "name",
    regexp_replace(
        "name",
        "(\s\w{2}\sBAR.*)|(A MEMBER OF THE BAR.*)|(ATTORNEY AND CALIFORNIA BAR MEMBER)|(STATE BAR OF CA)|(CA STATE BAR NO)|(CALIFORNIA STATE BAR NO)|(GA STATE BAR NO)|(WISCONSIN BAR MEMBER)|(WISCONSIN BAR MEMBER)|(CALIFORNIA BAR NO)|(ILLINOIS BAR MEMBER)|(ARIZONA BAR MEMBER)|(ATTORNEY OF RECORD)|(NY AND)|(CONNECTICUT BAR MEMBER)|(WHO IS)",
        "",
    ),
)

# trim final string and remove duplicate ws
dim_practitioner = dim_practitioner.withColumn("name", trim(regexp_replace("name", '\\s+', ' ')))

# remove non-alphanumeric characters except for spaces and dashes
dim_practitioner = dim_practitioner.withColumn(
    "bar_identity", regexp_replace(upper(trim("bar_identity")), "[^a-zA-Z0-9 -]", "")
)

# remove whitespaces
dim_practitioner = dim_practitioner.withColumn("bar_identity", trim("bar_identity"))

# distinct
dim_practitioner = dim_practitioner.select(
    [
        "practitioner_id",
        "fk_account_id",
        "fk_telecom_id",
        "fk_email_id",
        "fk_address_id",
        "role_type",
        "name",
        "bar_identity",
        "bar_state",
        "bar_identity_enforced",
        "suffix",
        "professional_title",
    ]
).distinct()

display(dim_practitioner.limit(5))

# COMMAND ----------

# DBTITLE 1,dim_telecom
dim_telecom = spark.sql(
    f"""
    select distinct
      fk_telecom_id telecom_id,
      practitioner_id fk_practitioner_id,
      telecom_number,
      telecom_extension_number,
      telecom_format_code,
      telecom_type_code
    from
      base
    where 
      fk_telecom_id is not null
      and practitioner_id is not null
"""
)

display(dim_telecom.limit(5))

# COMMAND ----------

# DBTITLE 1,dim_email
dim_email = spark.sql(
    f"""
    select distinct
        fk_email_id email_id,
        practitioner_id fk_practitioner_id,
        email,
        email_code
    from
        base
    where 
        fk_email_id is not null
        and practitioner_id is not null
    """
)

# add domain
dim_email = dim_email.withColumn("email_domain", element_at(split("email", "@"), -1))

dim_email = dim_email.distinct()
display(dim_email.limit(5))

# COMMAND ----------

# DBTITLE 1,dim_address
dim_address = spark.sql(
    f"""
    select distinct
        fk_address_id address_id,
        practitioner_id fk_practitioner_id,
        trim(upper(regexp_replace(country_code,' +', ' '))) country_code,
        trim(upper(regexp_replace(state_code,' +', ' '))) state_code,
        trim(initcap(regexp_replace(city_name,' +', ' '))) city_name,
        trim(regexp_replace((regexp_replace(postal_code,' +', ' ')), '[^a-zA-Z0-9 -]', '')) postal_code,
        trim(initcap(regexp_replace(street_line_one,' +', ' '))) street_line_one,
        trim(initcap(regexp_replace(street_line_two,' +', ' '))) street_line_two
    from
        base
    where 
        fk_address_id is not null
        and practitioner_id is not null
    """
)

dim_address = dim_address.distinct()
display(dim_address.limit(5))

# COMMAND ----------

# DBTITLE 1,Insert dim_account
dim_account.createOrReplaceTempView("dim_account_incoming")
table_columns = ", ".join(columns["dim_account"])
display(
    spark.sql(
        f"""
    insert overwrite
        {target_catalog}.silver.dim_account ({table_columns})
    select 
        {table_columns}
    from 
        dim_account_incoming
    """
    )
)
display(
    spark.sql(f"select * from {target_catalog}.silver.dim_account").limit(5)
)

# COMMAND ----------

# DBTITLE 1,Insert dim_practitioner
dim_practitioner.createOrReplaceTempView("dim_practitioner_incoming")
table_columns = ", ".join(columns["dim_practitioner"])
display(
    spark.sql(
        f"""
    insert overwrite
        {target_catalog}.silver.dim_practitioner ({table_columns})
    select 
        {table_columns}
    from 
        dim_practitioner_incoming
    """
    )
)
display(
    spark.sql(f"select * from {target_catalog}.silver.dim_practitioner").limit(5)
)

# COMMAND ----------

# DBTITLE 1,Insert dim_telecom
dim_telecom.createOrReplaceTempView("dim_telecom_incoming")
table_columns = ", ".join(columns["dim_telecom"])
display(
    spark.sql(
        f"""
    insert overwrite
        {target_catalog}.silver.dim_telecom ({table_columns})
    select 
        {table_columns}
    from 
        dim_telecom_incoming
    """
    )
)
display(
    spark.sql(f"select * from {target_catalog}.silver.dim_telecom").limit(5)
)

# COMMAND ----------

# DBTITLE 1,Insert dim_email
dim_email.createOrReplaceTempView("dim_email_incoming")
table_columns = ", ".join(columns["dim_email"])
display(
    spark.sql(
        f"""
    insert overwrite
        {target_catalog}.silver.dim_email ({table_columns})
    select 
        {table_columns}
    from 
        dim_email_incoming
    """
    )
)
display(
    spark.sql(f"select * from {target_catalog}.silver.dim_email").limit(5)
)

# COMMAND ----------

# DBTITLE 1,Insert dim_address
dim_address.createOrReplaceTempView("dim_address_incoming")
table_columns = ", ".join(columns["dim_address"])
display(
    spark.sql(
        f"""
    insert overwrite
        {target_catalog}.silver.dim_address ({table_columns})
    select 
        {table_columns}
    from 
        dim_address_incoming
    """
    )
)
display(
    spark.sql(f"select * from {target_catalog}.silver.dim_address").limit(5)
)
