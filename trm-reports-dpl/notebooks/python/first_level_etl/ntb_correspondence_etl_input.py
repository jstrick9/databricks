# Databricks notebook source
# MAGIC %md
# MAGIC ## Overview – Correspondence ETL Inputs
# MAGIC
# MAGIC Loads the five input DataFrames (ip1–ip5) and two reference DataFrames
# MAGIC (state_info_df, cntry_info_df) consumed by the successor ETL notebook.

# COMMAND ----------

# DBTITLE 1,Config File
print(f"{config_file=}")

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

# DBTITLE 1,Config Parameters
common_configs     = read_yaml(config_file)
reporting_catalog  = common_configs['schema']['trgt_catalog']
tmngpdb_catalog    = common_configs['schema']['tmngpdb_src_catalog']
tmintltm_catalog   = common_configs['schema']['tmintltm_src_catalog']
cdc_bucket         = common_configs['cdc']['cdc_bucket']

data_layer    = "bronze"
schema_silver = "silver"
table_silver  = "correspondence"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared Temp Views

# COMMAND ----------

# DBTITLE 1,Cached Party Role DataFrame
party_role_df = spark.sql(f"""
    select *,
        cast(split(fk_trademark_gid, ':')[2] as integer) as ser_num
    from {tmngpdb_catalog}.{data_layer}.tm_party_role
    where fk_tm_party_role_cd in ('DR', 'AT', 'COR')
""").cache()
party_role_df.createOrReplaceTempView("tm_party_role_cached")

# COMMAND ----------

# DBTITLE 1,View – Correspondent Mailing Address (vw_correspondent_addr)
# Resolves the cor party role to its standard mailing address (type 's').
# Used by INPUT 2. Promoted from an inline subquery to avoid nesting.
spark.sql(f"""
    create or replace temp view vw_correspondent_addr as
    select
        pr.fk_trademark_gid                                       as cad_fk_trademark_gid,
        pr.ser_num                                                 as cad_ser_num,
        max_by(nvl(ma.name_line_1_tx,       ' '), tma.primary_in) as cad_indvl_full_nm,
        max_by(nvl(ma.name_line_2_tx,       ' '), tma.primary_in) as cad_firm_nm,
        max_by(nvl(ma.street_line_1_tx,     ' '), tma.primary_in) as cad_addr_line1_tx,
        max_by(nvl(ma.street_line_2_tx,     ' '), tma.primary_in) as cad_addr_line2_tx,
        max_by(nvl(ma.city_nm,              ' '), tma.primary_in) as cad_city_nm,
        max_by(nvl(ma.postal_cd,            ' '), tma.primary_in) as cad_pstl_cd,
        max_by(nvl(ma.country_cd,           ' '), tma.primary_in) as cad_ctry_cd,
        max_by(nvl(ma.country_nm,           ' '), tma.primary_in) as cad_ctry_nm,
        max_by(nvl(ma.geographic_region_cd, ' '), tma.primary_in) as cad_geo_rgn_cd,
        max_by(nvl(ma.geographic_region_nm, ' '), tma.primary_in) as cad_geo_rgn_nm
    from  tm_party_role_cached pr
    inner join {tmngpdb_catalog}.{data_layer}.tm_mailing_addr tma
        on tm_party_role_id = fk_tm_party_role_id
    inner join {tmngpdb_catalog}.{data_layer}.mailing_address ma
        on tma.fk_mailing_address_gid = ma.mailing_address_gid
    where pr.fk_tm_party_role_cd = 'COR'
      and ma.address_type_ct      = 'S'
    group by
        all
""")

# COMMAND ----------

# DBTITLE 1,View – Electronic Addresses (vw_electronic_addrs)
# Joins tm_electronic_addr → electronic_address once.
# Shared by INPUT 4 (at role) and INPUT 5 (cor role).
electronic_addrs_df = spark.sql(f"""
    select
        tea.fk_tm_party_role_id,
        ea.electronic_address_gid,
        ea.electronic_addr_locator_tx as vt_text,
        tea.authorized_email_in
    from {tmngpdb_catalog}.{data_layer}.tm_electronic_addr tea
    inner join {tmngpdb_catalog}.{data_layer}.electronic_address ea
        on tea.fk_electronic_address_gid = ea.electronic_address_gid
""").cache()
electronic_addrs_df.createOrReplaceTempView("vw_electronic_addrs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Queries

# COMMAND ----------

# DBTITLE 1,INPUT 1 – Domestic Representative
# Joins tm_party_role → interested_party for role code 'DR'.
ip1_df = spark.sql(f"""
    select
        'DR0000'                                              as vt_text_type,
        rtrim(ip.interested_party_nm)                        as vt_text,
        pr.ser_num                                            as vt_ser_num,
        1                                                     as vt_ent_num
    from tm_party_role_cached pr
    inner join {tmngpdb_catalog}.{data_layer}.interested_party ip
        on  pr.fk_interested_party_gid = ip.interested_party_gid
        and pr.fk_tm_party_role_cd = 'DR'
""")

# COMMAND ----------

# DBTITLE 1,INPUT 2 – Correspondent Address
# Left-joins trademark → vw_correspondent_addr → international_reg_tm.
ip2_df = spark.sql(f"""
    select
        cad.cad_ser_num,
        cad.cad_indvl_full_nm,
        cad.cad_firm_nm,
        cad.cad_addr_line1_tx,
        cad.cad_addr_line2_tx,
        cad.cad_city_nm,
        cad.cad_pstl_cd,
        cad.cad_ctry_cd,
        cad.cad_ctry_nm,
        cad.cad_geo_rgn_cd,
        cad.cad_geo_rgn_nm,
        am.external_reference_tx as am_atty_dkt_num
    from {tmngpdb_catalog}.{data_layer}.trademark am
    left join vw_correspondent_addr cad
        on am.trademark_gid = cad.cad_fk_trademark_gid
""")

# COMMAND ----------

# DBTITLE 1,INPUT 3 – Attorney Name
# Structurally identical to INPUT 1 but for role code 'AT'.
ip3_df = spark.sql(f"""
    select
        'AT0000'                                             as vt_text_type,
        rtrim(ip.interested_party_nm)                        as vt_text,
        pr.ser_num                                           as vt_ser_num,
        1                                                    as vt_ent_num,
        ' ' as last_modified_date,
        ' ' as vt_rsn
    from tm_party_role_cached pr
    inner join {tmngpdb_catalog}.{data_layer}.interested_party ip
        on  pr.fk_interested_party_gid = ip.interested_party_gid
        and pr.fk_tm_party_role_cd = 'AT'
""")

# COMMAND ----------

# DBTITLE 1,INPUT 4 – Attorney Authorization Email
# References vw_electronic_addrs (role 'AT').
ip4_df = spark.sql(f"""
    select
        'EMAT00'                                                             as vt_text_type,
        max_by(em.authorized_email_in || em.vt_text, em.authorized_email_in) as vt_text,
        pr.ser_num                                                           as vt_ser_num,
        1                                                                    as vt_ent_num
    from tm_party_role_cached pr
    inner join vw_electronic_addrs em
        on pr.tm_party_role_id = em.fk_tm_party_role_id
    where pr.fk_tm_party_role_cd = 'AT'
    group by all
""")

# COMMAND ----------

# DBTITLE 1,INPUT 5 – Correspondent Email
# References vw_electronic_addrs (role 'COR'). Prepends ';' separator
# for entry numbers > 1 so downstream concat produces a delimited list.
ip5_df = spark.sql(f"""
    select
        'EMCR00' as vt_text_type,
        case when vt_ent_num = 1
             then authorized_email_in || vt_text
             else ';' || vt_text
        end       as vt_text,
        vt_ser_num,
        vt_ent_num
    from (
        select
            em.authorized_email_in,
            em.vt_text,
            pr.ser_num                                           as vt_ser_num,
            row_number() over (
                partition by pr.fk_trademark_gid
                order by em.electronic_address_gid
            )                                                    as vt_ent_num
        from tm_party_role_cached pr
        inner join vw_electronic_addrs em
            on pr.tm_party_role_id = em.fk_tm_party_role_id
        where pr.fk_tm_party_role_cd = 'COR'
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reference Data

# COMMAND ----------

# DBTITLE 1,State & Country Reference Files
def _read_ref_csv(path: str, schema):
    """Read a header-delimited CSV reference file with ISO-8859-1 encoding."""
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("sep", ",")
        .option("encoding", "ISO-8859-1")
        .load(path)
    )


state_info_path = f"s3://{cdc_bucket}/eds/static_files/state_info.csv"
cntry_info_path = f"s3://{cdc_bucket}/eds/static_files/ste_ctry_cd.csv"
print(f"{state_info_path=}, {cntry_info_path=}")

_state_schema = StructType([
    StructField("STATE_CODE", StringType(),  True),
    StructField("STATE_NAME", StringType(),  True),
])

_cntry_schema = StructType([
    StructField("STE_CTRY_CD",             StringType(),  True),
    StructField("CTRY_NAME_CAPS",          StringType(),  True),
    StructField("CTRY_NAME",               StringType(),  True),
    StructField("Country or Area Name",    StringType(),  True),
    StructField("ISO ALPHA-2 Code",        StringType(),  True),
    StructField("ISO ALPHA-3 CODE",        StringType(),  True),
    StructField("ISO NUMERIC CODE UN M49", IntegerType(), True),
])

state_info_df = _read_ref_csv(state_info_path, _state_schema)
assert state_info_df.count() > 0, f"Reference file is empty or missing: {state_info_path}"

cntry_info_df = _read_ref_csv(cntry_info_path, _cntry_schema)
assert cntry_info_df.count() > 0, f"Reference file is empty or missing: {cntry_info_path}"

# COMMAND ----------

# DBTITLE 1,Unpersist Cached DataFrames
party_role_df.unpersist()
electronic_addrs_df.unpersist()
