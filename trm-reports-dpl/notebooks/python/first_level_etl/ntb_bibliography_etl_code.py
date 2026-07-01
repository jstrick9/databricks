# Databricks notebook source
from pyspark.sql.functions import date_trunc

# COMMAND ----------

# DBTITLE 1,Setting environment
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
#config_file = "/Workspace/Users/Pawanpreet.Sangari@USPTO.GOV/bdr-trm-reports-dpl-tm-expired_prod_fix/notebooks/config/dev/trmreports-conf.yaml"

print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

# DBTITLE 1,Setting Config Param
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
edw_scope = common_configs['secrets']['edw_scope']
# print(reporting_catalog)
# print(tmngpdb_catalog)
schema_bronze = "bronze"
schema_silver = "silver"
table_silver= "bibliography"
print(tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,# Added Status_dt as part of user story US565540
input_1_df = spark.sql(f"""
Select
  CAST(split(TM.TRADEMARK_GID, ':')[2] AS INTEGER) AS AM_SER_NUM,
  TM.FILING_DT as AM_DT_FIL,
  TM.STATUS_DT, --Added as part of user story US565540 
  CASE
    WHEN tm.registration_num IS NULL THEN 0
    ELSE TM.REGISTRATION_NUM
  END as AM_REG_NUM,
  NVL(lcs.cfk_asgnd_exam_law_ofc_org_cd, ' ') as AM_LO_ASGN,
  fb_use.AM_FLG_USE_CUR,
  fb_use.AM_FLG_USE_FIL,
  fb_44d.AM_FLG_44D_AMED,
  fb_44d.AM_FLG_44D_CUR,
  fb_44d.AM_FLG_44D_FIL,
  fb_44e.AM_FLG_44E_AMED,
  fb_44e.AM_FLG_44E_CUR,
  fb_44e.AM_FLG_44E_FIL,
  fb_66a.AM_FLG_66A_CUR,
  fb_66a.AM_FLG_66A_FIL,
  fb_nb.AM_FLG_NO_BAS_CUR,
  fb_nb.AM_FLG_NO_BAS_FIL,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASR' THEN 1
    ELSE 0
  END AS AM_FLG_TEASRF_FIL,
  CASE
    WHEN TM.FK_FEE_PROCESS_TYPE_CD = 'TEASR' THEN 1
    ELSE 0
  END AS AM_FLG_TEASRF_CUR,
  CASE
    WHEN TM.FK_FEE_PROCESS_TYPE_CD = 'TEASP' THEN 1
    ELSE 0
  END AS AM_FLG_TEASPL_CUR,
  CASE
    WHEN TM.FK_FILED_FEE_PROCESS_TYPE_CD = 'TEASP' THEN 1
    ELSE 0
  END AS AM_FLG_TEASPL_FIL,
  fb_itu.AM_FLG_ITU_AMED,
  fb_itu.AM_FLG_ITU_CUR,
  fb_itu.AM_FLG_ITU_FIL,
  tm.legacy_status_cd as AM_STAT,
  CASE
    WHEN TM.FK_MARK_DRAWING_TYPE_CD = 0 THEN NULL
    ELSE TM.FK_MARK_DRAWING_TYPE_CD
  END as AM_MARK_DWG_CD,
  CASE
    WHEN TM.STANDARD_CHARACTER_TX IS NULL THEN nvl(TRIM(substr(lit.literal_element_tx, 0, 40)), ' ')
    ELSE TRIM(substr(TM.STANDARD_CHARACTER_TX, 0, 40))
  END as AM_MARK_1_LIN,
  TM.LAST_MOD_TS AS LAST_MODIFIED_DATE,
  CASE
    WHEN TME.CFK_EMPLOYEE_NO IS NULL THEN 0
    ELSE CAST(tme.cfk_employee_no AS integer)
  END as AM_EXMR_NUM
from
  {tmngpdb_catalog}.{schema_bronze}.TRADEMARK TM
  left join -- locations
  (
    select
      fk_trademark_gid,
      cfk_asgnd_exam_law_ofc_org_cd
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_LOCATIONS
  ) lcs on tm.trademark_gid = lcs.fk_trademark_gid
  left join -- use / 1a
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_USE_CUR,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_USE_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = '1(a)'
  ) fb_use on tm.trademark_gid = fb_use.fk_trademark_gid
  left join -- 44d
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_44D_CUR,
      case
        when amended_in = 'Y' then 1
        else 0
      end as AM_FLG_44D_AMED,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_44D_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = '44(d)'
  ) fb_44d on tm.trademark_gid = fb_44d.fk_trademark_gid
  left join --44e
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_44E_CUR,
      case
        when amended_in = 'Y' then 1
        else 0
      end as AM_FLG_44E_AMED,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_44E_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = '44(e)'
  ) fb_44e on tm.trademark_gid = fb_44e.fk_trademark_gid
  left join --66a
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_66A_CUR,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_66A_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = '66(a)'
  ) fb_66a on tm.trademark_gid = fb_66a.fk_trademark_gid
  left join --NOBAS
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_NO_BAS_CUR,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_NO_BAS_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = 'NOBAS'
  ) fb_nb on tm.trademark_gid = fb_nb.fk_trademark_gid
  left join -- ITU / 1b
  (
    select
      fk_trademark_gid,
      case
        when current_in = 'Y' then 1
        else 0
      end as AM_FLG_ITU_CUR,
      case
        when amended_in = 'Y' then 1
        else 0
      end as AM_FLG_ITU_AMED,
      case
        when filed_in = 'Y' then 1
        else 0
      end as AM_FLG_ITU_FIL
    from
      {tmngpdb_catalog}.{schema_bronze}.TM_FILING_BASIS
    where
      fk_filing_basis_cd = '1(b)'
  ) fb_itu on tm.trademark_gid = fb_itu.fk_trademark_gid
  left join -- literal
  (
    select
      fk_trademark_gid,
      literal_element_tx
    from
      {tmngpdb_catalog}.{schema_bronze}.tm_literal
  ) lit on tm.trademark_gid = lit.fk_trademark_gid -- employee assigment
  left join (
    select
      fk_trademark_gid,
      cfk_employee_no
    from
      {tmngpdb_catalog}.{schema_bronze}.tm_employee_assignment
    where
      fk_tm_employee_role_cd = 'EA'
  ) tme on tm.trademark_gid = tme.fk_trademark_gid
""")

# COMMAND ----------

input_2_df = spark.sql(f"""
SELECT
  CREATE_TS AS LAST_MODIFIED_DATE,
  1 AS VT_ENT_NUM,
  CAST(
    split(TMR.FK_PARENT_TRADEMARK_GID, ':')[2] AS INTEGER
  ) AS VT_SER_NUM,
  CAST(
    split(TMR.FK_RELATED_TRADEMARK_GID, ':')[2] AS INTEGER
  ) AS VT_TEXT,
  'TNSF00' AS VT_TEXT_TYPE
from
  {tmngpdb_catalog}.{schema_bronze}.TM_RELATIONSHIP TMR
WHERE
  TMR.FK_RELATIONSHIP_TYPE_CD = 'TNSF'
""")

# COMMAND ----------

input_3_df = spark.sql(f"""
SELECT
  CAST(split(FK_TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM,
  SUBSTR(literal_element_tx, 41) AS VT_TEXT,
  1 AS VT_ENT_NUM
FROM
  {tmngpdb_catalog}.{schema_bronze}.TM_LITERAL
WHERE
  LENGTH(literal_element_tx) > 40
UNION
SELECT
  CAST(split(TRADEMARK_GID, ':')[2] AS INTEGER) AS VT_SER_NUM,
  SUBSTR(standard_character_tx, 41) AS VT_TEXT,
  1 AS VT_ENT_NUM
FROM
  {tmngpdb_catalog}.{schema_bronze}.TRADEMARK
WHERE
  LENGTH(standard_character_tx) > 40
""")

# COMMAND ----------

vw_law_offices = spark.sql(f"select * from {reporting_catalog}.silver.vw_law_offices")

# COMMAND ----------

input_edw_df = f"""SELECT DISTINCT
SUM(FEE_AM),
LISTAGG(FEE_CD),
case WHEN LISTAGG(FEE_CD) LIKE '%7018%' 
          OR LISTAGG(FEE_CD) LIKE '%7019%' 
          OR LISTAGG(FEE_CD) LIKE '%7020%'  then 'Y' else 'N' end as SRCHRG_IND,
TRAN_PSTNG_REF_TX
FROM DW.VW_FPNG_SALE
WHERE
( LPAD(TRAN_PSTNG_REF_TX,1)='7'
OR   LPAD(TRAN_PSTNG_REF_TX,1)='8'
OR   LPAD(TRAN_PSTNG_REF_TX,1)='9'
OR   LPAD(TRAN_PSTNG_REF_TX,1)='A')
AND LENGTH(TRAN_PSTNG_REF_TX) = 8
AND FEE_CD IN ('7017','7018','7019','7020')
GROUP BY TRAN_PSTNG_REF_TX """

new_fees_edw_df = read_data_from_oracle_conn_dsu_cmn(input_edw_df,edw_scope)
#new_fees.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Tranformation

# COMMAND ----------

input_2_df = (
    input_2_df
        .select(
            col("LAST_MODIFIED_DATE").alias("TRANS_DT"),
            col("VT_ENT_NUM"),
            col("VT_SER_NUM").alias("SER_NUM"),
            col("VT_TEXT").alias("TRANSFORMED_SER_NUM"),
            col("VT_TEXT_TYPE")
        )
        .orderBy("SER_NUM")
)

# COMMAND ----------

input_3_df = (
    input_3_df
    .orderBy(
        "VT_SER_NUM" 
        #"VT_ENT_NUM"# Review_comment: does not need order by on  VT_ENT_NUM as it is hard coded to 1
    )
)

# COMMAND ----------

#review comment: Does not need this logic. as VT_TEXT is concatenated in TRM database

# input_3_df_group_by = (
#     input_3_df
#     .groupBy(
#         col("VT_SER_NUM")
#     )
#     .agg(
#         concat_ws(
#             ", ",
#             collect_list(col("VT_TEXT"))
#         ).alias("VT_MARK_NM")
#     )
# )

# COMMAND ----------

joined_inp1_inp2 = (
    input_1_df
        .join(
            input_2_df,
            on = [
                col("AM_SER_NUM") == col("SER_NUM")
            ],
            how="left"
        )
        .withColumn("MARK_DWG_CD", regexp_extract("AM_MARK_DWG_CD", r'(\d)', 1))
)

# COMMAND ----------

joined_inp1_inp2_selected = (
    joined_inp1_inp2
    .select(
        col("AM_SER_NUM").alias("SERIAL_NUMBER"),
        col("AM_DT_FIL").alias("FILING_DATE"),
        col("AM_REG_NUM").alias("REGISTRATION_NUMBER"),
        col("AM_LO_ASGN"),
        col("AM_FLG_USE_CUR"),
        col("AM_FLG_USE_FIL"),
        col("AM_FLG_44D_AMED"),
        col("AM_FLG_44D_CUR"),
        col("AM_FLG_44D_FIL"),
        col("AM_FLG_44E_AMED"),
        col("AM_FLG_44E_CUR"),
        col("AM_FLG_44E_FIL"),
        col("AM_FLG_66A_CUR"),
        col("AM_FLG_66A_FIL"),
        col("AM_FLG_NO_BAS_CUR"),
        col("AM_FLG_NO_BAS_FIL"),
        col("AM_FLG_TEASRF_FIL"),
        col("AM_FLG_TEASRF_CUR"),
        col("AM_FLG_TEASPL_CUR"),
        col("AM_FLG_TEASPL_FIL"),
        col("AM_FLG_ITU_AMED"),
        col("AM_FLG_ITU_CUR"),
        col("AM_FLG_ITU_FIL"),
        col("AM_STAT"),
        col("SER_NUM"),
        col("TRANSFORMED_SER_NUM"),
        col("TRANS_DT"),
        col("VT_ENT_NUM"),
        col("VT_TEXT_TYPE"),
        col("AM_MARK_DWG_CD"),
        col("AM_MARK_1_LIN"),
        col("MARK_DWG_CD"),
        col("LAST_MODIFIED_DATE"),
        col("AM_EXMR_NUM"),
        col("STATUS_DT")  # Added As part of task US565540
    )
)

# COMMAND ----------

### add in old hard coded law offices to main table

og_lo_dict = {
    'J10': '101',
    'J20': '102',
    'J30': '103',
    'J40': '104',
    'J50': '105',
    'J60': '106',
    'J70': '107',
    'J80': '108',
    'G10': '109',
    'G20': '110',
    'G30': '111',
    'G40': '112',
    'G50': '113',
    'G60': '114',
    'G70': '115',
    'G80': '116'
    }

df_og_lo = spark.createDataFrame(list(og_lo_dict.items()), schema=['law_office_cd', 'law_office_num'])

vw_law_offices = vw_law_offices.select("law_office_cd", "law_office_num")

law_offices = vw_law_offices.unionByName(df_og_lo)

# COMMAND ----------

case1 = (
    joined_inp1_inp2_selected.join(
        law_offices, joined_inp1_inp2_selected.AM_LO_ASGN == law_offices.law_office_cd, "left"
    ).withColumn(
        "MARK_DWG_DESC",
        when(col("MARK_DWG_CD") == "1", "TYPESET WORD(S)/LETTER(S)/NUMBER(S)")
        .when(col("MARK_DWG_CD") == "2", "AN ILLUSTRATION DRAWING WITHOUT ANY WORD(S)/LETTER(S)/NUMBER(S)")
        .when(col("MARK_DWG_CD") == "3", "AN ILLUSTRATION DRAWING WHICH INCLUDES WORD(S)/LETTER(S)/NUMBER(S)")
        .when(col("MARK_DWG_CD") == "4", "STANDARD CHARACTER MARK")
        .when(col("MARK_DWG_CD") == "5", "AN ILLUSTRATION DRAWING WITH WORD(S)/LETTER(S)/NUMBER(S) IN STYLIZED FORM")
        .when(col("MARK_DWG_CD") == "6", "NO DRAWING_SENSORY MARK")
    ).withColumnRenamed(
        'law_office_num', 'LAW_OFFICE'
    ).drop('law_office_cd')
)

# COMMAND ----------

case2 = (
    case1
    .withColumn(
        'FILING_METHOD_CUR_OLD', 
        expr("""
            CASE WHEN (AM_FLG_66A_CUR=1 OR StartsWith(SERIAL_NUMBER,'79'))
            THEN 'MADRID'
            WHEN AM_FLG_TEASPL_CUR=1
            THEN 'TEAS PLUS'
            WHEN AM_FLG_TEASRF_CUR=1
            THEN 'TEAS RF' 
            WHEN Startswith(SERIAL_NUMBER, '76')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '75')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '74')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '73')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '72')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '71')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '89')
            THEN '6 TER'
            ELSE 'TEAS'
            END
        """)
    )
    .withColumn(
        'FILING_METHOD_FILED_OLD', 
        expr("""
            CASE WHEN (AM_FLG_66A_FIL=1 OR StartsWith(SERIAL_NUMBER,'79'))
            THEN 'MADRID'
            WHEN AM_FLG_TEASPL_FIL=1
            THEN 'TEAS PLUS'
            WHEN AM_FLG_TEASRF_FIL=1
            THEN 'TEAS RF' 
            WHEN Startswith(SERIAL_NUMBER, '76')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '75')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '74')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '73')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '72')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '71')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '89')
            THEN '6 TER'
            ELSE 'TEAS'
            END
        """)
    )
    .withColumn(
        'FLG_PAPER_FIL', 
        expr("""
            CASE WHEN Startswith(SERIAL_NUMBER, '76')
            THEN '1'
            WHEN Startswith(SERIAL_NUMBER, '75')
            THEN '1'
            WHEN Startswith(SERIAL_NUMBER, '74')
            THEN '1'
            WHEN Startswith(SERIAL_NUMBER, '73')
            THEN '1'
            WHEN Startswith(SERIAL_NUMBER, '72')
            THEN '1'
            WHEN Startswith(SERIAL_NUMBER, '71')
            THEN '1'
            ELSE ''
            END
        """)
    )
    .withColumn(
        'FILING_METHOD_FILED', 
        expr("""
            CASE WHEN (AM_FLG_66A_FIL=1 OR StartsWith(SERIAL_NUMBER,'79'))
            THEN 'MADRID'
            WHEN AM_FLG_TEASPL_FIL=1
            THEN 'TEAS PLUS'
            WHEN AM_FLG_TEASRF_FIL=1
            THEN 'TEAS STD' 
            WHEN Startswith(SERIAL_NUMBER, '76')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '75')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '74')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '73')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '72')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '71')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '89')
            THEN '6 TER'
            WHEN FILING_DATE>'2019-12-20' AND FILING_DATE < '2025-01-18'
            THEN 'TEAS STD'
            WHEN FILING_DATE >= '2025-01-18'
            THEN 'BASE'
            ELSE 'TEAS'
            END
        """)
    )
    .withColumn(
        'FILING_METHOD_CUR', 
        expr("""
            CASE WHEN (AM_FLG_66A_FIL=1 OR StartsWith(SERIAL_NUMBER,'79'))
            THEN 'MADRID'
            WHEN AM_FLG_TEASPL_FIL=1
            THEN 'TEAS PLUS'
            WHEN AM_FLG_TEASRF_FIL=1
            THEN 'TEAS STD' 
            WHEN Startswith(SERIAL_NUMBER, '76')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '75')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '74')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '73')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '72')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '71')
            THEN 'Paper'
            WHEN Startswith(SERIAL_NUMBER, '89')
            THEN '6 TER'
            WHEN FILING_DATE>'2019-12-20' AND FILING_DATE < '2025-01-18'
            THEN 'TEAS STD'
            WHEN FILING_DATE >= '2025-01-18'
            THEN 'BASE'
            ELSE 'TEAS'
            END
        """)
    )
)

# COMMAND ----------

case3 = (
    case2
        .withColumn(
            "FILING_BASIS_FIL_CT", 
                col("AM_FLG_USE_FIL") 
                + col("AM_FLG_44D_FIL") 
                + col("AM_FLG_44E_FIL") 
                + col("AM_FLG_66A_FIL") 
                + col("AM_FLG_NO_BAS_FIL")
                + col("AM_FLG_ITU_FIL")
        )
        .withColumn(
            "FILING_BASIS_CUR_CT", 
                col("AM_FLG_USE_CUR") 
                + col("AM_FLG_44D_CUR") 
                + col("AM_FLG_44E_CUR") 
                + col("AM_FLG_66A_CUR") 
                + col("AM_FLG_NO_BAS_CUR")
                + col("AM_FLG_ITU_CUR")
        )
        .withColumn(
            "FILING_BASIS_AMED_CT", 
            when(
                col("AM_FLG_44D_AMED") 
                + col("AM_FLG_44E_AMED")
                + col("AM_FLG_ITU_AMED") == 0, 
                None
            )
            .otherwise(
                col("AM_FLG_44D_AMED")
                + col("AM_FLG_44E_AMED")
                + col("AM_FLG_ITU_AMED")
            )
        )
)

# COMMAND ----------

case4 = (
    case3
        .withColumn(
            "FILING_BASIS_FIL",
            expr("""
                CASE 
                    WHEN AM_FLG_44D_FIL=1 AND AM_FLG_44E_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN '44D/44E/ITU'
                    WHEN AM_FLG_USE_FIL=1 AND AM_FLG_44E_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN 'USE/44E/ITU'
                    WHEN AM_FLG_USE_FIL=1 AND AM_FLG_44D_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN 'USE/44D/ITU'
                    WHEN AM_FLG_44D_FIL=1 AND AM_FLG_44E_FIL=1
                    THEN '44D/44E'
                    WHEN AM_FLG_44E_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN '44E/ITU'
                    WHEN AM_FLG_44D_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN '44D/ITU'
                    WHEN AM_FLG_USE_FIL=1 AND AM_FLG_44D_FIL=1
                    THEN 'USE/44D'
                    WHEN AM_FLG_USE_FIL=1 AND AM_FLG_44E_FIL=1
                    THEN 'USE/44E'
                    WHEN AM_FLG_USE_FIL=1 AND AM_FLG_ITU_FIL=1
                    THEN 'USE/ITU'
                    WHEN AM_FLG_NO_BAS_FIL=1
                    THEN 'NO BASIS'
                    WHEN AM_FLG_USE_FIL=1
                    THEN 'USE'
                    WHEN AM_FLG_44D_FIL=1
                    THEN '44D'
                    WHEN AM_FLG_44E_FIL=1
                    THEN '44E'
                    WHEN AM_FLG_66A_FIL=1
                    THEN 'MADRID'
                    WHEN AM_FLG_ITU_FIL=1
                    THEN 'ITU'
                    ELSE ''
                END
            """)
        )
        .withColumn(
            "FILING_BASIS_CUR",
            expr("""
            CASE 
                WHEN AM_FLG_44D_CUR=1 AND AM_FLG_44E_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN '44D/44E/ITU'
                WHEN AM_FLG_USE_CUR=1 AND AM_FLG_44E_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN 'USE/44E/ITU'
                WHEN AM_FLG_USE_CUR=1 AND AM_FLG_44D_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN 'USE/44D/ITU'
                WHEN AM_FLG_44D_CUR=1 AND AM_FLG_44E_CUR=1
                THEN '44D/44E'
                WHEN AM_FLG_44E_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN '44E/ITU'
                WHEN AM_FLG_44D_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN '44D/ITU'
                WHEN AM_FLG_USE_CUR=1 AND AM_FLG_44D_CUR=1
                THEN 'USE/44D'
                WHEN AM_FLG_USE_CUR=1 AND AM_FLG_44E_CUR=1
                THEN 'USE/44E'
                WHEN AM_FLG_USE_CUR=1 AND AM_FLG_ITU_CUR=1
                THEN 'USE/ITU'
                WHEN AM_FLG_NO_BAS_CUR=1
                THEN 'NO BASIS'
                WHEN AM_FLG_USE_CUR=1
                THEN 'USE'
                WHEN AM_FLG_44D_CUR=1
                THEN '44D'
                WHEN AM_FLG_44E_CUR=1
                THEN '44E'
                WHEN AM_FLG_66A_CUR=1
                THEN 'MADRID'
                WHEN AM_FLG_ITU_CUR=1
                THEN 'ITU'
                ELSE ''
            END
            """)
        )
        .withColumn(
            "FILING_BASIS_AMED",
            expr("""
            CASE 
                WHEN AM_FLG_44D_AMED=1 AND AM_FLG_44E_AMED=1 AND AM_FLG_ITU_AMED=1
                THEN '44D/44E/ITU'
                WHEN AM_FLG_44E_AMED=1 AND AM_FLG_ITU_AMED=1
                THEN '44E/ITU'
                WHEN AM_FLG_44D_AMED=1 AND AM_FLG_ITU_AMED=1
                THEN '44D/ITU'
                WHEN AM_FLG_44D_AMED=1
                THEN '44D'
                WHEN AM_FLG_44E_AMED=1
                THEN '44E'
                WHEN AM_FLG_ITU_AMED=1
                THEN 'ITU'
                ELSE ''
            END
            """)
        )
        .withColumn(
            "TEST_PCTRAM_LINK",
            concat(lit("https://review.tm-examcenter.aws.uspto.gov/review/"), col("SERIAL_NUMBER"))
        )
        .withColumn(
            "FILING_BASIS_GRP",
            expr("""
            CASE 
                WHEN FILING_BASIS_FIL = 'ITU' THEN 'ITU' 
                WHEN FILING_BASIS_FIL = 'USE' THEN 'USE'
                WHEN FILING_BASIS_FIL = '44D' THEN '44D'
                WHEN FILING_BASIS_FIL = '44E' THEN '44E'
                WHEN FILING_BASIS_FIL = 'MADRID' THEN 'MADRID'
                WHEN FILING_BASIS_FIL = '' THEN 'NO BASIS'
                WHEN FILING_BASIS_FIL = 'NO BASIS' THEN 'NO BASIS'
                WHEN Contains(FILING_BASIS_FIL,'ITU') AND Contains(FILING_BASIS_FIL,'USE') THEN 'MULTIPLE BASIS-ITU/USE'
                WHEN Contains(FILING_BASIS_FIL,'USE') THEN 'MULTIPLE BASIS-USE'
                WHEN Contains(FILING_BASIS_FIL,'ITU') THEN 'MULTIPLE BASIS-ITU'
                WHEN Contains(FILING_BASIS_FIL,'/') THEN 'MULTIPLE BASIS-NON ITU/USE'
                ELSE NULL
            END
            """)
        )
        .withColumn(
            "TMNG_IMAGE_LINK",
            concat(lit("http://tmng-al.uspto.gov/resting2/api/img/"), col("SERIAL_NUMBER"), lit("/Large"))
        )
        .withColumn(
            "TM_ANALYTICS_TS",
            lit(current_timestamp())
        )
)

# COMMAND ----------

case4_select = (
    case4
        .select(    
            col("SERIAL_NUMBER").alias("SER_NUM"),
            col("TEST_PCTRAM_LINK"),
            col("LAW_OFFICE"),
            col("FILING_BASIS_CUR"),
            col("FILING_METHOD_FILED"),
            col("FILING_METHOD_CUR"),
            col("FILING_BASIS_FIL"),
            col("FILING_BASIS_AMED"),
            col("REGISTRATION_NUMBER"),
            col("AM_FLG_66A_FIL"),
            col("AM_FLG_44D_FIL"),
            col("AM_FLG_44E_FIL"),
            col("FLG_PAPER_FIL"),
            col("AM_STAT"),
            col("AM_FLG_NO_BAS_FIL"),
            col("AM_FLG_TEASRF_FIL"),
            col("AM_FLG_USE_FIL"),
            col("AM_FLG_ITU_FIL"),
            col("AM_FLG_TEASPL_FIL"),
            col("AM_MARK_1_LIN"),
            col("MARK_DWG_CD"),
            col("MARK_DWG_DESC"),
            col("LAST_MODIFIED_DATE"),
            col("FILING_BASIS_GRP"),
            col("TMNG_IMAGE_LINK"),
            col("TM_ANALYTICS_TS"),
            col("AM_EXMR_NUM"),
            col("FILING_METHOD_CUR_OLD"),
            col("FILING_METHOD_FILED_OLD"),
            col("STATUS_DT"), #added the Status_dt 
    )
)

# COMMAND ----------

intermediate_left = (
    joined_inp1_inp2_selected
    .select(
        "SERIAL_NUMBER",
        "AM_MARK_1_LIN"
    )
)

final_right = (
    intermediate_left
        .join(
            input_3_df,
            on = [
                col("SERIAL_NUMBER") == col("VT_SER_NUM")
            ],
            how = "left"
        )
        .withColumn(
            "MARK_NM",
            concat_ws(
                "",
                col("AM_MARK_1_LIN"),
                col("VT_TEXT").alias("VT_MARK_NM")
            )
        )
        .withColumnRenamed(
            "AM_MARK_1_LIN",
            "MARK_NM_SHORT"
        )
)

# COMMAND ----------

final_join = (
    case4_select
        .join(
            final_right,
            on = [
                col("SER_NUM") == col("SERIAL_NUMBER")
            ],
            how = "left"
        )
        .join(
            new_fees_edw_df,
            on = [
                col("SER_NUM") == col("TRAN_PSTNG_REF_TX")
            ],
            how = "left"
        )
        .select(
            col("SER_NUM"),
            col("TEST_PCTRAM_LINK"),
            col("LAW_OFFICE"),
            col("FILING_BASIS_CUR"),
            col("FILING_METHOD_FILED"),
            col("FILING_METHOD_CUR"),
            col("FILING_BASIS_FIL"),
            col("FILING_BASIS_AMED"),
            col("REGISTRATION_NUMBER"),
            col("AM_FLG_66A_FIL"),
            col("AM_FLG_44D_FIL"),
            col("AM_FLG_44E_FIL"),
            col("FLG_PAPER_FIL"),
            col("AM_STAT"),
            col("AM_FLG_NO_BAS_FIL"),
            col("AM_FLG_TEASRF_FIL"),
            col("AM_FLG_USE_FIL"),
            col("AM_FLG_ITU_FIL"),
            col("AM_FLG_TEASPL_FIL"),
            col("LAST_MODIFIED_DATE"),
            col("FILING_BASIS_GRP"),
            col("MARK_DWG_CD"),
            col("MARK_DWG_DESC"),
            col("MARK_NM_SHORT"), 
            col("MARK_NM"),
            col("TMNG_IMAGE_LINK"),
            col("TM_ANALYTICS_TS"),
            col("AM_EXMR_NUM").alias("EXMR_EID"),
            col("STATUS_DT"), # Added as part of user story US565540
            col("SRCHRG_IND")

        )
).distinct().withColumn("create_ts", current_timestamp()).withColumn("create_user_id", lit("-1")).withColumn("update_ts", current_timestamp()).withColumn("update_user_id", lit("-1"))

#final_join.count()

# COMMAND ----------

cleansed = (
    final_join
        .select(
            trim(col("SER_NUM")).cast(IntegerType()).alias("SER_NUM"),
            trim(col("TEST_PCTRAM_LINK")).alias("TEST_PCTRAM_LINK"),
            trim(col("LAW_OFFICE")).alias("LAW_OFFICE"),
            trim(col("FILING_BASIS_CUR")).alias("FILING_BASIS_CUR"),
            trim(col("FILING_METHOD_FILED")).alias("FILING_METHOD_FILED"),
            trim(col("FILING_METHOD_CUR")).alias("FILING_METHOD_CUR"),
            trim(col("FILING_BASIS_FIL")).alias("FILING_BASIS_FIL"),
            trim(col("FILING_BASIS_AMED")).alias("FILING_BASIS_AMED"),
            trim(col("REGISTRATION_NUMBER")).alias("REGISTRATION_NUMBER"),
            trim(col("AM_FLG_66A_FIL")).cast(IntegerType()).alias("AM_FLG_66A_FIL"),
            trim(col("AM_FLG_44D_FIL")).cast(IntegerType()).alias("AM_FLG_44D_FIL"),
            trim(col("AM_FLG_44E_FIL")).cast(IntegerType()).alias("AM_FLG_44E_FIL"),
            trim(col("FLG_PAPER_FIL")).cast(IntegerType()).alias("FLG_PAPER_FIL"),
            trim(col("AM_STAT")).cast(IntegerType()).alias("AM_STAT"),
            trim(col("AM_FLG_NO_BAS_FIL")).cast(IntegerType()).alias("AM_FLG_NO_BAS_FIL"),
            trim(col("AM_FLG_TEASRF_FIL")).cast(IntegerType()).alias("AM_FLG_TEASRF_FIL"),
            trim(col("AM_FLG_USE_FIL")).cast(IntegerType()).alias("AM_FLG_USE_FIL"),
            trim(col("AM_FLG_ITU_FIL")).cast(IntegerType()).alias("AM_FLG_ITU_FIL"),
            trim(col("AM_FLG_TEASPL_FIL")).cast(IntegerType()).alias("AM_FLG_TEASPL_FIL"),
            trim(col("LAST_MODIFIED_DATE")).cast(TimestampType()).alias("LAST_MODIFIED_DATE"),
            trim(col("FILING_BASIS_GRP")).alias("FILING_BASIS_GRP"),
            trim(col("MARK_DWG_CD")).alias("MARK_DWG_CD"),
            trim(col("MARK_DWG_DESC")).alias("MARK_DWG_DESC"),
            trim(col("MARK_NM_SHORT")).alias("MARK_NM_SHORT"), 
            trim(col("MARK_NM")).alias("MARK_NM"),
            trim(col("TMNG_IMAGE_LINK")).alias("TMNG_IMAGE_LINK"),
            trim(col("TM_ANALYTICS_TS")).cast(TimestampType()).alias("TM_ANALYTICS_TS"),
            col("EXMR_EID"),
            col("STATUS_DT"), # Added as part of user story US565540
            col("create_ts") ,
            col("create_user_id"),
            col("update_ts"),
            col("update_user_id"),
            col("SRCHRG_IND")
        )
        .withColumn("STATUS_DT", when(final_join.STATUS_DT == "0001-01-01T00:00:00.000", None).otherwise(final_join.STATUS_DT)) # Added as part of user story US565540
)

# COMMAND ----------

# trim timestamps to seconds
cleansed = cleansed.withColumn(
    "LAST_MODIFIED_DATE", date_trunc("second", col("LAST_MODIFIED_DATE"))
).withColumn(
    "TM_ANALYTICS_TS", date_trunc("second", col("TM_ANALYTICS_TS"))
).withColumn(
    "STATUS_DT", date_trunc("second", col("STATUS_DT"))
) # Added as part of user story US565540 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the dataframe in silver layer 

# COMMAND ----------

print(reporting_catalog,schema_silver, table_silver)

# COMMAND ----------

#cleansed.write.saveAsTable(
#    f"{reporting_catalog}.{schema_silver}.{table_silver}", mode="overwrite"
#)

cleansed.write.mode("overwrite").format("delta").saveAsTable(f'{reporting_catalog}.{schema_silver}.{table_silver}')
