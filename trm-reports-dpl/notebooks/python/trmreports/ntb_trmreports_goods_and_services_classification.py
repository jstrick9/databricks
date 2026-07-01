# Databricks notebook source
# DBTITLE 1,Imports
import datetime
from pyspark.sql.functions import col, countDistinct, datediff, expr, when, sum, round, min, ntile, max, count, first, row_number, desc, current_date
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmngidmp_catalog = common_configs["schema"]["tmngidmp_catalog"]
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
trm_scope = common_configs["secrets"]["trm_scope"]
primary_email, cc_email = common_configs["alerting"]["goods_and_services_classification"]["email"], common_configs["alerting"]["goods_and_services_classification"]["cc"]
print(reporting_catalog, tmngpdb_catalog, tmngidmp_catalog, trm_scope, primary_email, cc_email)

# COMMAND ----------

# DBTITLE 1,Globals
EMAIL_CSS = """
    body {
        font-family: Arial, sans-serif;
        margin: 20px;
    }

    .table-container {
        display: flex;
        flex-wrap: wrap;
        gap: 20px;
    }

    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
    }

    .header {
        flex: 1;
        text-align: center;
    }

    table {
        width: 40%;
        border-collapse: collapse;
        margin: 20px 20px;
        background-color: #fff;
        box-sizing: border-box;
    }

    th, td {
        text-align: center;
        padding: 12px, 15px;
    }

    thead th {
        background-color: #99ccff;
        color: #fff;
        font-weight: bold;
    }

    tbody tr {
        border-bottom: 1px solid #ddd;
    }

    tbody tr: nth-child(even) {
        background-color: #f9f9f9;
    }

    tfoot td {
        background-color: #99ccff;
        color: #fff;
        font-weight: bold;
        text-align: right;
    }
"""

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_goods_and_services_classification"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Class
input_208 = select_13 = spark.sql(
    f"""
    select
        cl_cls_us_ct,
        cl_cls_us,
        cl_dt_stat,
        cl_flg_anoth_form,
        vt_ser_num as ser_num,
        class_status,
        vt_class as class,
        goods_and_services_desc
    from
        {reporting_catalog}.silver.class
    """
)

# COMMAND ----------

# DBTITLE 1,Trademark
input_209 = select_5 = spark.sql(
    f"""
    select 
        serial_num_tx am_ser_num, 
        legacy_status_cd am_stat 
    from 
        {tmngpdb_catalog}.bronze.trademark
    where
        legacy_status_cd in (630, 638)
    """
)

# COMMAND ----------

# DBTITLE 1,Join Class to 630/638 Statuses
join_1 = filter_2 = input_208.alias("left").join(
    other=input_209.alias("right"),
    on=[col("left.SER_NUM") == col("right.AM_SER_NUM")],
    how="inner",
)

# COMMAND ----------

# DBTITLE 1,Milestone
input_210 = select_213 = spark.sql(
    f"""
    select
        ser_num,
        first_action_dt_ph,
        am_1_actn_ct_dt,
        first_action_type,
        filing_dt,
        ib_notification_dt,
        published_dt,
        noa_dt,
        abandonment_dt,
        aban_dt_ph,
        registration_dt,
        disposal_type,
        ext1_dt,
        ext2_dt,
        ext3_dt,
        ext4_dt,
        ext5_dt,
        cancellation_dt,
        renewal_dt,
        revival_dt,
        susp_check_dt,
        am_cls_ct_actv,
        pendency_cal_start_dt,
        pendency_cal_end_dt,
        noa_registration_check,
        wgtd_1st_actn_pendency,
        first_action_cd,
        disposal_pendency,
        suspension,
        ttab,
        disposal_dt,
        dock_dt,
        am_flg_66a_cur,
        am_flg_66a_fil,
        noa_dt_ph,
        filing_fy,
        non_pro_se,
        first_action_pendency_ph,
        last_modified_date,
        processing_pend,
        processing_pend_days,
        days_in_dock
    from
        {reporting_catalog}.silver.milestone
    where
        disposal_dt is null
""")

# COMMAND ----------

# DBTITLE 1,Filing Dashboard
input_211 = select_212 = summarize_135 = spark.sql(
    f"""
    select distinct 
        ser_num, 
        filing_method_filed 
    from 
        {reporting_catalog}.gold.filings_dashboard
    where 
        filing_method_filed not in ("Paper", "MADRID")
    """
)

# COMMAND ----------

# DBTITLE 1,Join Milestone to Filings
join_133 = filter_136 = filter_129 = filter_130 = (
    select_213.alias("left")
    .join(
        other=summarize_135.alias("right"),
        on=[col("left.ser_num") == col("right.ser_num")],
        how="inner",
    )
    .select([expr("* except(right.ser_num)")])
)

# COMMAND ----------

summarize_123 = join_133.select(["left.ser_num", "pendency_cal_start_dt"]).distinct()

# COMMAND ----------

join_124 = (
    filter_2.alias("left")
    .join(
        other=summarize_123.alias("right"),
        on=[col("left.ser_num") == col("right.ser_num")],
        how="inner",
    )
    .select([expr("* except(right.ser_num)")])
)

# COMMAND ----------

summarize_125 = join_124.select(
    [expr("min(pendency_cal_start_dt) as min_pendency_cal_start_dt")]
)

# COMMAND ----------

input_214 = select_220 = summarize_60 = spark.sql(
    f"""
    select 
        ser_num,
        first(atty_nm) as first_atty_nm
    from 
        {reporting_catalog}.silver.correspondence
    group by 
        ser_num
    """
)

# COMMAND ----------

join_124 = (
    join_1.alias("left")
    .join(
        other=summarize_123.alias("right"),
        on=[col("left.ser_num") == col("right.ser_num")],
        how="inner",
    )
    .select(
        [
            "cl_cls_us_ct",
            "cl_cls_us",
            "cl_dt_stat",
            "cl_flg_anoth_form",
            "goods_and_services_desc",
            "pendency_cal_start_dt",
        ]
    )
)

# COMMAND ----------

# DBTITLE 1,Workaround For String Formatting
# MAGIC %sql
# MAGIC create
# MAGIC or replace temp view input_206 as with tram_vt as (
# MAGIC   select
# MAGIC     regexp_substr(fk_trademark_gid, '[^:]+$') vt_ser_num,
# MAGIC     'GS' || case
# MAGIC       when b.class_no = 'A' then 'A  '
# MAGIC       when b.class_no = 'B' then 'B  '
# MAGIC       when b.class_no = 'NRN' then 'NRN'
# MAGIC       else trim(to_char(b.class_no, '000'))
# MAGIC     end || '1' as vt_text_type,
# MAGIC     a.gds_srvcs_stmnt_tx as vt_text
# MAGIC   from
# MAGIC     ${config.tmngpdb_catalog}.bronze.tm_class_h a
# MAGIC     inner join ${config.tmngpdb_catalog}.bronze.stnd_class b on a.fk_class_id = b.class_id
# MAGIC   where
# MAGIC     a.gds_srvcs_stmnt_tx is not null
# MAGIC     and end_effective_ts is null
# MAGIC )
# MAGIC select
# MAGIC   vt_ser_num,
# MAGIC   vt_text,
# MAGIC   regexp_extract(vt_text_type, '([A-Z]{2})(\\d{3})(\\d{1})', 1) as vt_prefix,
# MAGIC   regexp_extract(vt_text_type, '([A-Z]{2})(\\d{3})(\\d{1})', 2) as vt_class,
# MAGIC   regexp_extract(vt_text_type, '([A-Z]{2})(\\d{3})(\\d{1})', 3) as vt_last_digit
# MAGIC from
# MAGIC   tram_vt
# MAGIC where
# MAGIC   vt_text_type like 'GS____'

# COMMAND ----------

input_206 = select_8 = formula_11 = regex_12 = sort_10 = spark.sql("select * from input_206")

# COMMAND ----------

summarize_9 = sort_10.select(
    [
        "vt_ser_num",
        "vt_class",
        expr("vt_text as goods_and_services_desc"),
    ]
)

# COMMAND ----------

text_to_columns_17 = summarize_9.select(
    [
        "vt_ser_num",
        "vt_class",
        expr(
            """
            split(
                case
                when goods_and_services_desc = '' then null
                else goods_and_services_desc
                end,
                ";"
            ) as goods_and_services_desc
            """
        ),
    ]
)

# COMMAND ----------

join_14 = (
    text_to_columns_17.alias("left")
    .join(
        other=filter_2.alias("right"),
        on=[
            (col("left.vt_ser_num") == col("right.ser_num"))
            & (col("left.vt_class") == col("right.class"))
        ],
        how="inner",
    )
    .select(
        [
            expr(
                "* except(left.goods_and_services_desc, right.goods_and_services_desc)",
            ),
            expr(
                "explode(left.goods_and_services_desc) as goods_and_services_desc"
            )
        ]
    )
)

# COMMAND ----------

data_cleansing_16 = join_14.select(
    [
        expr("* except(goods_and_services_desc)"),
        expr(
            "upper(trim(regexp_replace(goods_and_services_desc, '[^a-zA-Z0-9 ]', ''))) as goods_and_services_desc"
        ),
    ]
)

# COMMAND ----------

join_188 = summarize_189 = (
    (
        data_cleansing_16.alias("left").join(
            other=filter_130.alias("right"),
            on=[col("left.ser_num") == col("right.ser_num")],
            how="inner",
        )
    )
    .select(
        [
            expr("left.ser_num as ser_num"),
            "class",
            "class_status",
            "am_stat",
            "goods_and_services_desc",
        ]
    )
    .distinct()
)

# COMMAND ----------

formula_15 = filter_24 = join_188.select(
    [expr("* except(class)"), expr("cast(cast(class as int) as string) class")]
).where(
    "goods_and_services_desc is not null and goods_and_services_desc != '' and goods_and_services_desc != ' '"
)

# COMMAND ----------

# DBTITLE 1,TMNGIDMP ID Manual
input_19 = filter_187 = spark.sql(f"""
SELECT
  A.GOODS_SERVICES_TERM_ID,
  A.GOODS_SERVICES_TERM_ID_TX,
  A.DESCRIPTION_TX,
  A.TM5_ACCEPT_IN,
  A.TERM_CT,
  A.FK_EDITION_NO,
  A.FK_VERSION_NO,
  A.FK_RELEASE_NO,
  B.TITLE_TX,
  B.INTL_CLASS_SHORT_TITLE_TX,
  B.CLASS_NO,
  A.BEGIN_EFFECTIVE_DT,
  A.END_EFFECTIVE_DT,
  B.GOODS_SERVICES_CT,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'P'
  ) AS NOTE_TX_P,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'I'
  ) AS NOTE_TX_I,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'E'
  ) AS NOTE_TX_E,
  DECODE(
    LENGTH(c.FK_EDITION_NO),
    1,
    '0' || c.FK_EDITION_NO,
    c.FK_EDITION_NO
  ) || '-' || c.version_year_no AS EDITION_VERSION,
  D.TITLE_TX,
  A.CREATE_TS AS CREATE_DATE,
  A.CREATE_USER_ID,
  A.LAST_MOD_TS AS LAST_MOD_DATE,
  A.LAST_MOD_USER_ID,
  'NOACTION' AS ACTION_CT,
  (
    SELECT
      MAX(TITLE_TX)
    FROM
      {tmngidmp_catalog}.bronze.TAXONOMY_GROUP
    WHERE
      TAXONOMY_GROUP_ID = A.FK_TAXONOMY_GROUP_ID
  ) AS TAXONOMY_GROUP_TITLE,
  A.FK_TAXONOMY_GROUP_ID
FROM
  {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM A,
  {tmngidmp_catalog}.bronze.STND_CLASS B,
  {tmngidmp_catalog}.bronze.INTL_CLSFCN_EDN_VER C,
  {tmngidmp_catalog}.bronze.STND_TERM_STATUS D
WHERE
  A.fk_class_id = B.class_id
  AND A.FK_EDITION_NO = C.FK_EDITION_NO
  AND A.FK_VERSION_NO = C.version_no
  AND D.TERM_STATUS_CD = A.FK_TERM_STATUS_CD
  AND D.TERM_STATUS_CD <> 'R'
UNION ALL
SELECT
  A.GOODS_SERVICES_TERM_ID,
  A.GOODS_SERVICES_TERM_ID_TX,
  A.DESCRIPTION_TX,
  A.TM5_ACCEPT_IN,
  A.TERM_CT,
  A.FK_EDITION_NO,
  A.FK_VERSION_NO,
  A.FK_RELEASE_NO,
  B.TITLE_TX,
  B.INTL_CLASS_SHORT_TITLE_TX,
  B.CLASS_NO,
  A.BEGIN_EFFECTIVE_DT,
  A.END_EFFECTIVE_DT,
  B.GOODS_SERVICES_CT,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE_DRAFT
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'P'
  ) AS NOTE_TX_P,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE_DRAFT
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'I'
  ) AS NOTE_TX_I,
  (
    SELECT
      MAX(NOTE_TX)
    FROM
      {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_NOTE_DRAFT
    WHERE
      FK_GOODS_SERVICES_TERM_ID = A.GOODS_SERVICES_TERM_ID
      AND FK_GOODS_SERVICES_NOTE_CD = 'E'
  ) AS NOTE_TX_E,
  DECODE(
    LENGTH(c.FK_EDITION_NO),
    1,
    '0' || c.FK_EDITION_NO,
    c.FK_EDITION_NO
  ) || '-' || c.version_year_no AS EDITION_VERSION,
  D.TITLE_TX,
  A.CREATE_TS AS CREATE_DATE,
  A.CREATE_USER_ID,
  A.LAST_MOD_TS AS LAST_MOD_DATE,
  A.LAST_MOD_USER_ID,
  A.ACTION_CT,
  (
    SELECT
      MAX(TITLE_TX)
    FROM
      {tmngidmp_catalog}.bronze.TAXONOMY_GROUP
    WHERE
      TAXONOMY_GROUP_ID = A.FK_TAXONOMY_GROUP_ID
  ) AS TAXONOMY_GROUP_TITLE,
  A.FK_TAXONOMY_GROUP_ID
FROM
  {tmngidmp_catalog}.bronze.GOODS_SERVICES_TERM_DRAFT A,
  {tmngidmp_catalog}.bronze.STND_CLASS B,
  {tmngidmp_catalog}.bronze.INTL_CLSFCN_EDN_VER C,
  {tmngidmp_catalog}.bronze.STND_TERM_STATUS D
WHERE
  A.fk_class_id = B.class_id
  AND A.FK_EDITION_NO = C.FK_EDITION_NO
  AND A.FK_VERSION_NO = C.version_no
  AND D.TERM_STATUS_CD = A.FK_TERM_STATUS_CD
  AND D.TERM_STATUS_CD <> 'P'
  AND D.TERM_STATUS_CD <> 'R'
""")

# COMMAND ----------

append_fields_126 = filter_187.join(summarize_125)

# COMMAND ----------

formula_127 = filter_128 = append_fields_126.select(
    [
        "*",
        expr(
            """
          case 
            when  
              END_EFFECTIVE_DT is null then 1
            when 
              END_EFFECTIVE_DT > min_pendency_cal_start_dt then 1
            else 0
          end as date_remove
          """
        ),
    ]
).where("date_remove = 1")

# COMMAND ----------

summarize_48 = formula_23 = formula_127.select(
    ["description_tx", expr("cast(cast(class_no as int) as string) class_no")]
).distinct()

# COMMAND ----------

summarize_6 = formula_23.select(
  [
      "description_tx",
      expr(
        """
          concat(
            ',', 
            array_join(
              collect_set(class_no) over (
                partition by 
                  description_tx
              ),
              ","
            ), 
            ','
          ) as class_no
        """
      ),
  ]
)

# COMMAND ----------

data_cleansing_30 = summarize_6.select(
    [
        expr(
            "upper(trim(regexp_replace(description_tx, '[^a-zA-Z0-9 ]', ''))) as description_tx"
        ),
        "class_no",
    ]
)

# COMMAND ----------

summarize_26 = data_cleansing_30.select(expr("description_tx as text")).distinct()

# COMMAND ----------

# not used
formula_28 = summarize_26.withColumn("source", lit("IDM"))

# COMMAND ----------

join_20 = filter_24.alias("left").join(
    other=data_cleansing_30.alias("right"),
    on=[col("left.goods_and_services_desc") == col("right.description_tx")],
    how="inner",
)

# COMMAND ----------

formula_21 = formula_120 = filter_22 = join_20.select(
    [
        "*",
        expr("contains(class_no, ',' || class || ',') as flag"),
        expr("cast(ser_num as string) || cast(class_no as string) as unique"),
    ]
).where("flag = false")

# COMMAND ----------

if filter_22.count() == 0:
    end_job_cntl(
        f"{reporting_catalog}.silver",
        job_name,
        job_start_ts,
        "completed",
        0,
        "job completed successfully",
    )
    dbutils.notebook.exit(f"Job completed with 0 records.")

# COMMAND ----------

summarize_119 = filter_22.select(expr("count(distinct unique) as classes_issues"))

# COMMAND ----------

summarize_145 = filter_22.select(
    [
        "ser_num",
        "goods_and_services_desc",
        expr("class as filed_class"),
        expr("class_no as idm_acceptable_classes"),
    ]
).distinct()

# COMMAND ----------

formula_149 = select_151 = summarize_145.select(
    [
        expr("* except(idm_acceptable_classes)"),
        expr(
            "trim(regexp_replace(idm_acceptable_classes, '[^a-zA-Z0-9 ]', '')) as idm_acceptable_classes"
        ),
    ]
)

# COMMAND ----------

input_215 = select_216 = summarize_58 = spark.sql(
  f"""
  select distinct
    ser_num,
    country_or_area_name,
    name,
    applicant_bin,
    filing_basis_grp,
    non_pro_se,
    nvl(entity_type, 'OTHER') as entity_type,
    filing_method_filed
  from 
    {reporting_catalog}.gold.filings_dashboard
  """
)

# COMMAND ----------

join_63 = formula_120.alias("left").join(
    other=summarize_60.alias("right"),
    on=[col("left.ser_num") == col("right.ser_num")],
    how="inner",
)

# COMMAND ----------

join_61 = formula_120.alias("left").join(
    other=summarize_58.alias("right"),
    on=[col("left.ser_num") == col("right.ser_num")],
    how="inner",
)

# COMMAND ----------

summarize_62 = (
    join_61.select(
        [
            "country_or_area_name",
            expr("nvl(name, 'Uknown') as `name`"),
            "applicant_bin",
            "filing_basis_grp",
            "non_pro_se",
            "entity_type",
            "filing_method_filed",
            "unique",
        ]
    )
    .groupBy(
        [
            "country_or_area_name",
            "name",
            "applicant_bin",
            "filing_basis_grp",
            "non_pro_se",
            "entity_type",
            "filing_method_filed",
        ]
    )
    .agg(countDistinct("unique").alias("count_distinct_unique"))
)

# COMMAND ----------

append_fields_65 = summarize_62.join(summarize_119)

# COMMAND ----------

formula_196 = select_151.select(["*", expr("filed_class || ';' || idm_acceptable_classes as class_combo")])

# COMMAND ----------

summarize_199 = (
  formula_196.select(["ser_num", "class_combo"])
  .groupBy("class_combo")
  .agg(count("ser_num").alias("count"))
)

# COMMAND ----------

summarize_202 = summarize_199.select(expr("sum(count) as sum_count"))

# COMMAND ----------

append_fields_200 = summarize_199.join(summarize_202)

# COMMAND ----------

text_to_columns_201 = append_fields_200.select(
  [
    "class_combo",
    "count",
    "sum_count",
    expr("split(class_combo, ';')[0] as `1`"),
    expr("split(class_combo, ';')[1] as `2`"),
  ]
)

# COMMAND ----------

summarize_64 = (
    join_63.select(["first_atty_nm", "unique"])
    .groupBy("first_atty_nm")
    .agg(countDistinct("unique").alias("count_distinct_unique"))
)

# COMMAND ----------

append_fields_66 = summarize_64.join(summarize_119)

# COMMAND ----------

summarize_137 = formula_120.select(["ser_num", "unique", "description_tx"]).agg(
    countDistinct("ser_num").alias("Total Cases"),
    countDistinct("unique").alias("Total Classes"),
    count("description_tx").alias("Total Goods/Services"),
)

# COMMAND ----------

# DBTITLE 1,Top 5 List Summaries
window = Window.orderBy(desc("percent_of_unpaid"))

# tl5-1
formula_192 = sort_193 = record_194 = filter_195 = select_196 = (
    text_to_columns_201.select(
        [
            "*",
            expr("(count / sum_count) * 100 as percent_of_unpaid"),
        ]
    )
    .withColumn("record_id", row_number().over(window))
    .select(
        expr("`2` as `IDM Acceptable Classes`"),
        expr("`1` as `Filed Class`"),
        expr("round(percent_of_unpaid, 2) as `Percent Total`"),
    )
)

# tl5-2
summarize_73 = formula_74 = sort_75 = record_76 = filter_77 = select_78 = (
    (
        (
            append_fields_65.select(["name", "classes_issues", "count_distinct_unique"])
            .groupBy("name")
            .agg(
                sum("count_distinct_unique").alias("sum_count_distinct_unique"),
                first("classes_issues").alias("first_classes_issues"),
            )
        )
        .select(
            [
                "*",
                expr(
                    "(sum_count_distinct_unique / first_classes_issues) * 100 as percent_of_unpaid"
                ),
            ]
        )
        .withColumn("record_id", row_number().over(window))
    )
    .where("record_id <= 5")
    .select(
        expr("name as `Top Applicants`"),
        expr("sum_count_distinct_unique as `Incorrect Classes`"),
        expr("round(percent_of_unpaid, 2) as `Percent Total`"),
    )
)

# tl5-3
summarize_104 = (
    formula_105
) = sort_106 = record_107 = filter_108 = formula_118 = select_109 = (
    (
        (
            append_fields_66.select(
                ["first_atty_nm", "classes_issues", "count_distinct_unique"]
            )
            .groupBy("first_atty_nm")
            .agg(
                sum("count_distinct_unique").alias("sum_count_distinct_unique"),
                first("classes_issues").alias("first_classes_issues"),
            )
        )
        .select(
            [
                "*",
                expr(
                    "(sum_count_distinct_unique / first_classes_issues) * 100 as percent_of_unpaid"
                ),
            ]
        )
        .withColumn("record_id", row_number().over(window))
    )
    .where("record_id <= 5")
    .select(
        [
            expr(
                "case when first_atty_nm is null then 'Pro Se' else first_atty_nm end as `Top Attorney`"
            ),
            expr("sum_count_distinct_unique as `Incorrect Classes`"),
            expr("round(percent_of_unpaid, 2) as `Percent Total`"),
        ]
    )
)

# tl5-4
summarize_67 = formula_68 = sort_69 = record_70 = filter_71 = select_72 = (
    (
        (
            append_fields_65.select(
                ["country_or_area_name", "classes_issues", "count_distinct_unique"]
            )
            .groupBy("country_or_area_name")
            .agg(
                sum("count_distinct_unique").alias("sum_count_distinct_unique"),
                first("classes_issues").alias("first_classes_issues"),
            )
        )
        .select(
            [
                "*",
                expr(
                    "(sum_count_distinct_unique / first_classes_issues) * 100 as percent_of_unpaid"
                ),
            ]
        )
        .withColumn("record_id", row_number().over(window))
    )
    .where("record_id <= 5")
    .select(
        expr("country_or_area_name as `Top Countries`"),
        expr("sum_count_distinct_unique as `Incorrect Classes`"),
        expr("round(percent_of_unpaid, 2) as `Percent Total`"),
    )
)

# COMMAND ----------

# DBTITLE 1,Breakdown by Characteristics Summaries
# bc1
summarize_79 = formula_80 = sort_81 = record_82 = select_83 = (
    (
        append_fields_65.select(
            ["applicant_bin", "classes_issues", "count_distinct_unique"]
        )
        .groupBy("applicant_bin")
        .agg(
            sum("count_distinct_unique").alias("sum_count_distinct_unique"),
            first("classes_issues").alias("first_classes_issues"),
        )
    )
    .select(
        [
            "*",
            expr(
                "round((sum_count_distinct_unique / first_classes_issues) * 100, 2) as percent_of_unpaid"
            ),
        ]
    )
    .withColumn("record_id", row_number().over(window))
).select(
    expr("applicant_bin as `Applicant Type`"),
    expr("sum_count_distinct_unique as `Incorrect Classes`"),
    expr("percent_of_unpaid as `Percent Total`"),
)

# bc2
summarize_84 = formula_85 = sort_86 = record_87 = select_88 = (
    (
        append_fields_65.select(
            ["filing_basis_grp", "classes_issues", "count_distinct_unique"]
        )
        .groupBy("filing_basis_grp")
        .agg(
            sum("count_distinct_unique").alias("sum_count_distinct_unique"),
            first("classes_issues").alias("first_classes_issues"),
        )
    )
    .select(
        [
            "*",
            expr(
                "round((sum_count_distinct_unique / first_classes_issues) * 100, 2) as percent_of_unpaid"
            ),
        ]
    )
    .withColumn("record_id", row_number().over(window))
).select(
    expr("filing_basis_grp as `Basis`"),
    expr("sum_count_distinct_unique as `Incorrect Classes`"),
    expr("percent_of_unpaid as `Percent Total`"),
)

# bc3
summarize_94 = formula_95 = sort_96 = record_97 = select_98 = (
    (
        append_fields_65.select(
            ["entity_type", "classes_issues", "count_distinct_unique"]
        )
        .groupBy("entity_type")
        .agg(
            sum("count_distinct_unique").alias("sum_count_distinct_unique"),
            first("classes_issues").alias("first_classes_issues"),
        )
    )
    .select(
        [
            "*",
            expr(
                "round((sum_count_distinct_unique / first_classes_issues) * 100, 2) as percent_of_unpaid"
            ),
        ]
    )
    .withColumn("record_id", row_number().over(window))
).select(
    expr("entity_type as `Entity`"),
    expr("sum_count_distinct_unique as `Incorrect Classes`"),
    expr("percent_of_unpaid as `Percent Total`"),
)

# bc4
summarize_99 = formula_100 = sort_101 = record_102 = select_103 = (
    (
        append_fields_65.select(
            ["filing_method_filed", "classes_issues", "count_distinct_unique"]
        )
        .groupBy("filing_method_filed")
        .agg(
            sum("count_distinct_unique").alias("sum_count_distinct_unique"),
            first("classes_issues").alias("first_classes_issues"),
        )
    )
    .select(
        [
            "*",
            expr(
                "round((sum_count_distinct_unique / first_classes_issues) * 100, 2) as percent_of_unpaid"
            ),
        ]
    )
    .withColumn("record_id", row_number().over(window))
).select(
    expr("filing_method_filed as `Method`"),
    expr("sum_count_distinct_unique as `Incorrect Classes`"),
    expr("percent_of_unpaid as `Percent Total`"),
)

# bc5
summarize_89 = formula_90 = sort_91 = record_92 = select_93 = (
    (
        append_fields_65.select(
            ["non_pro_se", "classes_issues", "count_distinct_unique"]
        )
        .groupBy("non_pro_se")
        .agg(
            sum("count_distinct_unique").alias("sum_count_distinct_unique"),
            first("classes_issues").alias("first_classes_issues"),
        )
    )
    .select(
        [
            "*",
            expr(
                "round((sum_count_distinct_unique / first_classes_issues) * 100, 2) as percent_of_unpaid"
            ),
        ]
    )
    .withColumn("record_id", row_number().over(window))
).select(
    expr("non_pro_se as `NON/PRO SE`"),
    expr("sum_count_distinct_unique as `Incorrect Classes`"),
    expr("percent_of_unpaid as `Percent Total`"),
)

# COMMAND ----------

input_183 = spark.sql(
    f"select * from {reporting_catalog}.silver.goods_and_services_incorrect_classification"
)

# COMMAND ----------

formula_181 = select_151

# COMMAND ----------

union_182 = formula_181.union(input_183.select(expr("* except(run_date)")))

# COMMAND ----------

formula_181.withColumn("run_date", current_date()).createOrReplaceTempView("formula_181")

# COMMAND ----------

display(
    spark.sql(
        f"""
    delete from 
        {reporting_catalog}.silver.goods_and_services_incorrect_classification 
    where 
        run_date = current_date
    """
    )
)

# COMMAND ----------

display(
    spark.sql(
    f"""
    insert into 
        {reporting_catalog}.silver.goods_and_services_incorrect_classification 
    select 
        * 
    from 
        formula_181
    """
    )
)

# COMMAND ----------

# DBTITLE 1,Convert Output to Pandas for Email
email_output = select_151

# COMMAND ----------

display(formula_120)

# COMMAND ----------

current_date = datetime.datetime.today().strftime("%B %d, %Y")
subject = (
    f"Auto-generated: Goods & Services Filed under Incorrect Classes ({current_date})"
)

attachment_name = f"Goods_Services_Incorrect_Classes_Report_{current_date}.xlsx"

top_front = "<"
top_middle = "style"
top_back = ">"
bottom_front = "</"
bottom_middle = "style"
bottom_back = ">"

style_front = top_front + top_middle + top_back
style_end = bottom_front + bottom_middle + bottom_back
styling = f"{style_front}{EMAIL_CSS}{style_end}"

body = f"""
{styling}

Greetings!

Here is a quick summary on goods and services that were filed under incorrect classes as of {current_date} in status 630 and 638. Attached is the full list of serial numbers with details.

<h3>Goods and Services: Incorrect Class (Status 630 and 638)</h3>

{summarize_137.toPandas().to_html(index=False)}
<div class='header-container'>
<h4 class='header'>Breakdown By Characteristics</h4>
<h4 class='header'>Top (5) List</h4>
</div>
<div class='table-container'>
{select_83.toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)} {select_196.limit(5).toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)}
{select_88.toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)} {select_78.limit(5).toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)}
{select_98.toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)} {select_109.limit(5).toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)}
{select_103.toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)} {select_72.limit(5).toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)}
{select_93.toPandas().sort_values(by="Percent Total", axis=0, ascending=False).to_html(index=False)}
</div>
"""

from_addr = 'Trademark_Analytics@uspto.gov'
attachments = [(email_output, attachment_name, 'excel')]

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = subject,
    send_from = from_addr,
    send_to = primary_email,
    send_to_cc = cc_email,
    html_body= body,
    attachments = attachments
)

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with {email_output.count()} records.")
