# Databricks notebook source
# Install required packages
%pip install --upgrade pip tableauhyperapi tableauserverclient hyperleaup certifi --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import ssl
import urllib3
import warnings

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# Override default SSL context
ssl._create_default_https_context = ssl._create_unverified_context

# patch tableauserverclient to disable SSL verification
import tableauserverclient as TSC
_original_server_init = TSC.Server.__init__

def _patched_server_init(self, *args, **kwargs):
    _original_server_init(self, *args, **kwargs)
    self.add_http_options({'verify': False})

TSC.Server.__init__ = _patched_server_init

# COMMAND ----------

import os

# Save original directory
NOTEBOOK_DIR = os.getcwd()
print(f"Notebook directory: {NOTEBOOK_DIR}")

# Set writable temp directories for Hyper process
os.environ['TMPDIR'] = '/tmp'
os.environ['TEMP'] = '/tmp'
os.environ['TMP'] = '/tmp'

# Create directories for Hyper logs and run files
os.makedirs('/tmp/hyper_logs', exist_ok=True)
os.makedirs('/tmp/hyper_run', exist_ok=True)

print(f"Current working directory: {os.getcwd()}")
print("Hyper temp directories configured successfully")

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file = f"../../config/{dbx_env}/trmreports-conf.yaml"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# MAGIC %run ../shared/ntb_tm_hyperfile_list

# COMMAND ----------

common_configs = read_yaml(config_file)
trgt_catalog = common_configs['schema']['trgt_catalog']
src_catalog = common_configs['schema']['tmngpdb_src_catalog']

spark.conf.set('conf.catalog', trgt_catalog)
spark.conf.set('conf.src_catalog', src_catalog)
spark.conf.set('conf.dbx_env', dbx_env)
print(f"{trgt_catalog=},{src_catalog=}")

if dbx_env == 'prod':
  tableau_scope = common_configs['secrets']['tableau_scope']

  spark.conf.set('config.tableau_scope', tableau_scope.lower()) 
  tableau_server = dbutils.secrets.get(scope=tableau_scope, key="host")
  username = dbutils.secrets.get(scope=tableau_scope, key="username")
  password = dbutils.secrets.get(scope=tableau_scope, key="password")
  project_name = 'Trademark'

  tableau_server = tableau_server.replace("https://", "").replace("http://", "").rstrip("/")
  tableau_server = f"https://{tableau_server}"

# COMMAND ----------

import pytz
from pytz import timezone

job_name = "ntb_gold_production_simulator"
start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f'{start_ts=}')
control_dt = begin_job_cntl(f'{trgt_catalog}.silver',job_name,start_ts)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_qtr = spark.table(f"{trgt_catalog}.silver.prod_simulator_qtr") 
df_fy = spark.table(f"{trgt_catalog}.silver.prod_simulator_fy")
df_qual = spark.table(f"{trgt_catalog}.silver.prod_simulator_qual")
df_bd = spark.table(f"{trgt_catalog}.silver.employee_bd")

union_df = df_qtr.unionByName(df_fy, allowMissingColumns=True) \
                 .unionByName(df_qual, allowMissingColumns=True) \
                 .unionByName(df_bd, allowMissingColumns=True)

#display(union_df)

# COMMAND ----------

union_df.createOrReplaceTempView("union_df")

# COMMAND ----------

from pyspark.sql.functions import sum, coalesce, lit

agg_df = union_df.groupBy("employee_no").agg(
    coalesce(sum("prac_pro_error_qt"), lit(0)).alias("sum_prac_pro_error_qt"),
    coalesce(sum("statutory_error_qt"), lit(0)).alias("sum_statutory_error_qt"),
    coalesce(sum("bds"), lit(0)).alias("sum_bds_fy"),
    coalesce(sum("exam_hrs"), lit(0)).alias("exam_hrs_fy")
)

#display(agg_df)

# COMMAND ----------

from pyspark.sql.functions import when, round, col

agg_df = agg_df.withColumn(
    "prac_err_rt",
    when(col("sum_bds_fy") > 0, round((col("sum_prac_pro_error_qt") / col("sum_bds_fy")) * 1000, 2)).otherwise(None)
).withColumn(
    "prac_err_score",
    when(col("sum_bds_fy") == 0, '---')
    .when(col("sum_prac_pro_error_qt") >= 60, 1)
    .when(col("sum_prac_pro_error_qt") >= 45, 2)
    .when(col("prac_err_rt") >= 30, 1)
    .when(col("prac_err_rt") >= 20, 2)
    .when(col("prac_err_rt") >= 12.5, 3)
    .when(col("prac_err_rt") >= 7.5, 4)
    .otherwise(5)
).withColumn(
    "stat_err_rt",
    when(col("sum_bds_fy") > 0, round((col("sum_statutory_error_qt") / col("sum_bds_fy")) * 1000, 2)).otherwise(None)
).withColumn(
    "stat_err_score",
    when(col("sum_bds_fy") == 0, '---')
    .when(col("sum_statutory_error_qt") >= 8, 1)
    .when(col("sum_statutory_error_qt") >= 6, 2)
    .when(col("stat_err_rt") >= 3.5, 1)
    .when(col("stat_err_rt") >= 2.5, 2)
    .otherwise('Not Rated')
)

#display(agg_df)

# COMMAND ----------

joined_df = agg_df.join(union_df, on="employee_no", how="inner").drop("employee_no")
joined_df.count()

# COMMAND ----------

from pyspark.sql.functions import when, col, lit, round, current_timestamp, month, to_date

# Convert relevant columns to numeric for scoring logic
def to_number(col_name):
    return when(col(col_name).isin('---', 'Not Rated'), lit(None)).otherwise(col(col_name).cast("double"))

joined_df = joined_df.withColumn(
    "write_def_score",
    when(
        (col("stat_err_score") == '---') | 
        (col("prac_err_score") == '---') | 
        (col("suff_score") == '---') | 
        (col("avg_write_score") == '---'),
        lit('---')
    ).when(
        (col("stat_err_score") == '1') | 
        (col("prac_err_score") == '1') | 
        (col("avg_write_score") == '1'),
        lit(1)
    ).when(
        (col("stat_err_score") == '2') | 
        (col("prac_err_score") == '2') | 
        (col("suff_score") == '1') | 
        (col("avg_write_score") == '2'),
        lit(2)
    ).when(
        col("suff_score") == '2',
        lit(3)
    ).when(
        col("sum_bds_fy") == 0,
        lit('---')
    ).when(
        col("CountNonNull_write_grade_qt") > 0,
        round(
            (
                to_number("prac_err_score") + 
                to_number("suff_score") + 
                to_number("avg_write_score")
            ) / 3, 2
        )
    ).otherwise(lit('---'))
)

joined_df = joined_df.withColumn(
    "quality_score",
    when(col("write_def_score") == '---', lit('---'))
    .when(to_number("write_def_score") >= 4.67, lit('Outstanding'))
    .when(to_number("write_def_score") >= 3.67, lit('Commendable'))
    .when(to_number("write_def_score") >= 3, lit('Marignal'))
    .when(
        (col("current_gs_grade_level_cd").isin('13', '14')) & (col("count_serial_num_tx") < 12),
        lit('Maginal')
    )
    .otherwise(lit('Unacceptable'))
)

joined_df = joined_df.withColumn("refreshed", current_timestamp())

joined_df = joined_df.withColumn(
    "max_pro_bds",
    when(to_number("fk_gs_level_cd") == 12, lit(1350))
    .when(to_number("fk_gs_level_cd") == 13, lit(1450))
    .when(to_number("fk_gs_level_cd") == 14, lit(1550))
    .when(to_number("fk_gs_level_cd").isin(9, 11), lit(1250))
    .otherwise(lit(None))
)

joined_df = joined_df.withColumn(
    "min_pro_bds",
    when(to_number("fk_gs_level_cd") == 12, lit(675))
    .when(to_number("fk_gs_level_cd") == 13, lit(725))
    .when(to_number("fk_gs_level_cd") == 14, lit(775))
    .when(to_number("fk_gs_level_cd").isin(9, 11), lit(625))
    .otherwise(lit(None))
)

joined_df = joined_df.withColumn(
    "cur_qtr",
    when(month(col("refreshed")) >= 10, lit(1))
    .when(month(col("refreshed")) <= 3, lit(2))
    .when(month(col("refreshed")) <= 6, lit(3))
    .otherwise(lit(4))
)

#display(joined_df)

# COMMAND ----------

from pyspark.sql.functions import col

selected_columns = [
    "employee_nm",
    "current_organization_cd",
    "quarter_bi_week_start_dt",
    "quarter_bi_week_end_dt",
    "quarter_no",
    "q1_wks",
    "q2_wks",
    "q3_wks",
    "q4_wks",
    "brs_user_id",
    "exam_hrs",
    "adj_hrs",
    "non_exam_hrs",
    "ot_hrs",
    "bds",
    "action_per_examining_hour_qt",
    "goal_status_ct",
    "docket_management_qt",
    "document_management_tx",
    "bi_week_below_goal_qt",
    "action_qt",
    "Table",
    "serial_num_tx",
    "statutory_error_qt",
    "prac_pro_error_qt",
    "search_ct",
    "write_grade_txt",
    "fk_gs_level_cd",
    "base_c_bds",
    "base_fs_bds",
    "base_m_bds",
    "base_o_bds",
    "transfer_balanced_disposal_qt",
    "bds_from_last_qtr",
    "workflow_qtr_goal",
    "schedule_hour_qt",
    "performance_rating_cd",
    "next_qtr_perf_rate_cd",
    "suff_rt",
    "suff_score",
    "avg_write_rt",
    "avg_write_score",
    "write_def%",
    "fk_start_gs_grade_level_cd",
    "promotion_dt",
    "org_effectiveness_rt",
    "org_mentor_qual_rt",
    "org_mentor_rt",
    "org_mentor_timely",
    "org_train_rt",
    "weighted_average_in",
    "weight_0_fully_successful_in",
    "org_mentor_score",
    "org_trn_score",
    "org_eff_score",
    col("prod_alloc_wgt_int").alias("prod_alloc_wgt"),
    col("qual_alloc_wgt_int").alias("qual_alloc_wgt"),
    col("wf_alloc_wgt_int").alias("wf_alloc_wgt"),
    col("org_alloc_wgt_int").alias("org_alloc_wgt"),
    "org_effectiveness_pt",
    "org_train_pt",
    "org_mentor_pt",
    "avg_score_rt",
    "examiner_amendment_usage_pt",
    "workflow_performance_rating_cd",
    "no_sig_trainee_biweeks",
    "partial_sig_trainee_biweeks",
    "pfs_trainee_biweeks",
    "exam_hrs_fy",
    "refreshed",
    "max_pro_bds",
    "min_pro_bds",
    "cur_qtr"
]

selected_df = joined_df.select(*selected_columns)
#display(selected_df)

# COMMAND ----------

from pyspark.sql.types import DecimalType, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import col

selected_df = selected_df \
    .withColumn("employee_nm", col("employee_nm").cast(StringType())) \
    .withColumn("current_organization_cd", col("current_organization_cd").cast(StringType())) \
    .withColumn("quarter_bi_week_start_dt", col("quarter_bi_week_start_dt").cast(DateType())) \
    .withColumn("quarter_bi_week_end_dt", col("quarter_bi_week_end_dt").cast(DateType())) \
    .withColumn("quarter_no", col("quarter_no").cast(IntegerType())) \
    .withColumn("q1_wks", col("q1_wks").cast(IntegerType())) \
    .withColumn("q2_wks", col("q2_wks").cast(IntegerType())) \
    .withColumn("q3_wks", col("q3_wks").cast(IntegerType())) \
    .withColumn("q4_wks", col("q4_wks").cast(IntegerType())) \
    .withColumn("brs_user_id", col("brs_user_id").cast(StringType())) \
    .withColumn("exam_hrs", col("exam_hrs").cast(DecimalType(14,2))) \
    .withColumn("adj_hrs", col("adj_hrs").cast(DecimalType(14,2))) \
    .withColumn("non_exam_hrs", col("non_exam_hrs").cast(DecimalType(14,2))) \
    .withColumn("ot_hrs", col("ot_hrs").cast(DecimalType(12,2))) \
    .withColumn("bds", col("bds").cast(IntegerType())) \
    .withColumn("action_per_examining_hour_qt", col("action_per_examining_hour_qt").cast(DecimalType(12,2))) \
    .withColumn("goal_status_ct", col("goal_status_ct").cast(StringType())) \
    .withColumn("docket_management_qt", col("docket_management_qt").cast(IntegerType())) \
    .withColumn("document_management_tx", col("document_management_tx").cast(StringType())) \
    .withColumn("bi_week_below_goal_qt", col("bi_week_below_goal_qt").cast(DecimalType(7,2))) \
    .withColumn("action_qt", col("action_qt").cast(IntegerType())) \
    .withColumn("Table", col("Table").cast(StringType())) \
    .withColumn("serial_num_tx", col("serial_num_tx").cast(StringType())) \
    .withColumn("statutory_error_qt", col("statutory_error_qt").cast(DecimalType(7,2))) \
    .withColumn("prac_pro_error_qt", col("prac_pro_error_qt").cast(IntegerType())) \
    .withColumn("search_ct", col("search_ct").cast(StringType())) \
    .withColumn("write_grade_txt", col("write_grade_txt").cast(StringType())) \
    .withColumn("fk_gs_level_cd", col("fk_gs_level_cd").cast(StringType())) \
    .withColumn("base_c_bds", col("base_c_bds").cast(IntegerType())) \
    .withColumn("base_fs_bds", col("base_fs_bds").cast(IntegerType())) \
    .withColumn("base_m_bds", col("base_m_bds").cast(IntegerType())) \
    .withColumn("base_o_bds", col("base_o_bds").cast(IntegerType())) \
    .withColumn("transfer_balanced_disposal_qt", col("transfer_balanced_disposal_qt").cast(IntegerType())) \
    .withColumn("bds_from_last_qtr", col("bds_from_last_qtr").cast(IntegerType())) \
    .withColumn("workflow_qtr_goal", col("workflow_qtr_goal").cast(DecimalType(7,2))) \
    .withColumn("schedule_hour_qt", col("schedule_hour_qt").cast(IntegerType())) \
    .withColumn("performance_rating_cd", col("performance_rating_cd").cast(StringType())) \
    .withColumn("next_qtr_perf_rate_cd", col("next_qtr_perf_rate_cd").cast(StringType())) \
    .withColumn("suff_rt", col("suff_rt").cast(DecimalType(18,4))) \
    .withColumn("suff_score", col("suff_score").cast(StringType())) \
    .withColumn("avg_write_rt", col("avg_write_rt").cast(StringType())) \
    .withColumn("avg_write_score", col("avg_write_score").cast(StringType())) \
    .withColumn("write_def%", col("write_def%").cast(StringType())) \
    .withColumn("fk_start_gs_grade_level_cd", col("fk_start_gs_grade_level_cd").cast(StringType())) \
    .withColumn("promotion_dt", col("promotion_dt").cast(DateType())) \
    .withColumn("org_effectiveness_rt", col("org_effectiveness_rt").cast(StringType())) \
    .withColumn("org_mentor_qual_rt", col("org_mentor_qual_rt").cast(StringType())) \
    .withColumn("org_mentor_rt", col("org_mentor_rt").cast(StringType())) \
    .withColumn("org_mentor_timely", col("org_mentor_timely").cast(StringType())) \
    .withColumn("org_train_rt", col("org_train_rt").cast(StringType())) \
    .withColumn("weighted_average_in", col("weighted_average_in").cast(IntegerType())) \
    .withColumn("weight_0_fully_successful_in", col("weight_0_fully_successful_in").cast(IntegerType())) \
    .withColumn("org_mentor_score", col("org_mentor_score").cast(StringType())) \
    .withColumn("org_trn_score", col("org_trn_score").cast(StringType())) \
    .withColumn("org_eff_score", col("org_eff_score").cast(StringType())) \
    .withColumn("prod_alloc_wgt", col("prod_alloc_wgt").cast(IntegerType())) \
    .withColumn("qual_alloc_wgt", col("qual_alloc_wgt").cast(IntegerType())) \
    .withColumn("wf_alloc_wgt", col("wf_alloc_wgt").cast(IntegerType())) \
    .withColumn("org_alloc_wgt", col("org_alloc_wgt").cast(IntegerType())) \
    .withColumn("org_effectiveness_pt", col("org_effectiveness_pt").cast(IntegerType())) \
    .withColumn("org_train_pt", col("org_train_pt").cast(IntegerType())) \
    .withColumn("org_mentor_pt", col("org_mentor_pt").cast(IntegerType())) \
    .withColumn("avg_score_rt", col("avg_score_rt").cast(DecimalType(7,2))) \
    .withColumn("examiner_amendment_usage_pt", col("examiner_amendment_usage_pt").cast(DecimalType(10,2))) \
    .withColumn("workflow_performance_rating_cd", col("workflow_performance_rating_cd").cast(StringType())) \
    .withColumn("no_sig_trainee_biweeks", col("no_sig_trainee_biweeks").cast(IntegerType())) \
    .withColumn("partial_sig_trainee_biweeks", col("partial_sig_trainee_biweeks").cast(IntegerType())) \
    .withColumn("pfs_trainee_biweeks", col("pfs_trainee_biweeks").cast(IntegerType())) \
    .withColumn("exam_hrs_fy", col("exam_hrs_fy").cast(DecimalType(14,2))) \
    .withColumn("refreshed", col("refreshed").cast(TimestampType())) \
    .withColumn("max_pro_bds", col("max_pro_bds").cast(IntegerType())) \
    .withColumn("min_pro_bds", col("min_pro_bds").cast(IntegerType())) \
    .withColumn("cur_qtr", col("cur_qtr").cast(IntegerType()))

# COMMAND ----------

selected_df.write.mode("overwrite").saveAsTable(f"{trgt_catalog}.gold.prod_simulator")

# COMMAND ----------

from pyspark.sql.types import DoubleType
from hyperleaup import HyperFile, HyperFileConfig

# Cast ByteType columns to DoubleType for HyperFile compatibility
df_prod_simulator = spark.table("trm_reporting.gold.prod_simulator")
for field in df_prod_simulator.schema.fields:
    if field.dataType.typeName() == "byte":
        df_prod_simulator = df_prod_simulator.withColumn(field.name, df_prod_simulator[field.name].cast(DoubleType()))

hyperfile_columns = df_prod_simulator.columns
df_hyperfile = df_prod_simulator.select(*hyperfile_columns)

os.chdir('/tmp')

hf_config = HyperFileConfig(timestamp_with_timezone=True,
                            allow_nulls=True,
                            convert_decimal_precision=True)
hf_name = "TM_EAPS"
hf = HyperFile(name=hf_name, df=df_hyperfile, is_dbfs_enabled=True, config=hf_config)

# Set EPS location for publishing
eps_location = "EPS"
datasource_name = hf_name  # TM_EAPS as datasource name

luid = hf.publish(tableau_server_url=tableau_server,
                  username=username,
                  password=password,
                  project_name=eps_location,
                  datasource_name=datasource_name
                  )
print(f'Published Hyper File to EPS/{datasource_name} as new datasource luid: {luid}')

os.chdir(NOTEBOOK_DIR)

# COMMAND ----------

end_job_cntl(f"{trgt_catalog}.silver", job_name, job_start_ts,'completed',0,"job completed successfully")
dbutils.notebook.exit(f"Completed loading Production Simulator Gold Table and pushed Hyperfile")