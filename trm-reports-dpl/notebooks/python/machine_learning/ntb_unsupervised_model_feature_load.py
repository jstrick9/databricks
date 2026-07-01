# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata
# MAGIC - **Created by:** Tyson Brown 
# MAGIC - **Created on:** 2025-08-15
# MAGIC - **Last updated by:** Ben Fielstra
# MAGIC - **Last updated on:** 2026-01-13
# MAGIC
# MAGIC ## Changelog
# MAGIC - **2026-01-13 (Ben Fielstra):** Added additional features, updated formatting, removed unnecessary comments

# COMMAND ----------

# DBTITLE 1,Import Libraries
import logging

import yaml

import itertools
import logging
import math

import numpy as np
import pandas as pd

from Levenshtein import ratio
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from rapidfuzz import fuzz
import sqlparse

# COMMAND ----------

# DBTITLE 1,Schemas
name_similarity_schema = StructType(
    [
        StructField("avg_name_similarity", DoubleType()),
        StructField("max_name_distance", DoubleType()),
        StructField("cluster_name_count", IntegerType()),
    ]
)

cluster_schema = MapType(StringType(), StringType())

# COMMAND ----------

# DBTITLE 1,Logger
logger = logging.getLogger("etl_auditor")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | [%(name)s]: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info("Initialized logger.")

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Workaround For Overridden Names
def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

# COMMAND ----------

# DBTITLE 1,Read Config
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
jbteasps_catalog = common_configs["schema"]["trm_jbteasps_src_catalog"]
tm_practitioner_catalog = common_configs["schema"]["tm_practitioner_catalog"]
print(reporting_catalog, tmngpdb_catalog, jbteasps_catalog, tm_practitioner_catalog)

# COMMAND ----------

# DBTITLE 1,Common Functions
def create_merge_query(target: str, source: str = "staging"):
    return f"""
    merge with schema evolution into
        {target} target
    using
        {source} source
    on
        {MERGE_KEY}
    when matched then update set
        {MERGE_UPDATE_SET_COLUMNS_QRY_STR}
    when not matched then insert (
        {MERGE_OUTPUT_COLUMNS_QRY_STR}
    )
    values (
        {MERGE_INSERT_COLUMNS_QRY_STR}
    )
    when not matched by source and target.load_date = current_date then delete
    when not matched by source and
        target.latest = true
        and target.load_date < current_date
    then update set target.latest = false
    """

def name_similarity_stats(signers: list):
    """
    Compute name similarity statistics for a cluster of signer names

    Parameters:
    ----------
    signers: list of str
        A list of cleaned signer names belonging to the same fuzzy-matched cluster for a given patron id

    Returns:
    -------
    tuple (avg_similarity, max_distance, cluster_size):
        - avg_similarity (float): The average normalized similarity score (0 to 1) across all unique signer name pairs in the cluster for a given patron_id

        - max_distance (float): 1-minimum pairwise similarity observed, representing the worst-case deviation in the cluster

        - cluster_size (int): The number of unique signer names in the cluster

    Notes:
    -----
        - Uses RapidFuzz's ratio() function (normalized Levenshtein similarity, scaled 0-100)
        - Returns perfect similarity (1.0) and zero max distance for clusters with <= 1 name
        - A higher max_distance implies greater variation within the cluster
    """

    if len(signers) <= 1:
        return (1.0, 0.0, len(signers))  # perfect consistency

    scores = []
    for i in range(len(signers)):
        for j in range(i + 1, len(signers)):
            s = fuzz.ratio(signers[i], signers[j]) / 100.0  # normalize
            scores.append(s)

    return (float(sum(scores)) / len(scores), 1.0 - min(scores), len(signers))


# Helper to group similar names by clustering
def cluster_names_py(name_list: list, threshold=80):
    """
    Very small single-link clustering:
    - Put the 1st name in cluster 0
    - Each subsequent name goes into the first cluster whose representative is >= threshold RapidFuzz ratio
    - Otherwise start a new cluster
    Returns {name -> cluster}
    """
    clusters = []
    for name in name_list:
        found = False
        for cluster in clusters:
            if fuzz.ratio(name, cluster[0]) >= threshold:
                cluster.append(name)
                found = True
                break

        if not found:
            clusters.append([name])

    # Return dict: name -> cluster_id
    mapping = {}
    for i, cluster in enumerate(clusters):
        for name in cluster:
            mapping[name] = f"cluster_{i}"

    return mapping


def compute_name_similarity_fuzzy_jaccard(audit_df, threshold=0.75):
    """
    Used to compute name similarity scores for a df column of names
    Input: a dataframe with a column with patron first, middle and last name
    threshold: the value at which a token match should occur
    Returns a decimel similarity score
    """
    # Build full name string
    audit_df["full_name"] = (
        (
            audit_df["dn_patron_first_nm"].fillna("")
            + " "
            + audit_df["dn_patron_middle_nm"].fillna("")
            + " "
            + audit_df["dn_patron_last_nm"].fillna("")
        )
        .str.strip()
        .str.lower()
    )

    # Filter out rows where full name is empty
    audit_df = audit_df[audit_df["full_name"] != ""]

    # Group all names used per patron_id
    grouped = audit_df.groupby("cfk_patron_id")["full_name"].apply(list).reset_index()

    # Define function to compute mean pairwise similarity

    def fuzzy_jaccard(names):
        if len(names) <= 1:
            return 1.0  # if only 1 name ever used/perfect score

        unique_names = list(set(names))  # Avoid duplicate exact matches

        if len(unique_names) == 1:
            return 1

        # Collect all unique tokens used across all names

        token_lists = []
        for name in unique_names:
            tokens = set(name.replace("-", " ").split())
            token_lists.append(tokens)

        # Now compare all pairs of names
        pairs = list(itertools.combinations(token_lists, 2))
        scores = []
        for tokens1, tokens2 in pairs:
            matched_tokens = 0
            for token1 in tokens1:
                for token2 in tokens2:
                    if ratio(token1, token2) >= threshold:
                        matched_tokens += 1
                        break  # only count best match once

            # union of tokens for this pair
            union_tokens = len(tokens1.union(tokens2))
            pair_score = matched_tokens / union_tokens if union_tokens > 0 else 0.0
            scores.append(pair_score)

        return sum(scores) / len(scores) if scores else 1.0

    # Apply function per applicant
    grouped["name_similarity_score"] = grouped["full_name"].apply(fuzzy_jaccard)

    # Output dataframe: patron_id + name_similarity_score
    result_df = grouped[["cfk_patron_id", "name_similarity_score"]]

    return result_df

# COMMAND ----------

# DBTITLE 1,Generate UDFs
name_stats_udf = F.udf(name_similarity_stats, name_similarity_schema)
cluster_udf = F.udf(cluster_names_py, cluster_schema)

# COMMAND ----------

# DBTITLE 1,Globals
# Note: Normally we would just use the delta history but since we run vacuum every Monday, we want
# a historical log for auditing. This may be tracked in MLFlow but for now, we'll maintain this
# ourselves.
FEATURE_STORE_LOAD_HISTORY: str = "unsupervised_anomalies_feature_load_history"
FEATURE_STORE_NON_EXCLUSIVE: str = "unsupervised_anomalies_features_non_exclusive"
FEATURE_STORE_EXCLUSIVE: str = "unsupervised_anomalies_features_exclusive"

FEATURE_STORE_LOAD_HISTORY_FQN: str = (
    f"{reporting_catalog}.silver.{FEATURE_STORE_LOAD_HISTORY}"
)
FEATURE_STORE_NON_EXCLUSIVE_FQN: str = (
    f"{reporting_catalog}.gold.{FEATURE_STORE_NON_EXCLUSIVE}"
)
FEATURE_STORE_EXCLUSIVE_FQN: str = (
    f"{reporting_catalog}.gold.{FEATURE_STORE_EXCLUSIVE}"
)

MERGE_KEY: str = (
    "target.cfk_patron_id = source.cfk_patron_id and target.load_date = source.load_date"
)

# Note: This is used to determine qualifying records for the FEATURE_STORE_EXCLUSIVE load.
CUMULATIVE_UNSUPERVISED_TABLE: str = "unsupervised_anomalies_cumulative"
CUMULATIVE_UNSUPERVISED_TABLE_FQN: str = (
    f"{reporting_catalog}.gold.{CUMULATIVE_UNSUPERVISED_TABLE}"
)

SMALL_BUCKET: int = 25
MEDIUM_BUCKET: int = 100
LARGE_BUCKET: int = 300

BURST_THRESHOLD_IN_MINUTES: int = 2
BAD_SIGNATURE_CATEGORIES: str = "('No Info-No Info', 'N/A-N/A')"

NON_FEATURE_COLUMNS: set = (
    "cfk_patron_id",
    "applicant_bin",
    "selected_role",
    "is_owner_account",
    "is_attorney_account",
    "is_attorney_support_account",
)

NULL_COLUMN_MAP_VALUES: dict = {
    "z_day_0": 0,
    "z_day_30": 0,
    "z_day_90": 0,
    "z_day_180": 0,
    "z_day_360": 0,
    "avg_sig_type_entropy": 0,
    "avg_sig_type_change_rate": 0,
    "max_sig_type_entropy": 0,
    "max_sig_type_change_rate": 0,
    "avg_name_similarity": 1,
    "avg_class_count": 0,
    "max_name_distance": 0,
    "avg_cluster_size": 1,
    "baseline_intl_ratio": 0,
    "recent_50_intl_ratio": 0,
    "recent_10_intl_ratio": 0,
    "intl_spike_zscore_50": 0,
    "intl_spike_zscore_10": 0,
    "intl_streak_len": 0,
    "num_has_sponsored": 0,
    "num_has_been_sponsored_by": 0,
    "is_ten_minute_rapid_filer": 0,
    "is_one_minute_rapid_filer": 0,
    "num_times_owner_signed_as_attorney": 0,
    "max_num_distinct_different_hand_and_e_sign_as_owner_same_ip": 0,
    "max_num_distinct_different_names": 0,
    "max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip": 0,
    "max_num_distinct_signatory_names_with_direct_signature_from_same_ip": 0,
    "num_distinct_signatory_names_with_direct_signature": 0,
    "has_submissions_every_fifteen_for_six_hours_or_more": 0,
    "has_submissions_without_six_hour_break_for_one_day": 0,
}

logger.info(
    f"Burst submissions occur within [{BURST_THRESHOLD_IN_MINUTES}] minutes of one another."
)

logger.info(f"Excluded signature types are: {BAD_SIGNATURE_CATEGORIES}")

for column, default in NULL_COLUMN_MAP_VALUES.items():
    logger.info(f"[{column}] will default to [{default}] in the feature store.")

# COMMAND ----------

# DBTITLE 1,Merge Output Details
# TODO: change to support column selection via widgets and cooperate with NULL_COLUMN_MAP_VALUES
OUTPUT_COLUMNS: list = [
    "load_date",
    "latest",
    "cfk_patron_id",
    "applicant_bin",
    "selected_role",
    "num_has_sponsored",
    "num_has_been_sponsored_by",
    "is_ten_minute_rapid_filer",
    "is_one_minute_rapid_filer",
    "num_times_owner_signed_as_attorney",
    "max_num_distinct_different_hand_and_e_sign_as_owner_same_ip",
    "max_num_distinct_different_names",
    "max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip",
    "max_num_distinct_signatory_names_with_direct_signature_from_same_ip",
    "num_distinct_signatory_names_with_direct_signature",
    "has_submissions_every_fifteen_for_six_hours_or_more",
    "has_submissions_without_six_hour_break_for_one_day",
    "submission_burst_rate",
    "d_sig_burst_rate",
    "submissions_per_day",
    "z_day_0",
    "z_day_30",
    "z_day_90",
    "z_day_180",
    "z_day_360",
    "log_cumulative_day_0",
    "log_cumulative_day_30",
    "log_cumulative_day_90",
    "log_cumulative_day_180",
    "log_cumulative_day_360",
    "name_similarity_score",
    "avg_class_count",
    "hourly_entropy",
    "weekday_entropy",
    "normalized_ip_entropy",
    "ip_switch_burst_rate",
    "avg_sig_type_entropy",
    "avg_sig_type_change_rate",
    "max_sig_type_entropy",
    "max_sig_type_change_rate",
    "avg_name_similarity",
    "max_name_distance",
    "avg_cluster_size",
    "baseline_intl_ratio",
    "recent_50_intl_ratio",
    "recent_10_intl_ratio",
    "intl_spike_zscore_50",
    "intl_spike_zscore_10",
    "intl_streak_len",
    "completeness",
    "create_ts",
    "create_user",
]

MERGE_OUTPUT_COLUMNS_QRY_STR: str = ", ".join(OUTPUT_COLUMNS)

IGNORED_MERGE_COLUMNS = set(["create_ts", "create_user"])

MERGE_UPDATE_SET_COLUMNS = [
    f"target.{column} = source.{column}"
    for column in OUTPUT_COLUMNS
    if column not in IGNORED_MERGE_COLUMNS
] + [
    "target.create_ts = current_timestamp",
    "target.create_user = 'UNSUPERVISED_MODEL_FEATURE_LOAD_ETL'",
]

MERGE_UPDATE_SET_COLUMNS_QRY_STR: str = ", ".join(MERGE_UPDATE_SET_COLUMNS)

MERGE_INSERT_COLUMNS = [
    f"source.{column}"
    for column in OUTPUT_COLUMNS
    if column not in IGNORED_MERGE_COLUMNS
] + [
    "current_timestamp",
    "'UNSUPERVISED_MODEL_FEATURE_LOAD_ETL'",
]

MERGE_INSERT_COLUMNS_QRY_STR: str = ", ".join(MERGE_INSERT_COLUMNS)
EXCLUSIVE_MERGE_QRY_STAGE = f"""
    (
        select
            *
        from
            {FEATURE_STORE_LOAD_HISTORY_FQN} a
        where
            latest = true
            and not exists (
                select
                    1
                from
                    {CUMULATIVE_UNSUPERVISED_TABLE_FQN} b
                where
                    b.is_anomaly = true
                    and a.cfk_patron_id = b.cfk_patron_id
            )
    )
"""

TODAYS_MERGE_QRY: str = create_merge_query(target=FEATURE_STORE_LOAD_HISTORY_FQN)
NON_EXCLUSIVE_MERGE_QRY: str = create_merge_query(
    target=FEATURE_STORE_NON_EXCLUSIVE_FQN
)
EXCLUSIVE_MERGE_QRY: str = create_merge_query(
    target=FEATURE_STORE_EXCLUSIVE_FQN, source=EXCLUSIVE_MERGE_QRY_STAGE
)

logger.info("The following statements will be used for each MERGE:")
for statement in [TODAYS_MERGE_QRY, NON_EXCLUSIVE_MERGE_QRY, EXCLUSIVE_MERGE_QRY]:
    formatted_statement = sqlparse.format(
        statement, reindent=True, keyword_case="lower"
    )
    logger.info(formatted_statement)

# COMMAND ----------

# DBTITLE 1,Create Base Views
spark.sql(
    f"""
    select
        pi.patron_id,
        min(src_create_ts) src_create_ts
    from
        {tm_practitioner_catalog}.bronze.dim_patron pi
    where
        pi.acct_type_cd = 'X'
    group by
        pi.patron_id
"""
).createOrReplaceTempView("all_patrons")

spark.sql(
    f"""
select
  f.cfk_patron_id,
  nvl(f.selected_role_nm, 'DataUnavailable') selected_role,
  iff(f.selected_role_nm = 'TrademarkOwner', 1, 0) is_owner_account,
  iff(f.selected_role_nm = 'TrademarkAttorney', 1, 0) is_attorney_account,
  iff(f.selected_role_nm = 'TrademarkAttorneySupport', 1, 0) is_attorney_support_account
from
  {jbteasps_catalog}.bronze.interested_party f
"""
).createOrReplaceTempView("roles")

spark.sql(
    f"""
    select 
        * 
    from 
        {jbteasps_catalog}.bronze.audit_log al
    where
        fk_transaction_type_cd = 'Submission'
        and cfk_patron_id like '%-%-%-%-%'
        and fk_form_cd in ('APPB', 'BAS', 'FTK')
"""
).createOrReplaceTempView("all_audit")

spark.sql(
    """
    select 
        * 
    from 
        all_audit
    where
        create_ts >= (current_date - interval 1 year)
"""
).createOrReplaceTempView("all_audit_past_year")

spark.sql(
    """
    select
      cfk_patron_id,
      create_ts submission_ts
    from
      all_audit
    """
).createOrReplaceTempView("all_audit_redux")

spark.sql(
    f"""
    select
        lower(patron_id) cfk_patron_id,
        b.src_create_ts latest_create_ts
    from
        all_audit_past_year a
        join all_patrons b
            on a.cfk_patron_id = lower(b.patron_id)
    group by
        patron_id,
        b.src_create_ts
    having
        count(distinct a.serial_no) >= 10
"""
).createOrReplaceTempView("base")

# COMMAND ----------

# DBTITLE 1,Create Eligible Patrons
logger.info("Creating view for historical run.")
eligible_patrons_qry: str = """
select 
    a.*,
    nvl(b.selected_role, 'DataUnavailable') selected_role
from 
    base a 
    left join `roles` b 
        on a.cfk_patron_id = b.cfk_patron_id
""" 
eligible_patrons = spark.sql(eligible_patrons_qry)
eligible_patrons.createOrReplaceTempView("eligible_patrons")
eligible_patrons.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Sponsored By
sponsored_by = spark.sql(
    f"""
  select distinct
    a.cfk_sponsoree_id cfk_patron_id,
    size(
      collect_set(a.cfk_sponsorer_id) over (partition by a.cfk_sponsoree_id)
    ) num_has_been_sponsored_by
  from
    {jbteasps_catalog}.bronze.sponsorship a
"""
)

feature_sponsored_by = (
    eligible_patrons.alias("a").join(
        other=sponsored_by.alias("y"), on="cfk_patron_id", how="left"
    )
).select("a.cfk_patron_id", "a.latest_create_ts", "a.selected_role", "num_has_been_sponsored_by")

# COMMAND ----------

# DBTITLE 1,Add Feature: Sponsored
sponsored = spark.sql(
    f"""
  select distinct
    a.cfk_sponsorer_id cfk_patron_id,
    size(collect_set(a.cfk_sponsoree_id) over (partition by a.cfk_sponsorer_id)) num_has_sponsored
  from
    {jbteasps_catalog}.bronze.sponsorship a
"""
)

feature_sponsored = feature_sponsored_by.alias("a").join(
    other=sponsored.alias("z"), on="cfk_patron_id", how="left"
)

# COMMAND ----------

# DBTITLE 1,Add Meta-Feature: 10 Minute Rapid Filers
spark.sql(
    """
select
  cfk_patron_id,
  extract(
      hour from ((session_window.end - interval 10 minutes) - session_window.start)
  ) 
  + 
  (
    extract(
      minute from ((session_window.end - interval 10 minutes) - session_window.start)
    ) / 60
  ) session_in_hours,
  session_window.start as submission_time,
  session_window.end as last_submission_time_of_session_plus_gap,
  count(*) num_submission_per_session
from
  all_audit_redux
group by
  cfk_patron_id,
  session_window(submission_ts, '10 minutes')
having
  date(submission_time) > current_date - interval 1 year
"""
).createOrReplaceTempView("ten_minute_submissions")
ten_minute_submissions_rows = [
    row
    for row in spark.sql("select * from ten_minute_submissions").collect()
    if row.session_in_hours >= 2
]
if len(ten_minute_submissions_rows) > 1:
    ten_minute_submissions = spark.createDataFrame(ten_minute_submissions_rows)
    ten_minute_submissions_out = ten_minute_submissions.select(
        "cfk_patron_id"
    ).distinct()
else:
    ten_minute_submissions_out = spark.sql("select null cfk_patron_id")

ten_minute_submissions_out.printSchema()
logger.info(f"Feature column length: {len(ten_minute_submissions_out.columns)}")

feature_ten_minute_submissions = (
    feature_sponsored.join(
        other=ten_minute_submissions_out.alias("b"), on="cfk_patron_id", how="left"
    ).selectExpr(
        "*", "iff(b.cfk_patron_id is not null, 1, 0) is_ten_minute_rapid_filer"
    )
)

feature_ten_minute_submissions.printSchema()
logger.info(f"Total column length: {len(feature_ten_minute_submissions.columns)}")

# COMMAND ----------

# DBTITLE 1,Add Meta-Feature: 1 Minute Rapid Filers
spark.sql(
    """
select
  cfk_patron_id,
  extract(
      hour from ((session_window.end - interval 1 minutes) - session_window.start)
  ) 
  + 
  (
    extract(
      minute from ((session_window.end - interval 1 minutes) - session_window.start)
    ) / 60
  ) session_in_hours,
  session_window.start as submission_time,
  session_window.end as last_submission_time_of_session_plus_gap,
  count(*) num_submission_per_session
from
  all_audit_redux
group by
  cfk_patron_id,
  session_window(submission_ts, '1 minutes')
having
  date(submission_time) > current_date - interval 1 year
"""
).createOrReplaceTempView("one_minute_submissions")
one_minute_submissions_rows = [
    row
    for row in spark.sql("select * from one_minute_submissions").collect()
    if row.num_submission_per_session > 1
]
if len(one_minute_submissions_rows) > 1:
    one_minute_submissions = spark.createDataFrame(one_minute_submissions_rows)
    one_minute_submissions_out = one_minute_submissions.select(
        "cfk_patron_id"
    ).distinct()
else:
    one_minute_submissions_out = spark.sql("select null cfk_patron_id")

one_minute_submissions_out.printSchema()
logger.info(f"Feature column length: {len(one_minute_submissions_out.columns)}")

feature_one_minute_submissions = feature_ten_minute_submissions.join(
    other=one_minute_submissions_out.alias("c"), on="cfk_patron_id", how="left"
).selectExpr("*", "iff(c.cfk_patron_id is not null, 1, 0) is_one_minute_rapid_filer")

feature_one_minute_submissions.printSchema()
logger.info(f"Total column length: {len(feature_one_minute_submissions.columns)}")

# COMMAND ----------

# DBTITLE 1,Add Feature: Number of Times Owner Account Had Signatory Position as Attorney
owners_who_signed_as_attorney = spark.sql(
    """
select
  a.cfk_patron_id,
  sum(
    case
      when contains(lower(signatory_position_nm), 'attorney') then 1
      else 0
    end
  ) num_times_owner_signed_as_attorney
from
  all_audit a
    join `roles` b
      on a.cfk_patron_id = b.cfk_patron_id
where
  b.selected_role = 'TrademarkOwner'
group by
  all
having
  num_times_owner_signed_as_attorney > 0
"""
)

owners_who_signed_as_attorney.printSchema()
logger.info(f"Feature column length: {len(owners_who_signed_as_attorney.columns)}")

feature_owners_who_signed_as_attorney = feature_one_minute_submissions.join(
    other=owners_who_signed_as_attorney.alias("d"), on="cfk_patron_id", how="left"
)

feature_owners_who_signed_as_attorney.printSchema()
logger.info(f"Total column length: {len(feature_owners_who_signed_as_attorney.columns)}")

# COMMAND ----------

# DBTITLE 1,Add Feature: Same Owner and IP With Multiple Signatures Across Multiple Types
owners_with_same_ip_who_handsigned_or_esigned_with_different_names = spark.sql(
    """
with base as (
  select distinct
    a.cfk_patron_id,
    size(
      collect_set(
        case
          when a.fk_signature_type_cd in ('D-HSIGN-OFF', 'D-HSIGN-ON', 'D-ESIGN-ON') then signatory_nm
        end
      ) over (partition by a.cfk_patron_id, a.ip_address_tx)
    ) num_distinct_different_hand_and_e_sign_as_owner_same_ip
  from
    all_audit a
      join `roles` b
        on a.cfk_patron_id = b.cfk_patron_id
  where
    b.selected_role = 'TrademarkOwner'
  qualify
    num_distinct_different_hand_and_e_sign_as_owner_same_ip > 1
)
select 
    cfk_patron_id,
    max(num_distinct_different_hand_and_e_sign_as_owner_same_ip) max_num_distinct_different_hand_and_e_sign_as_owner_same_ip 
from 
    base 
group by 
    cfk_patron_id
"""
)

owners_with_same_ip_who_handsigned_or_esigned_with_different_names.printSchema()
logger.info(
    f"Feature column length: {len(owners_with_same_ip_who_handsigned_or_esigned_with_different_names.columns)}"
)

feature_owner_ip_h_e_sig_names = feature_owners_who_signed_as_attorney.join(
    other=owners_with_same_ip_who_handsigned_or_esigned_with_different_names.alias("e"),
    on="cfk_patron_id",
    how="left",
)

feature_owner_ip_h_e_sig_names.printSchema()
logger.info(f"Total column length: {len(feature_owner_ip_h_e_sig_names.columns)}")

# COMMAND ----------

# DBTITLE 1,Add Feature: Same Owner With Different Signatory Names
owners_who_signed_using_multiple_names = spark.sql(
    """
with base as (
select distinct
  a.cfk_patron_id,
  size(collect_set(signatory_nm) over (partition by a.cfk_patron_id)) num_distinct_different_names
from
  all_audit a
    join `roles` b
      on a.cfk_patron_id = b.cfk_patron_id
where
  b.selected_role = 'TrademarkOwner'
qualify
  num_distinct_different_names > 1
  and b.selected_role = 'TrademarkOwner'
)
select 
    cfk_patron_id,
    max(num_distinct_different_names) max_num_distinct_different_names 
from 
    base 
group by 
    cfk_patron_id
"""
)

owners_who_signed_using_multiple_names.printSchema()
logger.info(
    f"Feature column length: {len(owners_who_signed_using_multiple_names.columns)}"
)

feature_owners_using_multiple_names = feature_owner_ip_h_e_sig_names.join(
    other=owners_who_signed_using_multiple_names.alias("f"),
    on="cfk_patron_id",
    how="left",
)

feature_owners_using_multiple_names.printSchema()
logger.info(
    f"Total column length: {len(feature_owners_using_multiple_names.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Same Attorney and IP With Multiple Signatures Across Multiple Types
attorneys_with_same_ip_who_handsigned_or_esigned_with_multiple_names = spark.sql(
    """
with base as (
select distinct
  a.cfk_patron_id,
  size(
    collect_set(
      case
        when a.fk_signature_type_cd in ('D-HSIGN-OFF', 'D-HSIGN-ON', 'D-ESIGN-ON') then signatory_nm
      end
    ) over (partition by a.cfk_patron_id, a.ip_address_tx)
  ) num_distinct_different_hand_and_e_sign_as_attorney_same_ip
from
  all_audit a
    join `roles` b
      on a.cfk_patron_id = b.cfk_patron_id
where
  b.selected_role in ('TrademarkAttorney', 'TrademarkAttorneySupport')
qualify
  num_distinct_different_hand_and_e_sign_as_attorney_same_ip > 1
)
select 
    cfk_patron_id,
    max(num_distinct_different_hand_and_e_sign_as_attorney_same_ip) max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip 
from 
    base 
group by 
    cfk_patron_id
"""
)

attorneys_with_same_ip_who_handsigned_or_esigned_with_multiple_names.printSchema()
logger.info(
    f"Feature column length: {len(attorneys_with_same_ip_who_handsigned_or_esigned_with_multiple_names.columns)}"
)

feature_attorneys_ip_h_e_sig_names = feature_owners_using_multiple_names.join(
    other=attorneys_with_same_ip_who_handsigned_or_esigned_with_multiple_names.alias(
        "g"
    ),
    on="cfk_patron_id",
    how="left",
)

feature_attorneys_ip_h_e_sig_names.printSchema()
logger.info(
    f"Total column length: {len(feature_attorneys_ip_h_e_sig_names.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Any Account and IP With Multiple Direct Signatures Across Multiple Types
accounts_with_same_ip_that_direct_signed_with_different_names = spark.sql(
    """
with base as (
select distinct
  a.cfk_patron_id,
  size(
    collect_set(
      case
        when a.fk_signature_type_cd in ('D-DIRECT', 'R-DIRECT') then signatory_nm
      end
    ) over (partition by a.cfk_patron_id, a.ip_address_tx)
  ) num_distinct_signatory_names_with_direct_signature_from_same_ip
from
  all_audit a
    join `roles` b
      on a.cfk_patron_id = b.cfk_patron_id
qualify
  num_distinct_signatory_names_with_direct_signature_from_same_ip > 1
)
select
    cfk_patron_id,
    max(num_distinct_signatory_names_with_direct_signature_from_same_ip) max_num_distinct_signatory_names_with_direct_signature_from_same_ip 
from 
    base 
group by 
    cfk_patron_id
"""
)

accounts_with_same_ip_that_direct_signed_with_different_names.printSchema()
logger.info(
    f"Feature column length: {len(accounts_with_same_ip_that_direct_signed_with_different_names.columns)}"
)

feature_accounts_ip_d_sig = feature_attorneys_ip_h_e_sig_names.join(
    other=accounts_with_same_ip_that_direct_signed_with_different_names.alias("h"),
    on="cfk_patron_id",
    how="left",
)

feature_accounts_ip_d_sig.printSchema()
logger.info(
    f"Total column length: {len(feature_accounts_ip_d_sig.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Any Account Multiple Direct Signatures Across Multiple Types
accounts_with_multiple_distinct_signatory_names_for_direct_signatures = spark.sql(
    """
select distinct
  a.cfk_patron_id,
  size(
    collect_set(
      case
        when a.fk_signature_type_cd in ('D-DIRECT', 'R-DIRECT') then signatory_nm
      end
    ) over (partition by a.cfk_patron_id)
  ) num_distinct_signatory_names_with_direct_signature
from
  all_audit a
    join `roles` b
      on a.cfk_patron_id = b.cfk_patron_id
qualify
  num_distinct_signatory_names_with_direct_signature > 1
"""
)

accounts_with_multiple_distinct_signatory_names_for_direct_signatures.printSchema()
logger.info(
    f"Feature column length: {len(accounts_with_multiple_distinct_signatory_names_for_direct_signatures.columns)}"
)

feature_accounts_sig_d = feature_accounts_ip_d_sig.join(
    other=accounts_with_multiple_distinct_signatory_names_for_direct_signatures.alias(
        "i"
    ),
    on="cfk_patron_id",
    how="left",
)

feature_accounts_sig_d.printSchema()
logger.info(
    f"Total column length: {len(feature_accounts_sig_d.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Submissions Every 15 Minutes For At Least 6 Hours
spark.sql(
    """
select
  cfk_patron_id,
  extract(
      hour from ((session_window.end - interval 15 minutes) - session_window.start)
  ) 
  + 
  (
    extract(
      minute from ((session_window.end - interval 15 minutes) - session_window.start)
    ) / 60
  ) session_in_hours,
  session_window.start as submission_time,
  session_window.end as last_submission_time_of_session_plus_gap,
  count(*) num_submission_per_session
from
  all_audit_redux
group by
  cfk_patron_id,
  session_window(submission_ts, '15 minutes')
having
  date(submission_time) > current_date - interval 1 year
"""
).createOrReplaceTempView(
    "accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours"
)
accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_rows = [
    row
    for row in spark.sql(
        "select * from accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours"
    ).collect()
    if row.session_in_hours >= 6
]

if (
    len(
        accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_rows
    )
    > 1
):
    accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours = spark.createDataFrame(
        accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_rows
    )
    accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_out = accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours.select(
        "cfk_patron_id"
    ).distinct()

else:
    accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_out = spark.sql(
        "select null cfk_patron_id"
    )

accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_out.printSchema()
logger.info(
    f"Feature column length: {len(accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_out.columns)}"
)

feature_accounts_having_one_submission_fifteen_six = feature_accounts_sig_d.join(
    other=accounts_flagged_with_at_least_one_submission_every_fifteen_minutes_for_six_hours_out.alias(
        "j"
    ),
    on="cfk_patron_id",
    how="left",
).selectExpr(
    "*",
    "iff(j.cfk_patron_id is not null, 1, 0) has_submissions_every_fifteen_for_six_hours_or_more",
)

feature_accounts_having_one_submission_fifteen_six.printSchema()
logger.info(
    f"Total column length: {len(feature_accounts_having_one_submission_fifteen_six.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Submissions Every 6 Hours For Sessions Lasting at Least 24 Hours
spark.sql(
    """
select
  cfk_patron_id,
  extract(
      hour from ((session_window.end - interval 6 hours) - session_window.start)
  ) 
  + 
  (
    extract(
      minute from ((session_window.end - interval 6 hours) - session_window.start)
    ) / 60
  ) session_in_hours,
  session_window.start as submission_time,
  session_window.end as last_submission_time_of_session_plus_gap,
  count(*) num_submission_per_session
from
  all_audit_redux
group by
  cfk_patron_id,
  session_window(submission_ts, '6 hours')
having
  date(submission_time) > current_date - interval 1 year
order by
  num_submission_per_session desc,
  cfk_patron_id
"""
).createOrReplaceTempView(
    "accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break"
)

accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_rows = [
    row
    for row in spark.sql(
        "select * from accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break"
    ).collect()
    if row.session_in_hours >= 24
]

if (
    len(
        accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_rows
    )
    > 1
):
    accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break = spark.createDataFrame(
        accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_rows
    )
    accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_out = accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break.select(
        "cfk_patron_id"
    ).distinct()

else:
    accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_out = (
        spark.sql("select null cfk_patron_id")
    )

accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_out.printSchema()
logger.info(
    f"Feature column length: {len(accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_out.columns)}"
)

feature_accounts_submissions_without_six_hour_break = feature_accounts_having_one_submission_fifteen_six.join(
    other=accounts_with_one_submission_every_twenty_four_hours_without_six_hour_break_out.alias(
        "k"
    ),
    on="cfk_patron_id",
    how="left",
).selectExpr(
    "*",
    "iff(k.cfk_patron_id is not null, 1, 0) has_submissions_without_six_hour_break_for_one_day",
)

feature_accounts_submissions_without_six_hour_break.printSchema()
logger.info(
    f"Total column length: {len(feature_accounts_submissions_without_six_hour_break.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Multiple IPs (Inclusive to Third Octet) In One-Hour Session
spark.sql(
    """
select
  cfk_patron_id,
  extract(hour from ((session_window.end - interval 60 minutes) - session_window.start))
  + (
    extract(minute from ((session_window.end - interval 60 minutes) - session_window.start)) / 60
  ) session_in_hours,
  session_window.start as submission_time,
  session_window.end as last_submission_time_of_session_plus_gap,
  count(distinct three_octet_ip) num_distinct_ips_per_session
from
  (
    select
      cfk_patron_id,
      regexp_extract(ip_address_tx, r'^(?:[^.]*\.){3}', 0) three_octet_ip,
      create_ts submission_ts
    from
      all_audit
  )
group by
  cfk_patron_id,
  session_window(submission_ts, '60 minutes')
having
  date(submission_time) > current_date - interval 1 year
"""
).createOrReplaceTempView("accounts_with_multiple_ips_in_one_hour_session")

accounts_with_multiple_ips_in_one_hour_session = [
    row
    for row in spark.sql(
        "select * from accounts_with_multiple_ips_in_one_hour_session"
    ).collect()
    if row.session_in_hours <= 1 and row.num_distinct_ips_per_session > 1
]

if len(accounts_with_multiple_ips_in_one_hour_session) > 1:
    accounts_with_multiple_ips_in_one_hour_session = spark.createDataFrame(
        accounts_with_multiple_ips_in_one_hour_session
    )

    accounts_with_multiple_ips_in_one_hour_session_out = (
        accounts_with_multiple_ips_in_one_hour_session.select(
            "cfk_patron_id"
        ).distinct()
    )

else:
    accounts_with_multiple_ips_in_one_hour_session_out = spark.sql(
        "select null cfk_patron_id"
    )

accounts_with_multiple_ips_in_one_hour_session_out.printSchema()
logger.info(f"Feature column length: {len(accounts_with_multiple_ips_in_one_hour_session_out.columns)}")

feature_accounts_ip_one_hour_session = feature_accounts_submissions_without_six_hour_break.join(
    other=accounts_with_multiple_ips_in_one_hour_session_out.alias("l"),
    on="cfk_patron_id",
    how="left",
).selectExpr(
    "*",
    "iff(l.cfk_patron_id is not null, 1, 0) has_submissions_with_multiple_ips_in_one_hour_session",
)

logger.info("Schema so far:")
feature_accounts_ip_one_hour_session.printSchema()
logger.info(f"Total column length: {len(feature_accounts_ip_one_hour_session.columns)}")

# COMMAND ----------

# DBTITLE 1,Add Meta-Feature: Total Submissions
logger.info("Generating totals for bucketing based on [all_audit].")

total_df = spark.sql(
    f"""
  select 
    al.cfk_patron_id,
    count(distinct al.serial_no) total_submissions
  from 
    all_audit al
  group by
    al.cfk_patron_id
"""
)

total_df.printSchema()
logger.info(
    f"Feature column length: {len(total_df.columns)}"
)

# COMMAND ----------

# DBTITLE 1,Add Feature: Bucketing
logger.info(f"""
    Generating buckets based on groups: 
    Small = {SMALL_BUCKET}
    Medium = {MEDIUM_BUCKET} 
    Large = {LARGE_BUCKET}
    Very Large > {LARGE_BUCKET}
""")

bucketed_df = total_df.withColumn(
    "applicant_bin",
    F.when(F.col("total_submissions") <= SMALL_BUCKET, "Small")
    .when(F.col("total_submissions") <= MEDIUM_BUCKET, "Medium")
    .when(F.col("total_submissions") <= LARGE_BUCKET, "Large")
    .otherwise("Very Large"),
)

buckets = bucketed_df.select("cfk_patron_id", "applicant_bin")

buckets.createOrReplaceTempView("buckets")

logger.info("Added feature schema:")
bucketed_df.printSchema()

df_main = feature_accounts_ip_one_hour_session.join(
    other=bucketed_df, on="cfk_patron_id", how="left"
)

logger.info("Schema so far:")
df_main.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Burst-Rate
logger.info(f"Generating submission burst rates based on [all_audit_past_year]")

df_burst = spark.sql(
    f"""
with submission_diffs as (
  select
    cfk_patron_id,
    create_ts,
    fk_signature_type_cd,
    lag(create_ts) over (
      partition by cfk_patron_id
      order by
        create_ts
    ) as prev_ts
  from
    all_audit_past_year al
),
time_deltas as (
  select
    cfk_patron_id,
    create_ts,
    prev_ts,
    fk_signature_type_cd,
    date_diff(minute, prev_ts, create_ts) as diff_minutes
  from
    submission_diffs
  where
    prev_ts is not null
),
burst_stats as (
  select
    cfk_patron_id,
    count(*) as burst_total_submissions,
    sum(
      case
        when diff_minutes <= {BURST_THRESHOLD_IN_MINUTES} then 1
        else 0
      end
    ) as burst_submissions,
    sum(
      case
        when diff_minutes <= {BURST_THRESHOLD_IN_MINUTES}
        and fk_signature_type_cd in ('D-DIRECT', 'R-DIRECT') then 1
        else 0
      end
    ) as sig_burst
  from
    time_deltas
  group by
    cfk_patron_id
)
select
  cfk_patron_id,
  burst_total_submissions,
  burst_submissions,
  round(burst_submissions * 1.0 / burst_total_submissions, 2) as submission_burst_rate,
  round(sig_burst * 1.0 / burst_total_submissions, 2) as d_sig_burst_rate
from
  burst_stats
"""
)

logger.info("Added feature schema:")
df_burst.printSchema()

burst_df = df_main.join(
    other=df_burst,
    on="cfk_patron_id",
    how="left",
)

logger.info("Schema so far:")
burst_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Account Age
logger.info("Generating account age based on [all_audit] and [eligible_patrons]")
age_df = spark.sql(
    f"""
  select distinct
    al.cfk_patron_id,
    ep.latest_create_ts,
    datediff(current_date, ep.latest_create_ts) as account_age_days,
    count(distinct al.serial_no) as total_submissions_account_age,
    round(
      count(distinct al.serial_no) / nullif(date_diff(current_date, ep.latest_create_ts), 0), 3
    ) as submissions_per_day
  from
    all_audit al
      join eligible_patrons ep
        on ep.cfk_patron_id = al.cfk_patron_id
  where
    al.create_ts between date(ep.latest_create_ts) - interval 1 day and current_date
  group by
    al.cfk_patron_id,
    ep.latest_create_ts
""")

logger.info("Added feature schema:")
age_df.printSchema()

df_age = burst_df.join(
    age_df.selectExpr("* except(ep.latest_create_ts)"),
    on="cfk_patron_id",
    how="left",
)

logger.info("Schema so far:")
df_age.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Submissions Benchmarks
logger.info(
    "Generating submission burst rate based on [all_audit] and [eligible_patrons]."
)
sub_df = spark.sql(
    f"""
with logs_with_age as (
  select
    al.cfk_patron_id,
    al.serial_no,
    case
      when datediff(date(al.create_ts), date(ep.latest_create_ts)) < 0 then 0
      else datediff(date(al.create_ts), date(ep.latest_create_ts))
    end as submission_day_age
  from
    all_audit al
      join eligible_patrons ep
        on ep.cfk_patron_id = al.cfk_patron_id
),
daily_cumulative as (
  select
    cfk_patron_id,
    submission_day_age,
    count(distinct serial_no) as daily_submissions
  from
    logs_with_age
  group by
    cfk_patron_id,
    submission_day_age
),
running_totals as (
  select
    cfk_patron_id,
    submission_day_age,
    sum(daily_submissions) over (
        partition by cfk_patron_id
        order by submission_day_age
        rows between unbounded preceding and current row
      ) as cumulative_submissions
  from
    daily_cumulative
),
benchmarks as (
  select
    *,
    case
      when submission_day_age <= 1 then 0
      when submission_day_age <= 30 then 30
      when submission_day_age <= 90 then 90
      when submission_day_age <= 180 then 180
      when submission_day_age <= 360 then 360
      else null
    end as benchmark_day
  from
    running_totals
  where
    submission_day_age <= 360
),
benchmark_days as (
  select
    0 as benchmark_day
  union all
  select
    30
  union all
  select
    90
  union all
  select
    180
  union all
  select
    360
),
patrons as (
  select distinct
    cfk_patron_id
  from
    running_totals
),
patron_benchmark_grid as (
  select
    p.cfk_patron_id,
    b.benchmark_day
  from
    patrons p cross join benchmark_days b
),
forward_filled as (
  select
    g.cfk_patron_id,
    applicant_bin,
    g.benchmark_day,
    max(cumulative_submissions) as cumulative_submissions_at_benchmark
  from
    patron_benchmark_grid g
      left join benchmarks b
        on g.cfk_patron_id = b.cfk_patron_id
        and b.benchmark_day <= g.benchmark_day
      left join `buckets`
        on `buckets`.cfk_patron_id = g.cfk_patron_id
  group by
    applicant_bin,
    g.cfk_patron_id,
    g.benchmark_day
),
stats as (
  select
    applicant_bin,
    benchmark_day,
    avg(cumulative_submissions_at_benchmark) as mean,
    stddev_pop(cumulative_submissions_at_benchmark) as std
  from
    forward_filled
  group by
    applicant_bin,
    benchmark_day
),
z_scores as (
  select
    b.cfk_patron_id,
    b.benchmark_day,
    coalesce(b.cumulative_submissions_at_benchmark, 0) as cumulative_submissions_at_benchmark,
    s.mean,
    s.std,
    round(
      (coalesce(b.cumulative_submissions_at_benchmark, 0) - s.mean) / nullif(s.std, 0), 3
    ) as z_score
  from
    forward_filled b
      join stats s
        on b.benchmark_day = s.benchmark_day
        and s.applicant_bin = b.applicant_bin
)
select
  cfk_patron_id,
  log1p(
    max(
      case
        when benchmark_day = 0 then cumulative_submissions_at_benchmark
      end
    )
  ) as log_cumulative_day_0,
  max(
    case
      when benchmark_day = 0 then mean
    end
  ) as mean_day_0,
  max(
    case
      when benchmark_day = 0 then std
    end
  ) as std_day_0,
  max(
    case
      when benchmark_day = 0 then z_score
    end
  ) as z_day_0,
  log1p(
    max(
      case
        when benchmark_day = 30 then cumulative_submissions_at_benchmark
      end
    )
  ) as log_cumulative_day_30,
  max(
    case
      when benchmark_day = 30 then mean
    end
  ) as mean_day_30,
  max(
    case
      when benchmark_day = 30 then std
    end
  ) as std_day_30,
  max(
    case
      when benchmark_day = 30 then z_score
    end
  ) as z_day_30,
  log1p(
    max(
      case
        when benchmark_day = 90 then cumulative_submissions_at_benchmark
      end
    )
  ) as log_cumulative_day_90,
  max(
    case
      when benchmark_day = 90 then mean
    end
  ) as mean_day_90,
  max(
    case
      when benchmark_day = 90 then std
    end
  ) as std_day_90,
  max(
    case
      when benchmark_day = 90 then z_score
    end
  ) as z_day_90,
  log1p(
    max(
      case
        when benchmark_day = 180 then cumulative_submissions_at_benchmark
      end
    )
  ) as log_cumulative_day_180,
  max(
    case
      when benchmark_day = 180 then mean
    end
  ) as mean_day_180,
  max(
    case
      when benchmark_day = 180 then std
    end
  ) as std_day_180,
  max(
    case
      when benchmark_day = 180 then z_score
    end
  ) as z_day_180,
  log1p(
    max(
      case
        when benchmark_day = 360 then cumulative_submissions_at_benchmark
      end
    )
  ) as log_cumulative_day_360,
  max(
    case
      when benchmark_day = 360 then mean
    end
  ) as mean_day_360,
  max(
    case
      when benchmark_day = 360 then std
    end
  ) as std_day_360,
  max(
    case
      when benchmark_day = 360 then z_score
    end
  ) as z_day_360
from
  z_scores
group by
  cfk_patron_id
"""
)

logger.info("Added feature schema:")
sub_df.printSchema()

submission_benchmarks_df = df_age.join(
    other=sub_df,
    on="cfk_patron_id",
    how="left",
)

logger.info("Schema so far:")
submission_benchmarks_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Name Similarity
logger.info("Generating name similarity based on [all_audit]")
sim_df = spark.sql(
    f"""
  select distinct
    cfk_patron_id,
    dn_patron_first_nm,
    dn_patron_middle_nm,
    dn_patron_last_nm
  from
    all_audit
  where
    not (
      (
        dn_patron_first_nm is null
        or trim(dn_patron_first_nm) = ''
      )
      and (
        dn_patron_middle_nm is null
        or trim(dn_patron_middle_nm) = ''
      )
      and (
        dn_patron_middle_nm is null
        or trim(dn_patron_middle_nm) = ''
      )
    )
"""
)

sim_df = sim_df.toPandas()
name_df = compute_name_similarity_fuzzy_jaccard(sim_df)

name_spark_df = spark.createDataFrame(name_df)

logger.info("Added feature schema:")
name_spark_df.printSchema()

df_name = submission_benchmarks_df.join(
    other=name_spark_df, on="cfk_patron_id", how="left"
)

logger.info("Schema so far:")
df_name.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Average Classes
logger.info("Generating name class statistics based on [all_audit]")

class_df = spark.sql(
    f"""
with ranked_audit as (
  select
    *,
    row_number() over (partition by cfk_patron_id, serial_no order by create_ts) as rn
  from
    all_audit
)
select
  a.cfk_patron_id,
  count(*) submissions,
  nvl(avg(ml.am_cls_ct_actv), 0) avg_class_count,
  nvl(sum(ml.am_cls_ct_actv), 0) as sum_count
from
  ranked_audit a
    inner join {reporting_catalog}.silver.milestone ml
      on ml.ser_num = a.serial_no
where
  rn = 1
group by
  cfk_patron_id
"""
)

logger.info("Added feature schema:")
class_df.printSchema()

df_class = df_name.join(other=class_df, on="cfk_patron_id", how="left")

logger.info("Schema so far:")
df_class.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Hourly Entropy
logger.info("Generating name class statistics based on [all_audit_past_year]")

entropy_df = spark.sql(
    f"""
  with hourly_distribution as (
    select
      cfk_patron_id,
      extract(hour from create_ts) as hour,
      count(*) as hour_count
    from
      all_audit_past_year
    group by
      cfk_patron_id,
      extract(hour from create_ts)
  ),
  total_counts as (
    select
      cfk_patron_id,
      sum(hour_count) as total_submissions
    from
      hourly_distribution
    group by
      cfk_patron_id
  ),
  hourly_probabilities as (
    select
      hd.cfk_patron_id,
      hd.hour,
      hd.hour_count,
      tc.total_submissions,
      (hd.hour_count * 1.0) / tc.total_submissions as prob
    from
      hourly_distribution hd
        join total_counts tc
          on hd.cfk_patron_id = tc.cfk_patron_id
  ),
  entropy_per_applicant as (
    select
      cfk_patron_id,
      -sum(prob * log(2, prob)) as hourly_entropy
    from
      hourly_probabilities
    group by
      cfk_patron_id
  )
  select
    *
  from
    entropy_per_applicant
"""
)

logger.info("Added feature schema:")
entropy_df.printSchema()

df_hourly_entropy = df_class.join(other=entropy_df, on="cfk_patron_id", how="left")

logger.info("Schema so far:")
df_hourly_entropy.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Entropy
logger.info("Generating entropy based on [all_audit_past_year]")

weekday_entropy_df = spark.sql(
    f"""
  with day as (
    select
      cfk_patron_id,
      dayofweek(create_ts) as weekday
    from
      all_audit_past_year
  ),
  weekday_counts as (
    select
      cfk_patron_id,
      weekday,
      count(*) as weekday_count
    from
      day
    group by
      cfk_patron_id,
      weekday
  ),
  total_counts as (
    select
      cfk_patron_id,
      sum(weekday_count) as total_submissions
    from
      weekday_counts
    group by
      cfk_patron_id
  ),
  weekday_probs as (
    select
      wc.cfk_patron_id,
      wc.weekday,
      wc.weekday_count,
      tc.total_submissions,
      cast(wc.weekday_count as double) / tc.total_submissions as p
    from
      weekday_counts wc
        join total_counts tc
          on wc.cfk_patron_id = tc.cfk_patron_id
  ),
  entropy_calc as (
    select
      cfk_patron_id,
      -sum(p * log(2, p)) as weekday_entropy
    from
      weekday_probs
    group by
      cfk_patron_id
  )
  select
    *
  from
    entropy_calc
  order by
    weekday_entropy desc
"""
)

logger.info("Added feature schema:")
weekday_entropy_df.printSchema()

df_weekly_entropy = df_hourly_entropy.join(
    other=weekday_entropy_df, on="cfk_patron_id", how="left"
)

logger.info("Schema so far:")
df_weekly_entropy.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: IP Statistics
logger.info(
    "Generating IP statistics based on [all_audit_past_year] and [eligible_patrons]"
)

ip_df = spark.sql(
    f"""
  with base as (
    select
      al.cfk_patron_id,
      create_ts,
      ip_address_tx,
      row_number() over (
        partition by al.cfk_patron_id
        order by
          create_ts
      ) as rn
    from
      all_audit_past_year al
      inner join eligible_patrons ep on al.cfk_patron_id = ep.cfk_patron_id
  ),
  ip_sequence as (
    select
      b.*,
      lag(ip_address_tx) over (
        partition by b.cfk_patron_id
        order by
          rn
      ) as prev_ip
    from
      base b
  ),
  ip_counts as (
    select
      cfk_patron_id,
      ip_address_tx,
      count(*) as ip_count
    from
      base
    group by
      cfk_patron_id,
      ip_address_tx
  ),
  total_ip_counts as (
    select
      cfk_patron_id,
      sum(ip_count) as total
    from
      ip_counts
    group by
      cfk_patron_id
  ),
  ip_probs as (
    select
      ic.cfk_patron_id,
      ic.ip_address_tx,
      ic.ip_count,
      tic.total,
      cast(ic.ip_count as double) / tic.total as p
    from
      ip_counts ic
      join total_ip_counts tic on ic.cfk_patron_id = tic.cfk_patron_id
  ),
  ip_entropy_calc as (
    select
      cfk_patron_id,
      count(*) as unique_ip_count,
      - sum(p * log(2, p)) as ip_entropy,
      log(2, count(*)) as max_entropy
    from
      ip_probs
    group by
      cfk_patron_id
  ),
  ip_switches as (
    select
      cfk_patron_id,
      count(*) as total_submissions,
      sum(
        case
          when ip_address_tx != prev_ip then 1
          else 0
        end
      ) as ip_switch_count
    from
      ip_sequence
    where
      prev_ip is not null
    group by
      cfk_patron_id
  ),
  final_features as (
    select
      e.cfk_patron_id,
      e.unique_ip_count,
      round(e.ip_entropy, 4) as ip_entropy,
      round(e.ip_entropy / nullif(e.max_entropy, 0), 4) as normalized_ip_entropy,
      s.total_submissions,
      s.ip_switch_count,
      round(s.ip_switch_count * 1.0 / s.total_submissions, 4) as ip_switch_burst_rate
    from
      ip_entropy_calc e
      join ip_switches s on e.cfk_patron_id = s.cfk_patron_id
  )
  select
    cfk_patron_id,
    case
      when normalized_ip_entropy is not null then normalized_ip_entropy
      else 0
    end as normalized_ip_entropy,
    ip_switch_burst_rate
  from
    final_features
  order by
    ip_switch_burst_rate desc
"""
)
logger.info("Added feature schema:")
ip_df.printSchema()

ent_df = df_weekly_entropy.join(other=ip_df, on="cfk_patron_id", how="left")

logger.info("Schema so far:")
ent_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: Add Signatory Clusters
logger.info("Generating IP statistics based on [all_audit_past_year]")

df_signers = spark.sql(
    f"""
    select
        cfk_patron_id,
        signatory_nm,
        fk_signature_type_cd,
        create_ts,
        regexp_replace(lower(trim(signatory_nm)), "[^a-z]", "") as signer_clean
    from
        all_audit_past_year
    where 
        fk_signature_type_cd not in {BAD_SIGNATURE_CATEGORIES}
"""
)
signers_arr = df_signers.groupBy("cfk_patron_id").agg(
    F.collect_set("signer_clean").alias("signer_names")
)

signer_map = signers_arr.withColumn("signer_cluster_map", cluster_udf("signer_names"))

cluster_lookup = signer_map.select(
    "cfk_patron_id",
    F.explode("signer_cluster_map").alias("signer_clean", "signer_cluster_id"),
)

labeled = df_signers.join(
    cluster_lookup, on=["cfk_patron_id", "signer_clean"], how="left"
)
sig_cnt = labeled.groupBy(
    "cfk_patron_id", "signer_cluster_id", "fk_signature_type_cd"
).count()

tot_cnt = sig_cnt.groupBy("cfk_patron_id", "signer_cluster_id").agg(
    F.sum("count").alias("total")
)

entropy = (
    sig_cnt.join(tot_cnt, on=["cfk_patron_id", "signer_cluster_id"])
    .withColumn("p", F.col("count") / F.col("total"))
    .groupBy("cfk_patron_id", "signer_cluster_id")
    .agg((-F.sum(F.col("p") * F.log2("p"))).alias("sig_type_entropy"))
)

w = Window.partitionBy("cfk_patron_id", "signer_cluster_id").orderBy("create_ts")

changes = (
    labeled.withColumn("prev_type", F.lag("fk_signature_type_cd").over(w))
    .withColumn(
        "changed",
        F.when(
            F.col("prev_type").isNotNull()
            & (F.col("prev_type") != F.col("fk_signature_type_cd")),
            1,
        ).otherwise(0),
    )
    .groupBy("cfk_patron_id", "signer_cluster_id")
    .agg(
        (
            F.sum("changed")
            / F.when(F.count("*") > 1, F.count("*") - 1).otherwise(F.lit(1))
        ).alias("sig_type_change_rate")
    )
)

cluster_metrics = entropy.join(changes, on=["cfk_patron_id", "signer_cluster_id"])

signer_names_by_cluster = labeled.groupBy("cfk_patron_id", "signer_cluster_id").agg(
    F.collect_set("signer_clean").alias("signers")
)

similarity_scores = signer_names_by_cluster.withColumn(
    "name_stats", name_stats_udf("signers")
).select(
    "cfk_patron_id",
    "signer_cluster_id",
    "name_stats.avg_name_similarity",
    "name_stats.max_name_distance",
    "name_stats.cluster_name_count",
)
cluster_all_metrics = cluster_metrics.join(
    similarity_scores, on=["cfk_patron_id", "signer_cluster_id"]
)

df_sig = cluster_all_metrics.groupBy("cfk_patron_id").agg(
    F.avg("sig_type_entropy").alias("avg_sig_type_entropy"),
    F.avg("sig_type_change_rate").alias("avg_sig_type_change_rate"),
    F.max("sig_type_entropy").alias("max_sig_type_entropy"),
    F.max("sig_type_change_rate").alias("max_sig_type_change_rate"),
    F.avg("avg_name_similarity").alias("avg_name_similarity"),
    F.max("max_name_distance").alias("max_name_distance"),
    F.avg("cluster_name_count").alias("avg_cluster_size"),
)

logger.info("Added feature schema:")
df_sig.printSchema()

sig_df = ent_df.join(other=df_sig, on="cfk_patron_id", how="left")

logger.info("Schema so far:")
sig_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Feature: International / Domestic Behavior
logger.info(
    "Generating international / domestic submission behavior based on [all_audit]"
)

df_int = spark.sql(
    f"""
  with params as (
    select
      10 as N10,
      50 as N50
  ),
  tagged as (
    select
      npid.serial_no,
      npid.cfk_patron_id,
      fk_form_cd,
      npid.create_ts,
      case
        when tpr.ctry_cd = 'US' then 0
        else 1
      end as is_intl,
      row_number() over (
        partition by npid.cfk_patron_id
        order by
          npid.create_ts desc
      ) as rn_desc
    from
      {reporting_catalog}.silver.owner tpr
      join all_audit npid on npid.serial_no = tpr.ser_num
    where
      tpr.current_owner = 'Y'
      and npid.fk_transaction_type_cd = 'Submission'
  ),
  submission_counts as (
    select
      cfk_patron_id,
      count(distinct serial_no) as total_submissions
    from
      tagged
    group by
      cfk_patron_id
  ),
  eligible_patrons as (
    select
      cfk_patron_id
    from
      tagged
    where
      create_ts >= add_months(current_date, -12)
    group by
      cfk_patron_id
    having
      count(*) >= 10
      and sum(
        case
          when is_intl = 1 then 1
          else 0
        end
      ) >= 1
      and sum(
        case
          when is_intl = 0 then 1
          else 0
        end
      ) >= 1
  ),
  split as (
    select
      t.*,
      case
        when t.rn_desc <= (
          select
            N10
          from
            params
        ) then 'last_10'
        when t.rn_desc <= (
          select
            N50
          from
            params
        ) then 'last_50'
        else 'baseline'
      end as window_type
    from
      tagged t
      join eligible_patrons ep on t.cfk_patron_id = ep.cfk_patron_id
  ),
  counts as (
    select
      cfk_patron_id,
      window_type,
      count(*) as total,
      sum(is_intl) as intl
    from
      split
    group by
      cfk_patron_id,
      window_type
  ),
  pivoted as (
    select
      cfk_patron_id,
      max(
        case
          when window_type = 'baseline' then total
        end
      ) as total_baseline,
      max(
        case
          when window_type = 'baseline' then intl
        end
      ) as intl_baseline,
      max(
        case
          when window_type = 'last_50' then total
        end
      ) as total_50,
      max(
        case
          when window_type = 'last_50' then intl
        end
      ) as intl_50,
      max(
        case
          when window_type = 'last_10' then total
        end
      ) as total_10,
      max(
        case
          when window_type = 'last_10' then intl
        end
      ) as intl_10
    from
      counts
    group by
      cfk_patron_id
  ),
  spike_metrics as (
    select
      cfk_patron_id,
      case
        when total_baseline > 0 then intl_baseline / total_baseline
        else 0.0
      end as baseline_intl_ratio,
      case
        when total_50 > 0 then intl_50 / total_50
        else 0.0
      end as recent_50_intl_ratio,
      case
        when total_10 > 0 then intl_10 / total_10
        else 0.0
      end as recent_10_intl_ratio,
      case
        when total_50 > 0
        and total_baseline > 0 then (
          (intl_50 * 1.0 / total_50) - (intl_baseline * 1.0 / total_baseline)
        ) / sqrt(
          (intl_baseline * 1.0 / total_baseline) * (1 - intl_baseline * 1.0 / total_baseline) / total_50
        )
        else null
      end as intl_spike_zscore_50,
      case
        when total_10 > 0
        and total_baseline > 0 then (
          (intl_10 * 1.0 / total_10) - (intl_baseline * 1.0 / total_baseline)
        ) / sqrt(
          (intl_baseline * 1.0 / total_baseline) * (1 - intl_baseline * 1.0 / total_baseline) / total_10
        )
        else null
      end as intl_spike_zscore_10,
      total_10,
      intl_10,
      total_50,
      intl_50,
      total_baseline,
      intl_baseline
    from
      pivoted
  ),
  streaks as (
    select
      cfk_patron_id,
      max(rn_desc) as intl_streak_len
    from
      (
        select
          *,
          sum(
            case
              when is_intl = 0 then 1
              else 0
            end
          ) over (
            partition by cfk_patron_id
            order by
              create_ts desc rows between unbounded preceding
              and current row
          ) as domestic_break
        from
          tagged
      ) t
    where
      domestic_break = 0
    group by
      cfk_patron_id
  )
  select
    sp.cfk_patron_id,
    coalesce(sp.baseline_intl_ratio, 0.0) as baseline_intl_ratio,
    coalesce(sp.recent_50_intl_ratio, 0.0) as recent_50_intl_ratio,
    coalesce(sp.recent_10_intl_ratio, 0.0) as recent_10_intl_ratio,
    coalesce(sp.intl_spike_zscore_50, 0.0) as intl_spike_zscore_50,
    coalesce(sp.intl_spike_zscore_10, 0.0) as intl_spike_zscore_10,
    coalesce(st.intl_streak_len, 0) as intl_streak_len
  from
    spike_metrics sp
    left join streaks st on sp.cfk_patron_id = st.cfk_patron_id
"""
)

logger.info("Added feature schema:")
df_int.printSchema()

output = sig_df.join(other=df_int, on="cfk_patron_id", how="left")

logger.info("Schema so far:")
output.printSchema()

# COMMAND ----------

# DBTITLE 1,Output Column Key
output_before_imputation_columns: list[str] = list(
    set(output.columns)
    & set([column for column in OUTPUT_COLUMNS if column not in IGNORED_MERGE_COLUMNS])
)
sorted(output_before_imputation_columns)

# COMMAND ----------

# DBTITLE 1,Generate Completeness Column
output_before_imputation = output.select(output_before_imputation_columns)

num_feature_columns: int = len(output_before_imputation_columns) - len(
    NON_FEATURE_COLUMNS
)

logger.info(
    f"`completeness` will be based on {num_feature_columns}, down [{len(NON_FEATURE_COLUMNS)}] from [{len(output_before_imputation_columns)}] columns."
)

count_of_null_columns_per_record = " + ".join(
    [
        f"iff({column_name} is null, 0, 1)"
        for column_name in output_before_imputation_columns
        if column_name not in NON_FEATURE_COLUMNS
    ]
)

logger.info("Generating completeness...")
completeness_formula = (
    f"({count_of_null_columns_per_record}) / {num_feature_columns} completeness"
)
output_before_imputation = output_before_imputation.selectExpr(
    f"*",
    "current_date load_date",
    "true latest",
    completeness_formula,
)
output_after_imputation = output_before_imputation.fillna(NULL_COLUMN_MAP_VALUES)
output_after_imputation.createOrReplaceTempView("staging")
output_count: int = output_after_imputation.count()

# COMMAND ----------

# DBTITLE 1,Incoming Data
display(output_after_imputation.limit(50))
logger.info("Schema so far:")
spark.sql(f"select * from {FEATURE_STORE_LOAD_HISTORY_FQN}").printSchema()

# COMMAND ----------

# DBTITLE 1,Display: New Columns Added If Not In Target
a = set(output_after_imputation.columns)
b = set(spark.sql(f"select * from {FEATURE_STORE_LOAD_HISTORY_FQN}").columns)
a.difference(b)

# COMMAND ----------

# DBTITLE 1,Insert: Today's Load
display(spark.sql(TODAYS_MERGE_QRY))
spark.sql(f"select * from {FEATURE_STORE_LOAD_HISTORY_FQN}").printSchema()
display(spark.sql(f"select * from {FEATURE_STORE_LOAD_HISTORY_FQN} where latest = true").limit(50))

# COMMAND ----------

# DBTITLE 1,Insert: Non-Exclusive
display(spark.sql(NON_EXCLUSIVE_MERGE_QRY))
spark.sql(f"select * from {FEATURE_STORE_NON_EXCLUSIVE_FQN}").printSchema()
display(spark.sql(f"select * from {FEATURE_STORE_NON_EXCLUSIVE_FQN} where latest = true").limit(50))

# COMMAND ----------

# DBTITLE 1,Insert: Exclusive
display(spark.sql(EXCLUSIVE_MERGE_QRY))
spark.sql(f"select * from {FEATURE_STORE_EXCLUSIVE_FQN}").printSchema()
display(spark.sql(f"select * from {FEATURE_STORE_EXCLUSIVE_FQN} where latest = true").limit(50))