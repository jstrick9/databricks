# Databricks notebook source
# MAGIC %md
# MAGIC # Table Overview
# MAGIC ## Generated Views
# MAGIC These are views that serve more or less as helpers to load data into the gold level table result sets.
# MAGIC - HISTORICAL_ANOMALY_VIEW: Temporary result set containing the current run results of anomalies that can include accounts that have been seen before. This is likely slowly changing.
# MAGIC - CURRENT_ANOMALY_VIEW: Temporary result set containing the current run results of anomalies that cannot include patrons that have been seen before. This should always have new, never seen accounts.
# MAGIC - HISTORICAL_STAGING_VIEW: This is the upsert query of the HISTORICAL_ANOMALY_VIEW.
# MAGIC - CURRENT_STAGING_VIEW: This is the upsert query of the CURRENT_ANOMALY_VIEW.
# MAGIC ## Tables
# MAGIC ### Source
# MAGIC - UNSUPERVISED_FEATURE_HISTORICAL: This is the source data for both FEATURE_STORE_NON_EXCLUSIVE and FEATURE_STORE_EXCLUSIVE.
# MAGIC - FEATURE_STORE_NON_EXCLUSIVE: Materialized feature store that can include accounts that have been seen before. This is likely slowly changing and will always grow over time as accounts are added.
# MAGIC - FEATURE_STORE_EXCLUSIVE: Materialized feature store that cannot include accounts that have been seen before. 
# MAGIC ### Target
# MAGIC - NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED: Materialized result set from running the model. It is derived from the UNSUPERVISED_FEATURE_HISTORICAL feature store as a result of upserting HISTORICAL_STAGING_VIEW.
# MAGIC - EXLUSIVE_CUMULATIVE_UNSUPERVISED: Materialized result set from running the model. It is derived from the FEATURE_STORE_NON_EXCLUSIVE feature store as a result of upserting HISTORICAL_STAGING_VIEW.
# MAGIC - ANOMALY_REPORT: Materialized result set containing the historical record of seen accounts that were anomalies. It is derived from NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED.

# COMMAND ----------

# DBTITLE 1,Imports
import datetime

import logging

import numpy as np
import pandas as pd

import pyspark

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

from mlflow.client import MlflowClient
from mlflow.data import load_delta
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature

import warnings

import uuid

import sqlparse

# COMMAND ----------

# DBTITLE 1,Config
warnings.filterwarnings("ignore")

dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Shared Library
# MAGIC %run ./../shared/ntb_common_func_and_params 

# COMMAND ----------

# DBTITLE 1,Read Configs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
print(reporting_catalog)

# COMMAND ----------

# DBTITLE 1,Temporary: Common Functions
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

# COMMAND ----------

# DBTITLE 1,Globals
MERGE_KEY: str = (
    "target.cfk_patron_id = source.cfk_patron_id and target.load_date = source.load_date"
)

UNSUPERVISED_FEATURE_HISTORICAL: str = "unsupervised_anomalies_feature_load_history"
UNSUPERVISED_FEATURE_HISTORICAL_FQN: str = (
    f"{reporting_catalog}.silver.{UNSUPERVISED_FEATURE_HISTORICAL}"
)

FEATURE_STORE_NON_EXCLUSIVE: str = "unsupervised_anomalies_features_non_exclusive"
FEATURE_STORE_NON_EXCLUSIVE_FQN: str = (
    f"{reporting_catalog}.gold.{FEATURE_STORE_NON_EXCLUSIVE}"
)

FEATURE_STORE_EXCLUSIVE: str = "unsupervised_anomalies_features_exclusive"
FEATURE_STORE_EXCLUSIVE_FQN: str = f"{reporting_catalog}.gold.{FEATURE_STORE_EXCLUSIVE}"

NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED: str = "unsupervised_anomalies_cumulative"
NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED_FQN: str = (
    f"{reporting_catalog}.gold.{NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED}"
)

EXCLUSIVE_CUMULATIVE_UNSUPERVISED: str = "unsupervised_anomalies_cumulative_exclusive"
EXCLUSIVE_CUMULATIVE_UNSUPERVISED_FQN: str = (
    f"{reporting_catalog}.gold.{EXCLUSIVE_CUMULATIVE_UNSUPERVISED}"
)

ANOMALY_REPORT: str = "unsupervised_anomalies"
ANOMALY_REPORT_FQN: str = f"{reporting_catalog}.gold.{ANOMALY_REPORT}"

HISTORICAL_ANOMALY_VIEW: str = "historical_view"
CURRENT_ANOMALY_VIEW: str = "current_view"

HISTORICAL_STAGING_VIEW: str = "historical_staging"
CURRENT_STAGING_VIEW: str = "current_staging"

STRATA: list[str] = ["applicant_bin", "selected_role"]

IGNORED_COLUMNS: list[str] = [
    "load_date",
    "applicant_bin",
    "cfk_patron_id",
    "selected_role",
    "create_ts",
    "create_user",
    "completeness",
]

CONTAMINATION_THRESHOLD: float = 0.02
SEED: int = 42

HYPERPARAMETERS = {"contamination": CONTAMINATION_THRESHOLD, "random_state": SEED}

CURRENT_USER: str = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
EXPERIMENT_LOCATION_EXCLUSIVE: str = (
    f"/Users/{CURRENT_USER}/unsupervised_anomaly_model_development_exclusive_tracking"
)
EXPERIMENT_LOCATION_NON_EXCLUSIVE: str = (
    f"/Users/{CURRENT_USER}/unsupervised_anomaly_model_development_non_exclusive_tracking"
)

TODAY: str = datetime.datetime.now().strftime("%Y%m%d")

# COMMAND ----------

# DBTITLE 1,Initialize Logger
logger = logging.getLogger("ml_auditor")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.info("Initialized logger.")

# COMMAND ----------

# DBTITLE 1,Temporary: Output Column
# TODO: remove duplicative code via a) move to common functions or b) move to YAML per Josh's suggestion
OUTPUT_COLUMNS: list = [
    "load_date",
    "latest",
    "cfk_patron_id",
    "applicant_bin",
    "selected_role",
    "is_anomaly",
    "anomaly_score",
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

NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY: str = create_merge_query(
    target=NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED_FQN, source=HISTORICAL_STAGING_VIEW
)

EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY: str = create_merge_query(
    target=EXCLUSIVE_CUMULATIVE_UNSUPERVISED_FQN, source=CURRENT_STAGING_VIEW
)

logger.info("The following statements will be used for each MERGE:")
for statement in [
    NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY,
    EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY,
]:
    formatted_statement = sqlparse.format(
        statement, reindent=True, keyword_case="lower"
    )
    logger.info(formatted_statement)

# COMMAND ----------

# DBTITLE 1,Condition: MLFlow Enabled
try:
    dbutils.widgets.text("mlflow_enabled", "N")
    mlflow_enabled: bool = (
        True if dbutils.widgets.get("mlflow_enabled") == "Y" else False
    )
except Exception:
    logger.error("There was an issue reading the parameter flag for enabling MLFLow.")
    logger.debug("Setting variable `mlflow_enabled` = False")
    mlflow_enabled: bool = False
if mlflow_enabled:
    logger.info(
        "MLFlow flag was enabled will be used to log and create artifacts for this run."
    )
else:
    logger.info(
        "MLFlow flag was disabled. MlfLow will not be used to log and create artifacts for this run."
    )

# COMMAND ----------

# DBTITLE 1,Functions
def generate_feature_insert_query_from_view(staging_view: str):
    return f"""
    select
        current_date load_date,
        true latest,
        iff(is_anomaly = -1, true, false) is_anomaly,
        cast(anomaly_score as double) anomaly_score,
        cfk_patron_id,
        applicant_bin,
        selected_role,
        num_has_sponsored,
        num_has_been_sponsored_by,
        is_ten_minute_rapid_filer,
        is_one_minute_rapid_filer,
        num_times_owner_signed_as_attorney,
        max_num_distinct_different_hand_and_e_sign_as_owner_same_ip,
        max_num_distinct_different_names,
        max_num_distinct_different_hand_and_e_sign_as_attorney_same_ip,
        max_num_distinct_signatory_names_with_direct_signature_from_same_ip,
        num_distinct_signatory_names_with_direct_signature,
        has_submissions_every_fifteen_for_six_hours_or_more,
        has_submissions_without_six_hour_break_for_one_day,
        submission_burst_rate,
        d_sig_burst_rate,
        submissions_per_day,
        z_day_0,
        z_day_30,
        z_day_90,
        z_day_180,
        z_day_360,
        log_cumulative_day_0,
        log_cumulative_day_30,
        log_cumulative_day_90,
        log_cumulative_day_180,
        log_cumulative_day_360,
        name_similarity_score,
        avg_class_count,
        hourly_entropy,
        weekday_entropy,
        normalized_ip_entropy,
        ip_switch_burst_rate,
        avg_sig_type_entropy,
        avg_sig_type_change_rate,
        max_sig_type_entropy,
        max_sig_type_change_rate,
        avg_name_similarity,
        max_name_distance,
        avg_cluster_size,
        baseline_intl_ratio,
        recent_50_intl_ratio,
        recent_10_intl_ratio,
        intl_spike_zscore_50,
        intl_spike_zscore_10,
        intl_streak_len,
        completeness,
        current_timestamp create_ts,
        'UNSUPERVISED_MODEL_EXECUTION_ETL' create_user
    from
        {staging_view}
    """


def get_feature_store_reference(
    include_historical_anomalies: bool,
    logger: logging.Logger = logger,
):
    """
    Helper function to get the Pandas DataFrame of features for the anomaly model.
    """
    logger.info("Getting latest feature store...")
    if include_historical_anomalies:
        feature_store = load_delta(
            table_name=FEATURE_STORE_NON_EXCLUSIVE_FQN,
            name="unsupervised_anomalies_non_exclusive",
        )
    else:
        feature_store = load_delta(
            table_name=FEATURE_STORE_EXCLUSIVE_FQN,
            name="unsupervised_anomalies_exclusive",
        )
    logger.info(
        f"Incoming Feature store record count: [{feature_store.df.where("latest = true").count()}]."
    )
    return feature_store


def convert_features_to_pandas(
    feature_store: mlflow.data.spark_dataset.SparkDataset,
    logger: logging.Logger = logger,
    ignored_columns: list[str] = IGNORED_COLUMNS,
) -> pd.core.frame.DataFrame:
    """
    Helper to convert features to Pandas DataFrame.
    """
    logger.info("Converting to Pandas DataFrame...")
    feature_store_pd: pd.core.frame.DataFrame = feature_store.df.where(
        "latest = true"
    ).toPandas()
    logger.info("Generated Pandas DataFrame.")
    if feature_store_pd.empty:
        logger.warning("`feature_store_pd` is empty.")
    return feature_store_pd


def generate_anomalies(
    view_name: str,
    converted_feature_store: pd.core.frame.DataFrame,
    include_historical_anomalies: bool,
    logger: logging.Logger = logger,
    contamination_threshold: float = CONTAMINATION_THRESHOLD,
    ignored_columns: list[str] = IGNORED_COLUMNS,
    seed: int = SEED,
    enable_mlflow: bool = mlflow_enabled,
) -> tuple:
    """
    Generates the anomalies based on the data provided. It currently
    doesn't support batch score so the sklearn piece is on the driver itself.

    returns a tuple in the form of (run_id, model_uri, name)
    """
    if include_historical_anomalies:
        experiment_location: str = EXPERIMENT_LOCATION_EXCLUSIVE
    else:
        experiment_location: str = EXPERIMENT_LOCATION_NON_EXCLUSIVE

    if enable_mlflow:
        print(f"MLflow Tracking URI: {mlflow.get_tracking_uri()}")
        mlflow.login()
        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_location)

        with mlflow.start_run() as run:
            run_id: str = run.info.run_id
            mlflow.set_tag(
                "mlflow.note.content",
                "Isolation Forest responsible for identifying anomalous accounts.",
            )
            mlflow.set_tag("data_version", TODAY)
            feature_columns: list[str] = [
                col
                for col in converted_feature_store.columns.tolist()
                if col not in ignored_columns
            ]

            category: str = (
                "historical" if include_historical_anomalies else "non_historical"
            )

            registered_name: str = (
                f"{reporting_catalog}.gold.anomalous_accounts_{category}"
            )

            anomaly_results = []
            for bin_value, group_df in converted_feature_store.groupby(STRATA):
                X = group_df[feature_columns]
                model = Pipeline(
                    steps=[
                        ("scale", StandardScaler()),
                        ("isolation_forest", IsolationForest(**HYPERPARAMETERS)),
                    ]
                )

                mlflow.log_params(HYPERPARAMETERS)
                model.fit(X)

                decisions = model.named_steps["isolation_forest"].decision_function(
                    model.named_steps["scale"].transform(X)
                )
                predictions = model.named_steps["isolation_forest"].predict(
                    model.named_steps["scale"].transform(X)
                )

                results = group_df.copy()
                results["anomaly_score"] = decisions
                results["is_anomaly"] = predictions

                percentage_anomalies = float(predictions.mean())
                mlflow.log_metric("percentage_anomalies_flagged", percentage_anomalies)
                mlflow.log_metric("decision_score_min", float(decisions.min()))
                mlflow.log_metric("decision_score_max", float(decisions.max()))
                mlflow.log_metric("decision_score_mean", float(decisions.mean()))

                signature = infer_signature(X, predictions[:10])
                mlflow.sklearn.log_model(
                    model,
                    artifact_path="model",
                    signature=signature,
                    input_example=X.head(10),
                    registered_model_name=registered_name,
                )

                anomaly_results.append(results)

            scores = pd.concat(anomaly_results).reset_index(drop=True)
            anomalies = spark.createDataFrame(scores)
            display(anomalies.limit(10))
            anomalies.createOrReplaceTempView(view_name)
            logger.info(f"Created view: {view_name}")
            model_uri, name = f"runs:/{run_id}/model", registered_name
            return run_id, model_uri, name
    else:
        feature_columns: list[str] = [
            col
            for col in converted_feature_store.columns.tolist()
            if col not in ignored_columns
        ]

        anomaly_results = []
        for bin_value, group_df in converted_feature_store.groupby(STRATA):
            X = group_df[feature_columns]
            model = Pipeline(
                steps=[
                    ("scale", StandardScaler()),
                    ("isolation_forest", IsolationForest(**HYPERPARAMETERS)),
                ]
            )

            model.fit(X)

            decisions = model.named_steps["isolation_forest"].decision_function(
                model.named_steps["scale"].transform(X)
            )

            predictions = model.named_steps["isolation_forest"].predict(
                model.named_steps["scale"].transform(X)
            )

            results = group_df.copy()
            results["anomaly_score"] = decisions
            results["is_anomaly"] = predictions
            anomaly_results.append(results)

        scores = pd.concat(anomaly_results).reset_index(drop=True)
        anomalies = spark.createDataFrame(scores)
        display(anomalies.limit(10))
        anomalies.createOrReplaceTempView(view_name)
        logger.info(f"Created view: {view_name}")

        run_id: str = uuid.uuid4()
        logger.info(f"Placeholder UUID was created to simulate MLFlow Run ID: {run_id}")
        model_uri, name = "no_uri", "unregistered"
        return run_id, model_uri, name

# COMMAND ----------

# DBTITLE 1,Execute: Non-Exclusive Results
insert_query: str = generate_feature_insert_query_from_view(HISTORICAL_ANOMALY_VIEW)

historical_feature_store = get_feature_store_reference(
    include_historical_anomalies=True
)
historical_converted_feature_store = convert_features_to_pandas(
    historical_feature_store
)

if not historical_converted_feature_store.empty:
    run_id, model_uri, name = generate_anomalies(
        view_name=HISTORICAL_ANOMALY_VIEW,
        converted_feature_store=historical_converted_feature_store,
        include_historical_anomalies=True,
    )

    if mlflow_enabled:
        model_version = mlflow.register_model(model_uri=model_uri, name=name)
        client = MlflowClient()
        alias = name.replace(".", "_")
        client.set_registered_model_alias(name, alias, model_version.version)

        client.update_model_version(
            name=name,
            version=model_version.version,
            description="Isolation Forest model for anomalous accounts.",
        )
        client.set_model_version_tag(name, model_version.version, "date_version", TODAY)

    spark.sql(insert_query).createOrReplaceTempView(HISTORICAL_STAGING_VIEW)
    display(
        spark.sql(
            """
            select
                count(1),
                load_date,
                cfk_patron_id
            from
                historical_staging
            group by
                all
        """
        )
    )
    display(spark.sql(NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY))
else:
    logger.warning(
        "No model run was generated because the input or result set was empty."
    )

# COMMAND ----------

# DBTITLE 1,Execute: Exclusive Results
insert_query: str = generate_feature_insert_query_from_view(
    EXCLUSIVE_CUMULATIVE_UNSUPERVISED
)

exclusive_historical_feature_store = get_feature_store_reference(
    include_historical_anomalies=False
)

exclusive_historical_converted_feature_store = convert_features_to_pandas(
    exclusive_historical_feature_store
)
if not exclusive_historical_converted_feature_store.empty:
    run_id, model_uri, name = generate_anomalies(
        view_name=EXCLUSIVE_CUMULATIVE_UNSUPERVISED,
        converted_feature_store=exclusive_historical_converted_feature_store,
        include_historical_anomalies=False,
    )
    if mlflow_enabled:
        model_version = mlflow.register_model(model_uri=model_uri, name=name)
        client = MlflowClient()
        alias = name.replace(".", "_")
        client.set_registered_model_alias(name, alias, model_version.version)

        client.update_model_version(
            name=name,
            version=model_version.version,
            description="Isolation Forest model for anomalous accounts.",
        )
        client.set_model_version_tag(name, model_version.version, "date_version", TODAY)
    spark.sql(insert_query).createOrReplaceTempView(CURRENT_STAGING_VIEW)
    display(
        spark.sql(
            """
            select
                count(1),
                load_date,
                cfk_patron_id
            from
                current_staging
            group by
                all
        """
        )
    )
    display(spark.sql(EXCLUSIVE_CUMULATIVE_UNSUPERVISED_MERGE_QRY))
else:
    logger.warning(
        "No model run was generated because the input or result set was empty."
    )

# COMMAND ----------

# DBTITLE 1,Generate Historical Statistics
display(
    spark.sql(
        f"""
    insert overwrite {reporting_catalog}.gold.{ANOMALY_REPORT} 
    select
      cfk_patron_id,
      last(anomaly_score) latest_anomaly_score,
      min(create_ts) first_appeared,
      max(create_ts) last_appeared,
      count(1) times_appeared,
      count(1) / (
        select
          count(distinct load_date)
        from
          {reporting_catalog}.gold.{NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED}
      ) pct_times_appeared_of_total_runs,
      current_timestamp create_ts,
      'UNSUPERVISED_MODEL_EXECUTION_ETL' create_user
    from
      {reporting_catalog}.gold.{NON_EXCLUSIVE_CUMULATIVE_UNSUPERVISED}
    where
      is_anomaly = true
    group by
      cfk_patron_id
    """
    )
)