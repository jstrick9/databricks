# Databricks notebook source
dbutils.widgets.removeAll()

dbutils.widgets.text(name="dbx_env", defaultValue="dev")
dbutils.widgets.text(name="historical", defaultValue="false")
dbutils.widgets.text(name="run_id", defaultValue="0")
dbutils.widgets.text(name="initial_run", defaultValue="false")
dbutils.widgets.text(name="delta_feed_date", defaultValue="")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env")
historical = dbutils.widgets.get("historical")
run_id = int(dbutils.widgets.get("run_id"))
initial_run = dbutils.widgets.get("initial_run")
delta_feed_date_param_value = dbutils.widgets.get("delta_feed_date")

config_file = f"../../config/{dbx_env}/tmngpdb-conf.yaml"

print(f"{dbx_env=}")
print(f"{historical=}")
print(f"{run_id=}")
print(f"{initial_run=}")
print(f"{config_file=}")
print(f"{delta_feed_date_param_value=}")

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# MAGIC %run ./ntb_gold_tmapplser_dtd

# COMMAND ----------

# MAGIC %run ./ntb_gold_tmapplser_functions

# COMMAND ----------

common_configs = read_yaml(config_file)

dbx_catalog = common_configs["schema"]["trgt_catalog"]
foreign_oracle_catalog = common_configs["schema"]["foreign_oracle_catalog"]
foreign_oracle_db = common_configs["schema"]["src_db_name"]
s3_path = common_configs["schema"]["s3_path"]
historical_file_batch_size = int(common_configs["schema"]["historical_file_batch_size"])
working_temp_directory = common_configs["schema"]["working_temp_directory"]

# Delta feed date is always the day before of pipeline run date,
# unless delta_feed_date parameter is passed.
delta_feed_date = (
    datetime.datetime.strptime(delta_feed_date_param_value, "%Y-%m-%d").date()
    if delta_feed_date_param_value and delta_feed_date_param_value.strip()
    else (datetime.datetime.today() - timedelta(days=1)).date()
)

print(f"{dbx_catalog=}")
print(f"{foreign_oracle_catalog=}")
print(f"{foreign_oracle_db=}")
print(f"{s3_path=}")
print(f"{historical_file_batch_size=}")
print(f"{working_temp_directory=}")
print(f"{delta_feed_date=}")

# COMMAND ----------

def daily_load(df: DataFrame, serial_numbers_df: DataFrame) -> DataFrame:
    """
    Daily load uses last-mod-dt to partition the data.
    Resulting in grouped serial-numbers with the same last-mod-dt in the same file.
    """

    # Join in order to get last-mod-ts for associated serial number.
    # last-mod-ts participate in partitioning and corrseponding file name.
    df = df.join(serial_numbers_df, on="serial-number", how="inner")
    df = (
        df.withColumn("last-mod-dt", date_format(col("last-mod-ts"), "yyMMdd"))
            .withColumn("transaction-date", date_format(col("last-mod-ts"), "yyyyMMdd")).drop("last-mod-ts")
    )

    return df 
    
def historical_load(df: DataFrame, serial_numbers_df: DataFrame, batch_size: int) -> DataFrame:
    """
    Historical load groups all serial numbers in the same file,
    regardless of the last-mod-dt.
    """

    df = df.join(serial_numbers_df, on="serial-number", how="inner")
    df = (
        df.withColumn("last-mod-timestamp", date_format(col("last-mod-ts"), "yyyyMMdd"))
            .withColumn("transaction-date", date_format(col("last-mod-ts"), "yyyyMMdd")).drop("last-mod-ts")
    )

    historical_ts = f"18840407-{(datetime.datetime.now().year - 1)}1231"

    df = df.withColumn("mid", monotonically_increasing_id())
    win_id = Window.orderBy(col("serial-number"), col("mid"))

    # Formatting batch_id to have leading 0 ex. (-01, -02, -10, -22, -84)
    df = (
        df.withColumn("row_num", row_number().over(win_id))
            .withColumn(
                "batch_id",
                format_string(
                    "%02d",
                    floor((col("row_num") - 1) / batch_size) + 1
                )
            )
            .withColumn(
                "last-mod-dt",
                concat(lit(historical_ts), lit("-"), col("batch_id"))
        )
        .drop("batch_id")
        .drop("row_num")
        .drop("last-mod-timestamp")
        .drop("mid")
    )

    return df

# COMMAND ----------

def get_dba_composed_data() -> DataFrame:
    """
    Get dba-aka-text and composed-of-statement fields from bronze layer and prepare
    dataframe for merging into case file owners data.
    """

    return spark.sql(
        f"""
        SELECT DISTINCT
            cast(split(tmpr.fk_trademark_gid, ':')[2] AS INTEGER) `serial-number`,
            ip.party_composition_tx AS `composed-of-statement`,
            ipanm.assumed_nm AS `dba-aka-text`
        FROM {foreign_oracle_catalog}.{foreign_oracle_db}.interested_party AS ip
        INNER JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.tm_party_role AS tmpr ON ip.interested_party_gid = tmpr.fk_interested_party_gid
        LEFT JOIN {foreign_oracle_catalog}.{foreign_oracle_db}.interested_party_assumed_nm AS ipanm ON ip.interested_party_gid = ipanm.fk_interested_party_gid
        WHERE tmpr.fk_trademark_gid
        IN
        (
            SELECT DISTINCT concat('Trademark:0:', sernum)
            FROM {dbx_catalog}.silver.tmappl_daily_consolidated_vw
        ) AND fk_tm_party_role_cd = 'OWNER' AND SUBSTR(party_role_sequence_no, 4, 1) IS NOT NULL
        AND (ip.party_composition_tx IS NOT NULL OR ipanm.assumed_nm IS NOT NULL)
    """
    )

# COMMAND ----------

def get_madrid_entry_number(serial_numbers_df: DataFrame) -> DataFrame:
    """
    Get entry-number from bronze layer as it needs only to look into
    required serial-numbers in order to build this field correctly as
    it is using row_number function.
    """

    serial_numbers_df.createOrReplaceTempView("tmappl_consolidated_vw")

    return spark.sql(
        f"""
        WITH history AS (
            SELECT 
                DISTINCT ia.international_us_ref_no AS mhi_ctl_num,
                date_format(cast(iae.effective_ts AS DATE), 'yyyyMMdd') AS ent_dt
            FROM {foreign_oracle_catalog}.TMINTLTM.international_appl_event iae
            LEFT JOIN {foreign_oracle_catalog}.TMINTLTM.international_appl_evnt_rsn iaer
            ON iae.international_appl_evnt_rsn_id = iaer.international_appl_evnt_rsn_id
            LEFT JOIN {foreign_oracle_catalog}.TMINTLTM.international_application ia
            ON ia.international_application_gid = iae.fk_international_appl_gid
            WHERE iaer.international_appl_evnt_rsn_cd NOT IN ('IRREP', 'IRRRJ', 'CRCRM', 'ADDCH', 'SYNC1', 'NCEDN', 'RENWL') 
            ORDER BY ent_dt
        ),

        madrid AS (
            SELECT 
                INTERNATIONAL_US_REF_NO AS mas_ctl_num,
                cast(split(ba.cfk_trademark_gid, ':')[2] AS INTEGER) AS sernum,
                row_number() OVER (ORDER BY INTERNATIONAL_US_REF_NO) AS mRow
            FROM {foreign_oracle_catalog}.TMINTLTM.base_application ba
            LEFT JOIN {foreign_oracle_catalog}.TMINTLTM.INTERNATIONAL_APPLICATION ia
            ON ia.INTERNATIONAL_APPLICATION_GID = ba.FK_INTERNATIONAL_APPL_GID
            LEFT JOIN {foreign_oracle_catalog}.TMINTLTM.BASE_APPL_INTL_REG bair
            ON bair.FK_INTERNATIONAL_APPL_GID = ba.FK_INTERNATIONAL_APPL_GID AND bair.CFK_TRADEMARK_GID = ba.CFK_TRADEMARK_GID
            LEFT OUTER JOIN {foreign_oracle_catalog}.TMINTLTM.INTERNATIONAL_REGISTRATION ir
            ON ir.INTERNATIONAL_REG_GID = bair.FK_INTERNATIONAL_REG_GID 
            LEFT OUTER JOIN {foreign_oracle_catalog}.TMINTLTM.INTERNATIONAL_TM it
            ON ir.FK_INTERNATIONAL_REG_NO  = it.INTERNATIONAL_REG_NO
            WHERE 
                cast(split(ba.cfk_trademark_gid, ':')[2] AS INTEGER)
                IN (
                        SELECT DISTINCT `serial-number`
                        FROM tmappl_consolidated_vw
                    ) 
        )

        SELECT DISTINCT
            sernum AS `serial-number`,
            mas_ctl_num AS `reference-number`,
            mRow
        FROM history, madrid
        WHERE history.mhi_ctl_num = madrid.mas_ctl_num
        """
    )

# COMMAND ----------

def get_serial_numbers(initial_run: str, historical: str, date: datetime.date) -> DataFrame:
    """
    Pull serial numbers based on date from
    consolidated view and tmappl_daily_delta table which are used to determine
    delta for given day.
    """

    serial_numbers_df = spark.read.table(f"{dbx_catalog}.silver.tmappl_daily_consolidated_vw")
    serial_numbers_df = serial_numbers_df.select(
            col("SERNUM"),
            col("LAST_MOD_TS"),
            col("PULLDT"),
            col("ACTCD")
        )

    if initial_run == "true":
        window_spec = Window.partitionBy(col("SERNUM")).orderBy(col("LAST_MOD_TS").desc())
        serial_numbers_df = serial_numbers_df.withColumn("row_number", row_number().over(window_spec))
        serial_numbers_df = (
            serial_numbers_df.filter(col("row_number") == 1)
                .select(
                    col("SERNUM").alias("serial-number"),
                    col("LAST_MOD_TS").alias("last-mod-ts"),
                    col("ACTCD").alias("action-key")
                )
        ).dropDuplicates(["serial-number"])

        print(f"Total number of records to process: {serial_numbers_df.count()}")
        return serial_numbers_df
    
    if historical == "false":
        delta_feed_start_ts = date.strftime("%Y-%m-%dT%H:%M:%S")
        delta_feed_end_ts = (date + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")

        serial_numbers_df = serial_numbers_df.filter(
            (to_date(col("PULLDT")) == date) |
            (
                (col("LAST_MOD_TS") >= delta_feed_start_ts) & (col("LAST_MOD_TS") <= delta_feed_end_ts)
            )
        )
        # Secure date is the same for all rows, because partitioning is done by this ts.
        serial_numbers_df = serial_numbers_df.withColumn("LAST_MOD_TS", lit(delta_feed_start_ts))
    else:
        # Annual files don't require filtering based on the date. Use full dataset.
        trademark_df = (
            spark.read.table(f"{dbx_catalog}.bronze.trademark")
                .select(
                    col("serial_num_tx").alias("SERNUM"),
                    col("last_mod_ts").alias("LAST_MOD_TS")
                )
        )

        serial_numbers_df = trademark_df
        serial_numbers_df = serial_numbers_df.withColumn("ACTCD", lit("TX"))

    # Deduplicate serial numbers
    window_spec = Window.partitionBy(col("SERNUM")).orderBy(col("LAST_MOD_TS").desc())
    serial_numbers_df = serial_numbers_df.withColumn("row_number", row_number().over(window_spec))
    serial_numbers_df = (
        serial_numbers_df.filter(col("row_number") == 1)
            .select(
                col("SERNUM").alias("serial-number"),
                col("LAST_MOD_TS").alias("last-mod-ts"),
                col("ACTCD").alias("action-key")
            )
    ).dropDuplicates(["serial-number"])

    print(f"Total number of records to process: {serial_numbers_df.count()}")
    return serial_numbers_df

# COMMAND ----------

tmappl_serial_numbers_df = get_serial_numbers(initial_run=initial_run, historical=historical, date=delta_feed_date)

if tmappl_serial_numbers_df.isEmpty():
    dbutils.notebook.exit(f"No delta data for {delta_feed_date}.")

tmappl_serial_numbers_df.display()

# COMMAND ----------

case_file_header_df = spark.read.table(f"{dbx_catalog}.silver.bdss_case_file_data_daily_stg")
case_file_header_df = case_file_header_df.withColumnRenamed("AM_SER_NUM", "serial-number")
if historical == "false":
    case_file_header_df = case_file_header_df.join(tmappl_serial_numbers_df.select("serial-number"), on="serial-number", how="inner")
    print(f"Total # of case-file-headers: {case_file_header_df.count()}")

case_file_owner_df = spark.read.table(f"{dbx_catalog}.silver.bdss_owners_data_daily_stg")
foreign_application_df = spark.read.table(f"{dbx_catalog}.silver.bdss_foreign_apps_daily_stg").drop("create_ts").drop("create_user_id")
madrid_history_df = spark.read.table(f"{dbx_catalog}.silver.bdss_madrid_and_history_data_daily_stg")
correspondent_df = spark.read.table(f"{dbx_catalog}.silver.bdss_correspondent_data_daily_stg")
design_search_df = spark.read.table(f"{dbx_catalog}.silver.bdss_designs_daily_stg").drop("create_ts").drop("create_user_id")
case_file_event_statement_df = spark.read.table(f"{dbx_catalog}.silver.bdss_case_events_daily_stg").drop("create_ts").drop("create_user_id")
case_file_statement_df = spark.read.table(f"{dbx_catalog}.silver.bdss_vt_text_data_daily_stg")
classification_df = spark.read.table(f"{dbx_catalog}.silver.bdss_class").drop("create_ts").drop("create_user_id")

name_change_df = spark.read.table(f"{dbx_catalog}.silver.bdss_name_change_daily_stg")
prior_registration_application_df = spark.read.table(f"{dbx_catalog}.silver.bdss_prior_regs_daily_stg").drop("create_ts").drop("create_user_id")

# COMMAND ----------

case_file_header_df = remove_invalid_xml_characters(df=case_file_header_df)
case_file_owner_df = remove_invalid_xml_characters(df=case_file_owner_df)
foreign_application_df = remove_invalid_xml_characters(df=foreign_application_df)
madrid_history_df = remove_invalid_xml_characters(df=madrid_history_df)
correspondent_df = remove_invalid_xml_characters(df=correspondent_df)
design_search_df = remove_invalid_xml_characters(df=design_search_df)
case_file_event_statement_df = remove_invalid_xml_characters(df=case_file_event_statement_df)
case_file_statement_df = remove_invalid_xml_characters(df=case_file_statement_df)
classification_df = remove_invalid_xml_characters(df=classification_df)
name_change_df = remove_invalid_xml_characters(df=name_change_df)
prior_registration_application_df = remove_invalid_xml_characters(df=prior_registration_application_df)

# COMMAND ----------

case_file_header_df = get_attorney_name(name_change_df=name_change_df, case_file_header_df=case_file_header_df)
case_file_header_df = get_domestic_representative_name(name_change_df=name_change_df, case_file_header_df=case_file_header_df)
case_file_header_df = replace_zero_date_with_null(df=case_file_header_df, columns=["DT_FIL", "DT_REG", "DT_STAT", "DT_PUB", "DT_AMND_REG", "DT_ABAN", "DT_CNCL", "DT_PUB_12_C", "DT_IN_LOC", "DT_RNWL"])
case_file_header_df = replace_empty_string_with_null(df=case_file_header_df)

case_file_header_xml_df = case_file_header_df.select(
    col("serial-number"),
    col("REG_NUM").alias("registration-number"),
    struct([
        col("DT_FIL").alias("filing-date"),
        col("DT_REG").alias("registration-date"),
        col("AM_STAT").alias("status-code"),
        col("DT_STAT").alias("status-date"),
        col("MARK_1_LIN").alias("mark-identification"),
        col("AM_MARK_DWG_CD").alias("mark-drawing-code"),
        col("DT_PUB").alias("published-for-opposition-date"),
        col("DT_AMND_REG").alias("amend-to-register-date"),
        col("DT_ABAN").alias("abandonment-date"),
        col("CNCL_CD").alias("cancellation-code"),
        col("DT_CNCL").alias("cancellation-date"),
        col("DT_PUB_12_C").alias("republished-12c-date"),
        col("domestic-representative-name"),
        col("ATTY_DKT_NUM").alias("attorney-docket-number"),
        col("attorney-name"),
        col("FLG_AMND_PRIN").alias("principal-register-amended-in"),
        col("FLG_AMND_SUPL").alias("supplemental-register-amended-in"),
        col("FLG_TM").alias("trademark-in"),
        col("FLG_COLL_TM").alias("collective-trademark-in"),
        col("FLG_SM").alias("service-mark-in"),
        col("FLG_COLL_SM").alias("collective-service-mark-in"),
        col("FLG_COLL_MM").alias("collective-membership-mark-in"),
        col("FLG_CM").alias("certification-mark-in"),
        col("FLG_CNCL_PEND").alias("cancellation-pending-in"),
        col("FLG_PUB_CNCR").alias("published-concurrent-in"),
        col("FLG_CNCR").alias("concurrent-use-in"),
        col("FLG_CNCR_PEND").alias("concurrent-use-proceeding-in"),
        col("FLG_INTF_PEND").alias("interference-pending-in"),
        col("FLG_OPPS_PEND").alias("opposition-pending-in"),
        col("FLG_RPB_SCT_12").alias("section-12c-in"),
        col("FLG_SCT_2F").alias("section-2f-in"),
        col("FLG_SCT_2F_PT").alias("section-2f-in-part-in"),
        col("FLG_RNWL_FIL").alias("renewal-filed-in"),
        col("FLG_SCT_8_FIL").alias("section-8-filed-in"),
        col("FLG_SCT_8_P_A").alias("section-8-partial-accept-in"),
        col("FLG_SCT_8_ACPT").alias("section-8-accepted-in"),
        col("FLG_SCT_15_ACK").alias("section-15-acknowledged-in"),
        col("FLG_SCT_15_FIL").alias("section-15-filed-in"),
        col("FLG_SUPL_REG").alias("supplemental-register-in"),
        col("FLG_FRPR_CLMD").alias("foreign-priority-in"),
        col("FLG_CHNG_REG").alias("change-registration-in"),
        col("FLG_ITU_FIL").alias("intent-to-use-in"),
        col("FLG_ITU_CUR").alias("intent-to-use-current-in"),
        col("FLG_USE_FIL").alias("filed-as-use-application-in"),
        col("FLG_USE_AMED").alias("amended-to-use-application-in"),
        col("FLG_USE_CUR").alias("use-application-currently-in"),
        col("FLG_ITU_AMED").alias("amended-to-itu-application-in"),
        col("FLG_44D_FIL").alias("filing-basis-filed-as-44d-in"),
        col("FLG_44D_AMED").alias("amended-to-44d-application-in"),
        col("FLG_44D_CUR").alias("filing-basis-current-44d-in"),
        col("FLG_44E_FIL").alias("filing-basis-filed-as-44e-in"),
        col("FLG_44E_CUR").alias("filing-basis-current-44e-in"),
        col("FLG_44E_AMED").alias("amended-to-44e-application-in"),
        col("FLG_NO_BAS_CUR").alias("without-basis-currently-in"),
        col("FLG_NO_BAS_FIL").alias("filing-current-no-basis-in"),
        col("FLG_C_DRW_FIL").alias("color-drawing-filed-in"),
        col("FLG_C_DRW_CUR").alias("color-drawing-current-in"),
        col("FLG_3D_DRW_FIL").alias("drawing-3d-filed-in"),
        col("FLG_3D_DRW_CUR").alias("drawing-3d-current-in"),
        col("FLG_STD_CHAR").alias("standard-characters-claimed-in"),
        col("FLG_66A_FIL").alias("filing-basis-filed-as-66a-in"),
        col("FLG_66A_CUR").alias("filing-basis-current-66a-in"),
        col("DT_RNWL").alias("renewal-date"),
        col("LO_ASGN").alias("law-office-assigned-location-code"),
        col("CURR_LOC").alias("current-location"),
        col("DT_IN_LOC").alias("location-date"),
        col("EMPE_NAM").alias("employee-name")
    ]).alias("case-file-header")
)

# COMMAND ----------

case_file_statement_df = case_file_statement_df.dropDuplicates(["sernum", "vt_text_type", "vt_text"])
case_file_statement_df = case_file_statement_df.withColumn("vt_text", rtrim(col("vt_text")))
case_file_statement_xml_df = case_file_statement_df.select(
    col("sernum").alias("serial-number"),
    struct([
        col("vt_text_type").alias("type-code"),
        col("vt_text").alias("text")
    ]).alias("case-file-statement")
)

case_file_statement_xml_df = case_file_statement_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("case-file-statement").alias("case-file-statement"))

case_file_statement_xml_df = case_file_statement_xml_df.withColumn(
    "case-file-statements",
    struct([
        col("case-file-statement")
    ])
).select(
    col("serial-number"),
    col("case-file-statements")
)

# COMMAND ----------

case_file_event_statement_df = case_file_event_statement_df.dropDuplicates([
    "CM_SER_NUM",
    "CM_ENT_CD",
    "CM_ENT_TYPE",
    "tt_text_1",
    "ent_dt",
    "CM_ENT_NUM"
])
case_file_event_statement_df = replace_empty_string_with_null(df=case_file_event_statement_df)

case_file_event_statement_xml_df = case_file_event_statement_df.select(
    col("CM_SER_NUM").alias("serial-number"),
    struct([
        col("CM_ENT_CD").alias("code"),
        col("CM_ENT_TYPE").alias("type"),
        col("tt_text_1").alias("description-text"),
        col("ent_dt").alias("date"),
        col("CM_ENT_NUM").alias("number")
    ]).alias("case-file-event-statement")
)

case_file_event_statement_xml_df = case_file_event_statement_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("case-file-event-statement").alias("case-file-event-statement"))

case_file_event_statement_xml_df = case_file_event_statement_xml_df.withColumn(
    "case-file-event-statements",
    struct([
        col("case-file-event-statement")
    ])
).select(
    col("serial-number"),
    col("case-file-event-statements")
)

case_file_event_statement_xml_df = sort_struct_field_ascending(
    df=case_file_event_statement_xml_df,
    base_struct_field="case-file-event-statements",
    nested_struct_field="case-file-event-statement",
    ascending_field="number"
)

# COMMAND ----------

case_file_registration_flag_df = case_file_header_df.select(
    col("serial-number"),
    col("FLG_AND_OTH_CD").alias("other_related_in")
)
prior_registration_application_df = prior_registration_application_df.withColumnRenamed("sernum", "serial-number")
prior_registration_application_df = prior_registration_application_df.join(case_file_registration_flag_df, on="serial-number", how="inner")

prior_registration_application_xml_df = prior_registration_application_df.select(
    col("serial-number"),
    col("other_related_in").alias("other-related-in"),
    struct([
        col("pr_rcd_type").alias("relationship-type"),
        col("pr_rel_id_num").alias("number")
    ]).alias("prior-registration-application")
)

prior_registration_application_xml_df = prior_registration_application_xml_df.groupBy(
    col("serial-number"),
    col("other-related-in")
).agg(collect_set("prior-registration-application").alias("prior-registration-application"))

prior_registration_application_xml_df = prior_registration_application_xml_df.withColumn(
    "prior-registration-applications",
    struct([
        col("other-related-in"),
        col("prior-registration-application")
    ])
).select(
    col("serial-number"),
    col("prior-registration-applications")
)

prior_registration_application_xml_df = sort_struct_field_ascending(
    df=prior_registration_application_xml_df,
    base_struct_field="prior-registration-applications",
    nested_struct_field="prior-registration-application",
    ascending_field="number"
)

# COMMAND ----------

foreign_application_df = replace_empty_string_with_null(df=foreign_application_df)
foreign_application_df = foreign_application_df.withColumn(
    "registration-number",
    explode_outer(
        split(col("frgn_reg_num"), ",")
    )
)

foreign_application_df = (
    foreign_application_df.withColumn(
        "country",
        when(
            (col("country").isNotNull()) &
            (length(col("country")) == 3) &
            (col("country").endswith("X")),
           substring(col("country"), 1, 2)
        ).otherwise(col("country"))
    )
)

foreign_application_xml_df = foreign_application_df.select(
    col("fn_ser_num").alias("serial-number"),
    struct([
        col("dt_frgn_fil").alias("filing-date"),
        col("dt_frgn_reg").alias("registration-date"),
        col("dt_frgn_exp").alias("registration-expiration-date"),
        col("dt_rnwl_reg").alias("registration-renewal-date"),
        col("dt_rnwl_exp").alias("registration-renewal-expiration-date"),
        col("fn_ent_num").alias("entry-number"),
        col("frgn_appl_num").alias("application-number"),
        col("country"),
        col("other"),
        col("registration-number"),
        col("rnwl_reg_num").alias("renewal-number"),
        col("flg_frpr_clmd").alias("foreign-priority-claim-in")
    ]).alias("foreign-application")
)

foreign_application_xml_df = foreign_application_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("foreign-application").alias("foreign-application"))

foreign_application_xml_df = foreign_application_xml_df.withColumn(
    "foreign-applications",
    struct([
        col("foreign-application")
    ])
).select(
    col("serial-number"),
    col("foreign-applications")
)

# COMMAND ----------

classification_df = replace_zero_date_with_null(df=classification_df, columns=["dt_1_use", "dt_1_use_comm"])
classification_df = classification_df.withColumn("us_code", split_us_or_international_code_udf("cls_us"))
classification_df = classification_df.withColumn("international_code", split_us_or_international_code_udf("cls_intl"))

classification_xml_df = classification_df.select(
    col("cl_ser_num").alias("serial-number"),
    struct([
        col("cl_cls_intl_ct").alias("international-code-total-no"),
        col("cl_cls_us_ct").alias("us-code-total-no"),
        col("international_code").alias("international-code"),
        col("us_code").alias("us-code"),
        col("cls_stat").alias("status-code"),
        col("dt_stat").alias("status-date"),
        col("dt_1_use").alias("first-use-anywhere-date"),
        col("dt_1_use_comm").alias("first-use-in-commerce-date"),
        col("prime_cls").alias("primary-code")
    ]).alias("classification")
)

classification_xml_df = classification_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("classification").alias("classification"))

classification_xml_df = classification_xml_df.withColumn(
    "classifications",
    struct([
        col("classification")
    ])
).select(
    col("serial-number"),
    col("classifications")
)

classification_xml_df = sort_struct_field_ascending(
    df=classification_xml_df,
    base_struct_field="classifications",
    nested_struct_field="classification",
    ascending_field="primary-code"
)

# COMMAND ----------

correspondent_df = replace_empty_string_with_null(df=correspondent_df)
correspondent_df = correspondent_df.dropDuplicates(["sernum"])

corrsepondent_xml_df = correspondent_df.select(
    col("sernum").alias("serial-number"),
    struct([
        col("address1").alias("address-1"),
        col("address2").alias("address-2"),
        col("address3").alias("address-3"),
        col("address4").alias("address-4"),
        col("address5").alias("address-5")
    ]).alias("correspondent")
)

# COMMAND ----------

case_file_owner_df = replace_empty_string_with_null(df=case_file_owner_df)
case_file_owner_df = case_file_owner_df.withColumnRenamed("sernum", "serial-number")
case_file_owner_df = case_file_owner_df.dropDuplicates(["serial-number", "PY_ENT_NUM", "PARTY_TYPE"])

name_change_df = get_name_change_data(df=name_change_df)
name_change_df = replace_empty_string_with_null(df=name_change_df)
case_file_owner_df = case_file_owner_df.join(name_change_df, on=["serial-number", "PARTY_TYPE"], how="left")

case_file_owner_dba_composed_df = get_dba_composed_data()
case_file_owner_dba_composed_df = replace_empty_string_with_null(df=case_file_owner_dba_composed_df)
case_file_owner_df = case_file_owner_df.join(case_file_owner_dba_composed_df, on="serial-number", how="left")
case_file_owner_df = (
    case_file_owner_df.withColumn("dba-aka-text", when(col("PY_FLG_DBA_AKA") == 1, col("dba-aka-text")).otherwise(None))
        .withColumn("composed-of-statement", when(col("PY_FLG_CMP_STMT") == 1, col("composed-of-statement")).otherwise(None))
)

# This process state, country and other fields on both case-file-owner and nationality level
case_file_owner_df = process_owners_nationality_data(df=case_file_owner_df)
case_file_owner_df = replace_null_with_empty_string(df=case_file_owner_df, columns=["name-change-explanation"])

case_file_owner_xml_df = case_file_owner_df.select(
    col("serial-number"),
    struct([
        col("PY_ENT_NUM").alias("entry-number"),
        col("PARTY_TYPE").alias("party-type"),
        struct([
            col("nationality_state").alias("state"),
            col("nationality_country").alias("country"),
            col("nationality_other").alias("other")
        ]).alias("nationality"),
        col("ENTITY_TYPE").alias("legal-entity-type-code"),
        col("NAM").alias("party-name"),
        col("ADDR_1").alias("address-1"),
        col("ADDR_2").alias("address-2"),
        col("CITY").alias("city"),
        col("owner_state").alias("state"),
        col("owner_country").alias("country"),
        col("owner_other").alias("other"),
        col("ZIP_CD").alias("postcode"),
        col("dba-aka-text"),
        col("composed-of-statement"),
        col("name-change-explanation")
    ]).alias("case-file-owner")
)

case_file_owner_xml_df = case_file_owner_xml_df.groupBy(
    col("serial-number")
).agg(collect_list("case-file-owner").alias("case-file-owner"))

case_file_owner_xml_df = case_file_owner_xml_df.withColumn(
    "case-file-owners",
    struct([
        col("case-file-owner")
    ])
).select(
    col("serial-number"),
    col("case-file-owners")
)

case_file_owner_xml_df = sort_struct_field(
    df=case_file_owner_xml_df,
    base_struct_field="case-file-owners",
    nested_struct_field="case-file-owner",
    ascending_field="entry-number",
    descending_field="party-type"
)

# COMMAND ----------

design_search_xml_df = design_search_df.select(
    col("sernum").alias("serial-number"),
    struct([
        col("wp_wipo_cd").alias("code")
    ]).alias("design-search")
)

design_search_xml_df = design_search_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("design-search").alias("design-search"))

design_search_xml_df = design_search_xml_df.withColumn(
    "design-searches",
    struct([
        col("design-search")
    ])
).select(col("serial-number"), col("design-searches"))

design_search_xml_df = sort_struct_field_ascending(
    df=design_search_xml_df,
    base_struct_field="design-searches",
    nested_struct_field="design-search",
    ascending_field="code"
)

# COMMAND ----------

international_registration_df = case_file_header_df.filter(col("RI_SER_NUM").isNotNull())
international_registration_df = replace_zero_date_with_null(df=international_registration_df, columns=["INTL_REG_DT", "IB_PUB_DT", "RNWL_DT", "AUTO_PROTEC_DT", "DEATH_DT", "STAT_DT", "PRIOR_CLMD_DT"])
international_registration_df = replace_null_with_empty_string(df=international_registration_df, columns=["RI_INTL_REG_NUM", "INTL_REG_DT", "IB_PUB_DT", "RNWL_DT", "AUTO_PROTEC_DT", "STAT", "STAT_DT", "FLG_PRIOR_CLMD", "FLG_1ST_REF"])

international_registration_xml_df = international_registration_df.select(
    col("RI_SER_NUM").alias("serial-number"),
    struct([
        col("RI_INTL_REG_NUM").alias("international-registration-number"),
        col("INTL_REG_DT").alias("international-registration-date"),
        col("IB_PUB_DT").alias("international-publication-date"),
        col("RNWL_DT").alias("international-renewal-date"),
        col("AUTO_PROTEC_DT").alias("auto-protection-date"),
        col("DEATH_DT").alias("international-death-date"),
        col("STAT").alias("international-status-code"),
        col("STAT_DT").alias("international-status-date"),
        col("FLG_PRIOR_CLMD").alias("priority-claimed-in"),
        col("PRIOR_CLMD_DT").alias("priority-claimed-date"),
        col("FLG_1ST_REF").alias("first-refusal-in")
    ]).alias("international-registration")
)

# COMMAND ----------

madrid_history_df = replace_zero_date_with_null(df=madrid_history_df, columns=["orig_fil_dt", "stat_dt", "reply_by_dt", "rnwl_dt", "intl_reg_dt", "ent_dt"])
madrid_history_df = madrid_history_df.drop("mRow")
madrid_history_df = (
    madrid_history_df
        .withColumnRenamed("sernum", "serial-number")
        .withColumnRenamed("mhi_ctl_num", "reference-number")
)
madrid_entry_number_df = get_madrid_entry_number(serial_numbers_df=tmappl_serial_numbers_df)
madrid_history_df = madrid_history_df.join(madrid_entry_number_df, on=["serial-number", "reference-number"], how="inner")
madrid_history_df = replace_empty_string_with_null(df=madrid_history_df)
madrid_history_df = madrid_history_df = replace_null_with_empty_string(df=madrid_history_df, columns=["mhi_action", "ent_dt", "tt_text_1", "hRow"])

madrid_history_event_xml_df = madrid_history_df.select(
    col("serial-number"),
    col("mRow"),
    struct([
        col("mhi_action").alias("code"),
        col("ent_dt").alias("date"),
        col("tt_text_1").alias("description-text"),
        col("hRow").alias("entry-number")
    ]).alias("madrid-history-event")
)

madrid_history_event_xml_df = madrid_history_event_xml_df.groupBy(
    col("serial-number"),
    col("mRow")
).agg(collect_set("madrid-history-event").alias("madrid-history-event"))

madrid_history_event_xml_df = madrid_history_event_xml_df.withColumn(
    "madrid-history-events",
    struct([
        col("madrid-history-event")
    ])
).select(
    col("serial-number"),
    col("mRow"),
    col("madrid-history-events")
)

madrid_history_event_xml_df = sort_struct_field_ascending(
    df=madrid_history_event_xml_df,
    base_struct_field="madrid-history-events",
    nested_struct_field="madrid-history-event",
    ascending_field="entry-number"
)

madrid_filing_record_xml_df = madrid_history_df.join(madrid_history_event_xml_df, on=["serial-number", "mRow"], how="inner")
madrid_filing_record_xml_df = replace_empty_string_with_null(df=madrid_filing_record_xml_df)
madrid_history_df = madrid_history_df = replace_null_with_empty_string(df=madrid_history_df, columns=["mRow", "mas_ctl_num", "orig_fil_dt"])

madrid_filing_record_xml_df = madrid_filing_record_xml_df.select(
    col("serial-number"),
    struct([
        col("mRow").alias("entry-number"),
        col("mas_ctl_num").alias("reference-number"),
        col("orig_fil_dt").alias("original-filing-date-uspto"),
        col("intl_reg_num").alias("international-registration-number"),
        col("intl_reg_dt").alias("international-registration-date"),
        col("stat").alias("international-status-code"),
        col("stat_dt").alias("international-status-date"),
        col("reply_by_dt").alias("irregularity-reply-by-date"),
        col("rnwl_dt").alias("international-renewal-date"),
        col("madrid-history-events")
    ]).alias("madrid-international-filing-record")
)

madrid_filing_record_xml_df = madrid_filing_record_xml_df.groupBy(
    col("serial-number")
).agg(collect_set("madrid-international-filing-record").alias("madrid-international-filing-record"))

madrid_filing_record_xml_df = madrid_filing_record_xml_df.withColumn(
    "madrid-international-filing-requests",
    struct([
        col("madrid-international-filing-record")
    ])
).select(
    col("serial-number"),
    col("madrid-international-filing-requests")
)

# COMMAND ----------

final_xml_df = (
    case_file_header_xml_df.join(case_file_statement_xml_df, on="serial-number", how="left")
    .join(case_file_event_statement_xml_df, on="serial-number", how="left")
    .join(prior_registration_application_xml_df, on="serial-number", how="left")
    .join(foreign_application_xml_df, on="serial-number", how="left")
    .join(classification_xml_df, on="serial-number", how="left")
    .join(corrsepondent_xml_df, on="serial-number", how="left")
    .join(case_file_owner_xml_df, on="serial-number", how="left")
    .join(design_search_xml_df, on="serial-number", how="left")
    .join(international_registration_xml_df, on="serial-number", how="left")
    .join(madrid_filing_record_xml_df, on="serial-number", how="left")
)

if historical == "false":
    final_xml_df = daily_load(df=final_xml_df, serial_numbers_df=tmappl_serial_numbers_df)
else:
    final_xml_df = historical_load(df=final_xml_df, serial_numbers_df=tmappl_serial_numbers_df, batch_size=historical_file_batch_size)

final_xml_df = final_xml_df.select(
    col("serial-number"),
    col("registration-number"),
    col("transaction-date"),
    col("case-file-header"),
    col("case-file-statements"),
    col("case-file-event-statements"),
    col("prior-registration-applications"),
    col("foreign-applications"),
    col("classifications"),
    col("correspondent"),
    col("case-file-owners"),
    col("design-searches"),
    col("international-registration"),
    col("madrid-international-filing-requests"),
    col("last-mod-dt"),
    col("action-key")
).orderBy(col("last-mod-dt"), col("action-key"), col("serial-number"))
    
final_xml_df.display()

# COMMAND ----------

if historical == "false":
    gold_df = final_xml_df.select(
        col("serial-number").alias("serial_number"),
        struct(
            [
                col("serial-number"),
                col("registration-number"),
                col("transaction-date"),
                col("case-file-header"),
                col("case-file-statements"),
                col("case-file-event-statements"),
                col("prior-registration-applications"),
                col("foreign-applications"),
                col("classifications"),
                col("correspondent"),
                col("case-file-owners"),
                col("design-searches"),
                col("international-registration"),
                col("madrid-international-filing-requests")
            ]
        ).alias("case_file"),
        col("last-mod-dt")
    )

    gold_df = (
        gold_df.withColumn(
            "xml_file_name", concat(lit("apc"), col("last-mod-dt"), lit(".xml"))
        )
        .withColumn(
            "zip_file_path",
            concat(lit(s3_path), lit("/apc"), col("last-mod-dt"), lit(".zip"))
        )
        .withColumn("create_ts", lit(get_current_datetime()).cast(TimestampType()))
        .withColumn("create_user_id", lit("etl"))
        .withColumn("run_id", lit(run_id).cast(LongType()))
        .withColumn(
            "delta_feed_date",
            to_date(col("last-mod-dt"), "yyMMdd").cast(DateType())
        )
        .drop("last-mod-dt")
    )

    print(f"Records to push to tmapplser table: {gold_df.count()}")

    if spark.catalog.tableExists(f"{dbx_catalog}.gold.tmapplser"):
        tmapplser_gold_dt = DeltaTable.forName(spark, f"{dbx_catalog}.gold.tmapplser")
        columns_to_update = [col for col in gold_df.columns if col not in ["serial_number"]]

        tmapplser_gold_dt.alias("trgt").merge(
            gold_df.alias("src"), "trgt.serial_number = src.serial_number"
        ).whenMatchedUpdate(
            set={col: f"src.{col}" for col in columns_to_update}
        ).whenNotMatchedInsert(
            values={col: f"src.{col}" for col in gold_df.columns}
        ).execute()
    else:
        gold_df.write.mode("append").option("mergeSchema", True).saveAsTable(f"{dbx_catalog}.gold.tmapplser")

# COMMAND ----------

run_index_df = final_xml_df.select(
    col("serial-number").alias("serial_number"),
    col("last-mod-dt")
)

run_index_df = (
    run_index_df.withColumn("xml_file_name", concat(lit("apc"), col("last-mod-dt"), lit(".xml")))
    .withColumn("create_ts", lit(get_current_datetime()).cast(TimestampType()))
    .withColumn(
        "run_execution_date", to_date(col("create_ts"), "yyyy-MM-dd")
    )
    .withColumn("run_id", lit(run_id).cast(LongType()))
    .withColumn(
        "delta_feed_date",
        to_date(col("last-mod-dt"), "yyMMdd").cast(DateType())
    )
    .withColumn("create_user_id", lit("etl"))
    .drop("last-mod-dt")
)

print(f"Records to push to tmapplser_run_index table: {run_index_df.count()}")
run_index_df.write.mode("append").option("mergeSchema", True).saveAsTable(f"{dbx_catalog}.gold.tmapplser_run_index")

# COMMAND ----------

ctrl_index_df = final_xml_df.select(col("last-mod-dt"))

ctrl_index_df = (
    ctrl_index_df.withColumn("xml_file_name", concat(lit("apc"), col("last-mod-dt"), lit(".xml")))
        .withColumn(
            "zip_file_path",
            concat(lit(s3_path), lit("/apc"), col("last-mod-dt"), lit(".zip"))
        )
        .withColumn("run_id", lit(run_id).cast(LongType()))
        .withColumn("create_ts", lit(get_current_datetime()).cast(TimestampType()))
        .withColumn("create_user_id", lit("etl"))
        .drop("last-mod-dt")
)

ctrl_index_df = (
    ctrl_index_df.select(
        col("xml_file_name"),
        col("zip_file_path"),
        col("run_id"),
        col("create_ts"),
        col("create_user_id"),
    )
    .groupBy(
        col("xml_file_name"),
        col("zip_file_path"),
        col("run_id"),
        col("create_ts"),
        col("create_user_id"),
    )
    .count()
)

ctrl_index_df = ctrl_index_df.withColumnRenamed("count", "record_count")

print(f"Records to push to tmapplser_ctrl_index table: {ctrl_index_df.count()}")
ctrl_index_df.write.mode("append").option("mergeSchema", True).saveAsTable(f"{dbx_catalog}.gold.tmapplser_ctrl_index")

# COMMAND ----------

final_xml_df.write.partitionBy("last-mod-dt", "action-key").options(
    rootTag="action-keys", rowTag="case-file", encoding="UTF-8"
).mode("overwrite").xml(working_temp_directory)

# COMMAND ----------

# Iterate throughout first partition (last-mod-dt)
for last_mod_dt_dir in dbutils.fs.ls(working_temp_directory):
    if not last_mod_dt_dir.isDir():
        continue

    last_mod_dt = extract_info_from_partition(root_path=last_mod_dt_dir.name.replace("/", ""))
    creation_datetime = get_creation_datetime(creation_date=last_mod_dt, historical=historical)

    new_xml_file_path = os.path.join(last_mod_dt_dir.path, f"apc{last_mod_dt}.xml").replace("dbfs:", "/dbfs")
    new_zip_file_path = os.path.join(last_mod_dt_dir.path, f"apc{last_mod_dt}.zip").replace("dbfs:", "/dbfs")

    print(f"Processing apc{last_mod_dt}.zip file...")
    # Create new XML file
    with open(new_xml_file_path, "w", encoding="utf-8") as f:
        f.write(tmapplser_dtd)

        f.write("<trademark-applications-daily>\n")
        f.write("   <version>\n")
        f.write("       <version-no>2.0</version-no>\n")
        f.write("       <version-date>20041108</version-date>\n")
        f.write("   </version>\n")
        f.write(f"  <creation-datetime>{creation_datetime}</creation-datetime>\n")
        f.write("   <application-information>\n")
        f.write("       <file-segments>\n")
        f.write("           <file-segment>TRMK</file-segment>\n")

        # Iterate throughout second partition (action-key)
        for action_key_dir in dbutils.fs.ls(last_mod_dt_dir.path):
            if not action_key_dir.isDir():
                continue

            action_key = extract_info_from_partition(root_path=action_key_dir.name.replace("/", ""))
            f.write("           <action-keys>\n")
            f.write(f"                <action-key>{action_key}</action-key>\n")

            # Iterate throughout part files
            for file in dbutils.fs.ls(action_key_dir.path):        
                if not file.name.endswith(".xml"):
                    continue
            
                content = get_part_file_content(xml_file_path=file.path.replace("dbfs:", "/dbfs"))

                # Append content to new xml file
                f.write(content)
            f.write("            </action-keys>\n")

            
        # Close tags
        f.write("       </file-segments>\n")
        f.write("   </application-information>\n")
        f.write("</trademark-applications-daily>")

    zip_success = zip_file(file_path=new_xml_file_path)
    if zip_success == False:
        raise Exception(f"Failed to zip file")

    s3_success = upload_to_s3(source_path=new_zip_file_path)
    if s3_success == False:
        raise Exception(f"Failed to upload to s3")

# COMMAND ----------

dbutils.fs.rm(working_temp_directory, True)
