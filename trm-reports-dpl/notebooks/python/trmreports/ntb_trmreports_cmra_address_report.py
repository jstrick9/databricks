# Databricks notebook source
# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Shared Function
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Catalogs
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_cmra_address_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Generate CMRA Upsert Data
spark.sql(
    f"""
  create or replace temp view upsert as
  with cmra_business_data as (
    select
      a.serial_number,
      cast(try_parse_json(a.response):analysis:dpv_cmra as string) dpv_cmra
    from
      {reporting_catalog}.silver.cmra_request_status a
    where
      status = 'completed'
  )
  select
    a.serial_number,
    a.dpv_cmra cmra_status,
    iff(
      a.dpv_cmra is null, 
      "[blank] — Address was not submitted for CMRA verification.", 
      b.dpv_cmra_code_description
    ) cmra_status_description
  from
    cmra_business_data a
    left join {reporting_catalog}.silver.stnd_smarty_streets_dpv_cmra_code b 
      on a.dpv_cmra = b.dpv_cmra_code
""")

display(
  spark.sql("""
    select
      *
    from
      upsert
    limit 5;
  """)
)

# COMMAND ----------

# DBTITLE 1,Perform CMRA Upsert
display(
  spark.sql(f"""
    merge into
      {reporting_catalog}.silver.cmra_case_status `target`
    using
      upsert `source`
    on
      `target`.serial_number = `source`.serial_number
    when matched then update set
      serial_number = `source`.serial_number,
      cmra_status = `source`.cmra_status,
      cmra_status_description = `source`.cmra_status_description,
      update_ts = current_timestamp
    when not matched then insert (serial_number, cmra_status, cmra_status_description)
      values (`source`.serial_number, `source`.cmra_status, `source`.cmra_status_description)
  """)
)

display(
  spark.sql(f"""
    select
      *
    from
      {reporting_catalog}.silver.cmra_case_status
    limit 
      10
  """)
)

# COMMAND ----------

# DBTITLE 1,Generate Validation and Verification Detail Upsert
validated_addresses_detail = spark.sql(
    f"""
with base as (
  select
    serial_number,
    from_json(
      cast(
        from_json(
          cast(response as string), 'STRUCT<analysis: STRUCT<components: STRING>>'
        ).analysis.components as string
      ),
      'STRUCT<
        primary_number:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        street_name: 
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        street_suffix:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        city_name:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        state_abbreviation:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        zipcode:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >,
        plus4_code:
          STRUCT<
            status: STRING,
            change: ARRAY<STRING>
          >
      >'
    ) as components
  from
    {reporting_catalog}.silver.cmra_request_status a
  where
    status = 'completed'
)
select
  serial_number,
  components.primary_number.status primary_number_status,
  components.primary_number.`change` primary_number_changes,
  components.street_name.status street_name_status,
  components.street_name.`change` street_name_changes,
  components.street_suffix.status street_suffix_status,
  components.street_suffix.`change` street_suffix_changes,
  components.state_abbreviation.status state_abbreviation_status,
  components.state_abbreviation.`change` state_abbreviation_changes,
  components.zipcode.status zipcode_status,
  components.zipcode.`change` zipcode_changes,
  components.plus4_code.status plus4_code_status,
  components.plus4_code.`change` plus4_code_changes
from
  base
"""
)
display(validated_addresses_detail.limit(25))
validated_addresses_detail.createOrReplaceTempView("upsert")

# COMMAND ----------

# DBTITLE 1,Perform Address Validation and Verification Detail Upsert
display(
    spark.sql(
        f"""
    merge into
      {reporting_catalog}.silver.smarty_streets_address_validation_detail `target`
    using
      upsert `source`
    on
      `target`.serial_number = `source`.serial_number
    when matched then update set
      serial_number = `source`.serial_number,
      primary_number_status = `source`.primary_number_status,
      primary_number_changes = `source`.primary_number_changes,
      street_name_status = `source`.street_name_status,
      street_name_changes = `source`.street_name_changes,
      street_suffix_status = `source`.street_suffix_status,
      street_suffix_changes = `source`.street_suffix_changes,
      state_abbreviation_status = `source`.state_abbreviation_status,
      state_abbreviation_changes = `source`.state_abbreviation_changes,
      zipcode_status = `source`.zipcode_status,
      zipcode_changes = `source`.zipcode_changes,
      plus4_code_status = `source`.plus4_code_status,
      plus4_code_changes = `source`.plus4_code_changes,
      update_ts = current_timestamp
    when not matched then insert (
      serial_number,
      primary_number_status,
      primary_number_changes,
      street_name_status,
      street_name_changes,
      street_suffix_status,
      street_suffix_changes,
      state_abbreviation_status,
      state_abbreviation_changes,
      zipcode_status,
      zipcode_changes,
      plus4_code_status,
      plus4_code_changes
    )
    values (
      `source`.serial_number,
      `source`.primary_number_status,
      `source`.primary_number_changes,
      `source`.street_name_status,
      `source`.street_name_changes,
      `source`.street_suffix_status,
      `source`.street_suffix_changes,
      `source`.state_abbreviation_status,
      `source`.state_abbreviation_changes,
      `source`.zipcode_status,
      `source`.zipcode_changes,
      `source`.plus4_code_status,
      `source`.plus4_code_changes
    )
  """
    )
)

display(
    spark.sql(
        f"""
    select
      *
    from
      {reporting_catalog}.silver.smarty_streets_address_validation_detail
    limit 
      25
"""
    )
)

# COMMAND ----------

# DBTITLE 1,Generate Address Validation and Verification Upsert
validated_addresses = spark.sql(f"""
with analysis as (
  select
    serial_number,
    from_json(
      cast(response as string), 'analysis STRUCT<dpv_footnotes: STRING>'
    ).analysis.dpv_footnotes dpv_footnote,
    from_json(
      cast(response as string), 'analysis STRUCT<footnotes: STRING>'
    ).analysis.footnotes footnote,
    from_json(
      cast(response as string), 'analysis STRUCT<dpv_match_code: STRING>'
    ).analysis.dpv_match_code dpv_match_code
  from
    {reporting_catalog}.silver.cmra_request_status a
  where
    status = 'completed'
    and not exists (
      select 
        1 
      from
        {reporting_catalog}.silver.smarty_streets_address_validation b
      where
        a.serial_number = b.serial_number
        and b.dpv_match_code != 'Y'
    )
),
dpv_code_rows as (
  select
    serial_number,
    dpv_match_code,
    dpv_footnote,
    footnote,
    explode_outer(
      transform(sequence(1, length(dpv_footnote), 2), i -> substring(dpv_footnote, i, 2))
    ) dpv_footnote_code,
    explode_outer(
      transform(sequence(1, length(footnote), 2), i -> substring(footnote, i, 2))
    ) footnote_code
  from
    analysis a
),
combined_dpv_match_results as (
  select distinct
    a.serial_number,
    a.dpv_match_code,
    a.dpv_match_code || ': ' || "[" || d.dpv_match_code_description || "] "
    || d.dpv_match_code_description_verbose dpv_match_code_description
  from
    dpv_code_rows a
      inner join {reporting_catalog}.silver.stnd_smarty_streets_dpv_match_code d
        on a.dpv_match_code = d.dpv_match_code
),
combined_dpv_footnote_results as (
  select distinct
    a.serial_number,
    a.dpv_footnote full_dpv_footnote,
    array_join(
      array_distinct(collect_list(a.dpv_footnote_code || ': ' || b.dpv_footnote_code_description) over (
          partition by serial_number
        )),
      '; '
    ) combined_dpv_footnote_code_description
  from
    dpv_code_rows a
      inner join {reporting_catalog}.silver.stnd_smarty_streets_dpv_footnote_code b
        on a.dpv_footnote_code = b.dpv_footnote_code
),
combined_footnote_results as (
  select distinct
    a.serial_number,
    a.footnote full_footnote,
    array_join(
      array_distinct(collect_list(
        a.footnote_code || ': ' || "[" || c.footnote_code_description || "] "
        || c.footnote_code_description_verbose
      ) over (partition by serial_number)),
      '; '
    ) combined_footnote_code_description
  from
    dpv_code_rows a
      inner join {reporting_catalog}.silver.stnd_smarty_streets_footnote_code c
        on a.footnote_code = c.footnote_code
)
select distinct
  a.serial_number,
  b.dpv_match_code,
  iff(
    b.dpv_match_code is null,
    '[blank or null] — The address is not present in the USPS database.',
    b.dpv_match_code_description
  ) dpv_match_code_description,
  c.full_dpv_footnote,
  c.combined_dpv_footnote_code_description,
  d.full_footnote,
  d.combined_footnote_code_description
from
  dpv_code_rows a
    left join combined_dpv_match_results b
      on a.serial_number = b.serial_number
    left join combined_dpv_footnote_results c
      on a.serial_number = c.serial_number
    left join combined_footnote_results d
      on a.serial_number = d.serial_number
"""
)
display(validated_addresses.limit(25))
validated_addresses.createOrReplaceTempView("upsert")

# COMMAND ----------

# DBTITLE 1,Perform Address Validation and Verification Upsert
display(
    spark.sql(
        f"""
    merge into
      {reporting_catalog}.silver.smarty_streets_address_validation `target`
    using
      upsert `source`
    on
      `target`.serial_number = `source`.serial_number
    when matched then update set
      serial_number = `source`.serial_number,
      dpv_match_code = `source`.dpv_match_code,
      dpv_match_code_description = `source`.dpv_match_code_description,
      full_dpv_footnote = `source`.full_dpv_footnote,
      combined_dpv_footnote_code_description = `source`.combined_dpv_footnote_code_description,
      full_footnote = `source`.full_footnote,
      combined_footnote_code_description = `source`.combined_footnote_code_description,
      update_ts = current_timestamp
    when not matched then insert (
      serial_number,
      dpv_match_code,
      dpv_match_code_description,
      full_dpv_footnote,
      combined_dpv_footnote_code_description,
      full_footnote,
      combined_footnote_code_description
    )
    values (
      `source`.serial_number,
      `source`.dpv_match_code,
      `source`.dpv_match_code_description,
      `source`.full_dpv_footnote,
      `source`.combined_dpv_footnote_code_description,
      `source`.full_footnote,
      `source`.combined_footnote_code_description
    )
  """
    )
)

display(
    spark.sql(
        f"""
    select
      *
    from
      {reporting_catalog}.silver.smarty_streets_address_validation
    limit 
      25
"""
    )
)

# COMMAND ----------

# DBTITLE 1,Generate Address Confirmation and Validity
validated_and_confirmed_addresses = spark.sql(
    f"""
    select
        serial_number,
        case
            when startswith(a.full_dpv_footnote, 'AABB') then true
            else false
        end as is_fully_valid_address,
        case
            when
            startswith(a.full_dpv_footnote, 'AA')
            and not startswith(a.full_dpv_footnote, 'AABB')
            then
            true
            else false
        end as is_partially_valid_address,
        case
            when
            startswith(a.full_dpv_footnote, 'AA')
            and not startswith(a.full_dpv_footnote, 'AABB')
            then
            true
            else false
        end as is_invalid_address,
        case
            when a.dpv_match_code is null then true
            else false
        end as is_unconfirmed_address,
        case
            when a.dpv_match_code = 'Y' then true
            else false
        end as is_fully_confirmed_address,
        case
            when a.dpv_match_code in ('D', 'S') then true
            else false
        end as is_partially_confirmed_address,
        case
            when a.dpv_match_code is null then true
            else false
        end as is_not_exists_in_usps
    from
        {reporting_catalog}.silver.smarty_streets_address_validation a
""")
display(validated_and_confirmed_addresses.limit(25))
validated_and_confirmed_addresses.createOrReplaceTempView("upsert")

# COMMAND ----------

# DBTITLE 1,Perform  Address Confirmation and Validity
display(
    spark.sql(
        f"""
    merge into
      {reporting_catalog}.gold.cmra_address_validation `target`
    using
      upsert `source`
    on
      `target`.serial_number = `source`.serial_number
    when matched then update set
        serial_number = `source`.serial_number,
        is_fully_valid_address = `source`.is_fully_valid_address,
        is_partially_valid_address = `source`.is_partially_valid_address,
        is_invalid_address = `source`.is_invalid_address,
        is_unconfirmed_address = `source`.is_unconfirmed_address,
        is_fully_confirmed_address = `source`.is_fully_confirmed_address,
        is_partially_confirmed_address = `source`.is_partially_confirmed_address,
        is_not_exists_in_usps = `source`.is_not_exists_in_usps,
        update_ts = current_timestamp
    when not matched then insert (
        serial_number,
        is_fully_valid_address,
        is_partially_valid_address,
        is_invalid_address,
        is_unconfirmed_address,
        is_fully_confirmed_address,
        is_partially_confirmed_address,
        is_not_exists_in_usps
    )
    values (
        `source`.serial_number,
        `source`.is_fully_valid_address,
        `source`.is_partially_valid_address,
        `source`.is_invalid_address,
        `source`.is_unconfirmed_address,
        `source`.is_fully_confirmed_address,
        `source`.is_partially_confirmed_address,
        `source`.is_not_exists_in_usps
    )
  """
    )
)

display(
    spark.sql(
        f"""
    select
      *
    from
      {reporting_catalog}.gold.cmra_address_validation
    limit 
      10
  """
    )
)

# COMMAND ----------

# DBTITLE 1,End Job
output_count_cmra = spark.sql(
    f"""
  select
    *
  from
    {reporting_catalog}.silver.cmra_case_status
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.silver.cmra_case_status
      order by
        create_ts desc
      limit 1
    )
"""
).count()

output_count_address_validation = spark.sql(
    f"""
  select
    *
  from
    {reporting_catalog}.silver.smarty_streets_address_validation
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.silver.smarty_streets_address_validation
      order by
        create_ts desc
      limit 1
    )
"""
).count()

output_count_address_validation_detail = spark.sql(
    f"""
  select
    *
  from
    {reporting_catalog}.silver.smarty_streets_address_validation_detail
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.silver.smarty_streets_address_validation_detail
      order by
        create_ts desc
      limit 1
    )
"""
).count()

output_count_address_validation_and_confirmation = spark.sql(
    f"""
  select
    *
  from
    {reporting_catalog}.gold.cmra_address_validation
  where
    create_ts = (
      select
        create_ts
      from
        {reporting_catalog}.gold.cmra_address_validation
      order by
        create_ts desc
      limit 1
    )
"""
).count()

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    output_count_cmra + output_count_address_validation,
    "job completed successfully",
)
dbutils.notebook.exit(
    f"""
    Job completed with:
    - [{output_count_cmra}] records for the CMRA load 
    - [{output_count_address_validation}] records for the address validation load
    - [{output_count_address_validation_detail}] records for the address validation detail load
    - [{output_count_address_validation_and_confirmation}] records for the address validation and confirmation load
    """
)