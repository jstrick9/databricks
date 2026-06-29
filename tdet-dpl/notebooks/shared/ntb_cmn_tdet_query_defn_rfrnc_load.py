# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env").rstrip()

config_file_name = "tdet-conf.yaml"
config_file = f"../config/{dbx_env}/{config_file_name}"
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

configs = read_yaml(config_file)
data_quality_catalog = configs["schema"]["data_quality_catalog"]
tmngpdb_catalog = configs["schema"]["source_tmngpdb_catalog"]
src_sys_name = "TDET_SEARCH"

spark.conf.set("config.data_quality_catalog", data_quality_catalog)
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
spark.conf.set("config.config_file_name", config_file_name)
spark.conf.set("config.src_sys_name", src_sys_name)

print(f"{data_quality_catalog=} {src_sys_name=} {tmngpdb_catalog=}")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_dq_vrfctn_query_rfrnc
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   TABLE ${config.data_quality_catalog}.silver.cmn_dq_vrfctn_query_rfrnc (
# MAGIC     QUERY_NAME,
# MAGIC     QUERY_DESC,
# MAGIC     CNCTN_DTL_DESC,
# MAGIC     QUERY_TEXT,
# MAGIC     SRC_SYS_NAME
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_NON_OWNER_PR',
# MAGIC     'Target count query for ensuring only one distinct non-owner party role exists simultaneously',
# MAGIC     'DELTA_LAKE',
# MAGIC     "select
# MAGIC   count(*) as QRY_CNT
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       fk_trademark_gid,
# MAGIC       latest,
# MAGIC       count(fk_tm_party_role_cd) as cnt_rl_cd
# MAGIC     from
# MAGIC       (
# MAGIC         select
# MAGIC           *,
# MAGIC           case
# MAGIC             when party_type = max(party_type) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd
# MAGIC             ) then 1
# MAGIC             else 0
# MAGIC           end as latest,
# MAGIC           collect_set(interested_party_nm) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type
# MAGIC           ) as current_interested_party_nm,
# MAGIC           first_value(
# MAGIC             lag(
# MAGIC               collect_set(interested_party_nm) over (
# MAGIC                 partition by fk_trademark_gid,
# MAGIC                 fk_tm_party_role_cd,
# MAGIC                 party_type
# MAGIC               )
# MAGIC             ) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd
# MAGIC               order by
# MAGIC                 party_type
# MAGIC             )
# MAGIC           ) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type
# MAGIC             order by
# MAGIC               joint_party_num
# MAGIC           ) as previous_interested_party_nm,
# MAGIC           collect_set(tm_party_role_id) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type
# MAGIC           ) as current_party_ids,
# MAGIC           first_value(
# MAGIC             lag(
# MAGIC               collect_set(tm_party_role_id) over (
# MAGIC                 partition by fk_trademark_gid,
# MAGIC                 fk_tm_party_role_cd,
# MAGIC                 party_type
# MAGIC               )
# MAGIC             ) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd
# MAGIC               order by
# MAGIC                 party_type
# MAGIC             )
# MAGIC           ) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type
# MAGIC             order by
# MAGIC               joint_party_num
# MAGIC           ) as previous_party_ids
# MAGIC         from
# MAGIC           (
# MAGIC             select
# MAGIC               distinct *,
# MAGIC               dense_rank() over (
# MAGIC                 partition by fk_trademark_gid,
# MAGIC                 fk_tm_party_role_cd,
# MAGIC                 party_type
# MAGIC                 order by
# MAGIC                   last_mod_ts
# MAGIC               ) as rank_per_party_type
# MAGIC             from
# MAGIC               (
# MAGIC                 select
# MAGIC                   distinct tpr.fk_trademark_gid,
# MAGIC                   tpr.tm_party_role_id,
# MAGIC                   bar_information_tx,
# MAGIC                   tpr.fk_tm_party_role_cd,
# MAGIC                   tpr.party_role_sequence_no,
# MAGIC                   tpr.party_role_sequence_no % 10 as joint_party_num,
# MAGIC                   floor(tpr.party_role_sequence_no / 100) as party_type,
# MAGIC                   trim(ip.interested_party_nm) as interested_party_nm,
# MAGIC                   ip.last_mod_ts
# MAGIC                 from
# MAGIC                   ${config.tmngpdb_catalog}.bronze.tm_party_role tpr
# MAGIC                   join ${config.tmngpdb_catalog}.bronze.interested_party ip on tpr.fk_interested_party_gid = ip.interested_party_gid
# MAGIC                   join ${config.tmngpdb_catalog}.bronze.tm_party_role_owner tpro on tpr.fk_trademark_gid = tpro.fk_trademark_gid
# MAGIC                   and tpr.party_role_sequence_no = tpro.fk_party_role_sequence_no
# MAGIC               )
# MAGIC           )
# MAGIC         union
# MAGIC         select
# MAGIC           *,
# MAGIC           case
# MAGIC             when rank_per_party_type = max(rank_per_party_type) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd
# MAGIC             ) then 1
# MAGIC             else 0
# MAGIC           end as latest,
# MAGIC           collect_set(interested_party_nm) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type,
# MAGIC             last_mod_ts
# MAGIC           ) as current_interested_party_nm,
# MAGIC           lag(
# MAGIC             collect_set(interested_party_nm) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd,
# MAGIC               party_type,
# MAGIC               last_mod_ts
# MAGIC             )
# MAGIC           ) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd
# MAGIC             order by
# MAGIC               last_mod_ts
# MAGIC           ) as previous_interested_party_nm,
# MAGIC           collect_set(tm_party_role_id) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd,
# MAGIC             party_type
# MAGIC           ) as current_party_ids,
# MAGIC           lag(
# MAGIC             collect_set(tm_party_role_id) over (
# MAGIC               partition by fk_trademark_gid,
# MAGIC               fk_tm_party_role_cd,
# MAGIC               party_type,
# MAGIC               last_mod_ts
# MAGIC             )
# MAGIC           ) over (
# MAGIC             partition by fk_trademark_gid,
# MAGIC             fk_tm_party_role_cd
# MAGIC             order by
# MAGIC               last_mod_ts
# MAGIC           ) as previous_party_ids
# MAGIC         from
# MAGIC           (
# MAGIC             select
# MAGIC               distinct *,
# MAGIC               dense_rank() over (
# MAGIC                 partition by fk_trademark_gid,
# MAGIC                 fk_tm_party_role_cd,
# MAGIC                 party_type
# MAGIC                 order by
# MAGIC                   last_mod_ts
# MAGIC               ) as rank_per_party_type
# MAGIC             from
# MAGIC               (
# MAGIC                 select
# MAGIC                   distinct tprh.fk_trademark_gid,
# MAGIC                   tprh.tm_party_role_id,
# MAGIC                   last_value(tprh.bar_information_tx) over (
# MAGIC                     partition by fk_trademark_gid,
# MAGIC                     tm_party_role_id
# MAGIC                     order by
# MAGIC                       iph.begin_effective_ts
# MAGIC                   ) as bar_information_tx,
# MAGIC                   tprh.fk_tm_party_role_cd,
# MAGIC                   tprh.party_role_sequence_no,
# MAGIC                   tprh.party_role_sequence_no % 10 as joint_party_num,
# MAGIC                   floor(tprh.party_role_sequence_no / 100) as party_type,
# MAGIC                   trim(iph.interested_party_nm) as interested_party_nm,
# MAGIC                   max(iph.last_mod_ts) over (
# MAGIC                     partition by tprh.fk_trademark_gid,
# MAGIC                     tprh.tm_party_role_id,
# MAGIC                     tprh.fk_tm_party_role_cd,
# MAGIC                     iph.interested_party_nm
# MAGIC                   ) as last_mod_ts
# MAGIC                 from
# MAGIC                   ${config.tmngpdb_catalog}.bronze.tm_party_role_h tprh
# MAGIC                   inner join ${config.tmngpdb_catalog}.bronze.interested_party_h iph on tprh.fk_interested_party_gid = iph.interested_party_gid
# MAGIC                 where
# MAGIC                   tprh.fk_tm_party_role_cd != 'OWNER'
# MAGIC                   and iph.action_ct != 'D'
# MAGIC                   and tprh.action_ct != 'D'
# MAGIC               )
# MAGIC           )
# MAGIC       )
# MAGIC     group by
# MAGIC       fk_trademark_gid,
# MAGIC       latest,
# MAGIC       fk_tm_party_role_cd
# MAGIC     having
# MAGIC       latest = 1
# MAGIC       and fk_tm_party_role_cd != 'OWNER'
# MAGIC       and cnt_rl_cd > 1
# MAGIC   )",
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_PHONES_PER_PR_PER_PRN',
# MAGIC     'Target count query for ensuring only one distinct party number per party role is assigned to one distinct phone number',
# MAGIC     'DELTA_LAKE',
# MAGIC     "select
# MAGIC   count(*) as QRY_CNT
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no,
# MAGIC       count(distinct telecom_no) as dstnct_tn_cnt
# MAGIC     from
# MAGIC       (
# MAGIC         select
# MAGIC           tmta.fk_tm_party_role_id,
# MAGIC           ta.telecom_address_gid,
# MAGIC           ta.telecom_no,
# MAGIC           row_number() over (
# MAGIC             partition by tmta.fk_tm_party_role_id
# MAGIC             order by
# MAGIC               ta.telecom_address_gid
# MAGIC           ) as party_no
# MAGIC         from
# MAGIC           ${config.tmngpdb_catalog}.bronze.tm_telecom_addr tmta
# MAGIC           inner join ${config.tmngpdb_catalog}.bronze.telecom_address ta on tmta.fk_telecom_address_gid = ta.telecom_address_gid
# MAGIC         where
# MAGIC           ta.fk_telecom_type_cd = 'OFC'
# MAGIC           and ta.fk_telecom_format_cd = 'US'
# MAGIC           and ta.telecom_no is not null
# MAGIC       )
# MAGIC     group by
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no
# MAGIC     having
# MAGIC       dstnct_tn_cnt > 1
# MAGIC   )",
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_EMAILS_PER_PR_PER_PRN',
# MAGIC     'Target count query for ensuring only one distinct party number per party role is assigned to one distinct email address',
# MAGIC     'DELTA_LAKE',
# MAGIC     "select
# MAGIC   count(*) as QRY_CNT
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no,
# MAGIC       recency,
# MAGIC       count(distinct email) cnt_dstnct_email
# MAGIC     from
# MAGIC       (
# MAGIC         select
# MAGIC           *
# MAGIC         from
# MAGIC           (
# MAGIC             select
# MAGIC               distinct *
# MAGIC             except
# MAGIC               (last_mod_ts, update_per_email, slot, sequence_no),
# MAGIC               dense_rank() over (
# MAGIC                 partition by fk_tm_party_role_id,
# MAGIC                 slot
# MAGIC                 order by
# MAGIC                   sequence_no
# MAGIC               ) as party_no,
# MAGIC               dense_rank() over (
# MAGIC                 partition by fk_tm_party_role_id
# MAGIC                 order by
# MAGIC                   slot desc,
# MAGIC                   last_mod_ts desc
# MAGIC               ) as recency
# MAGIC             from
# MAGIC               (
# MAGIC                 select
# MAGIC                   tmeah.fk_tm_party_role_id,
# MAGIC                   eah.electronic_address_gid,
# MAGIC                   eah.electronic_addr_locator_tx as email,
# MAGIC                   cast(split(electronic_address_gid, ':') [1] as int) as slot,
# MAGIC                   cast(split(electronic_address_gid, ':') [2] as int) as sequence_no,
# MAGIC                   dense_rank() over (
# MAGIC                     partition by eah.electronic_address_gid
# MAGIC                     order by
# MAGIC                       eah.last_mod_ts
# MAGIC                   ) as update_per_email,
# MAGIC                   case
# MAGIC                     when elkp.cnt = 1 then max(eah.last_mod_ts) over (
# MAGIC                       partition by tmeah.fk_tm_party_role_id,
# MAGIC                       eah.electronic_address_gid
# MAGIC                     )
# MAGIC                     else eah.last_mod_ts
# MAGIC                   end as last_mod_ts
# MAGIC                 from
# MAGIC                   ${config.tmngpdb_catalog}.bronze.tm_electronic_addr_h tmeah
# MAGIC                   inner join ${config.tmngpdb_catalog}.bronze.electronic_address_h eah on eah.electronic_address_gid = tmeah.fk_electronic_address_gid
# MAGIC                   inner join (
# MAGIC                     select
# MAGIC                       electronic_address_gid as cnt_electronic_address_gid,
# MAGIC                       count(distinct electronic_addr_locator_tx) as cnt
# MAGIC                     from
# MAGIC                       ${config.tmngpdb_catalog}.bronze.electronic_address_h
# MAGIC                     group by
# MAGIC                       electronic_address_gid
# MAGIC                   ) elkp on elkp.cnt_electronic_address_gid = eah.electronic_address_gid
# MAGIC                 where
# MAGIC                   eah.action_ct != 'D'
# MAGIC                   and tmeah.action_ct != 'D'
# MAGIC               )
# MAGIC           )
# MAGIC         where
# MAGIC           recency < 3
# MAGIC           and party_no < 3
# MAGIC       )
# MAGIC     group by
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no,
# MAGIC       recency
# MAGIC     having
# MAGIC       cnt_dstnct_email > 1
# MAGIC   )",
# MAGIC     '${config.src_sys_name}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TDET_TRGT_GOLD_ZERO_COUNT_CHECK_MULTIPLE_ADDR_PER_PR_PER_PRN',
# MAGIC     'Target count query for ensuring one distinct party number per party role is assigned to one distinct mailing address',
# MAGIC     'DELTA_LAKE',
# MAGIC     "select
# MAGIC   count(*) as QRY_CNT
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no,
# MAGIC       count(distinct address) as dstnct_addr_cnt
# MAGIC     from
# MAGIC       (
# MAGIC         select
# MAGIC           ma.mailing_address_gid,
# MAGIC           fk_tm_party_role_id,
# MAGIC           ma.name_line_2_tx as firm_name,
# MAGIC           row_number() over (
# MAGIC             partition by tmma.fk_tm_party_role_id
# MAGIC             order by
# MAGIC               ma.mailing_address_gid
# MAGIC           ) as party_no,
# MAGIC           trim(
# MAGIC             concat(
# MAGIC               iff(
# MAGIC                 ma.street_line_1_tx is not null,
# MAGIC                 concat(ma.street_line_1_tx, ' '),
# MAGIC                 ''
# MAGIC               ),
# MAGIC               iff(
# MAGIC                 ma.street_line_2_tx is not null,
# MAGIC                 concat(ma.street_line_2_tx, ' '),
# MAGIC                 ''
# MAGIC               ),
# MAGIC               iff(
# MAGIC                 ma.city_nm is not null,
# MAGIC                 concat(ma.city_nm, ' '),
# MAGIC                 ''
# MAGIC               ),
# MAGIC               iff(
# MAGIC                 ma.geographic_region_cd is not null,
# MAGIC                 concat(ma.geographic_region_cd, ' '),
# MAGIC                 ''
# MAGIC               ),
# MAGIC               iff(
# MAGIC                 ma.postal_cd is not null,
# MAGIC                 ma.postal_cd,
# MAGIC                 ''
# MAGIC               )
# MAGIC             )
# MAGIC           ) as address,
# MAGIC           iff(
# MAGIC             ma.country_cd is null
# MAGIC             or ma.country_cd = '',
# MAGIC             null,
# MAGIC             ma.country_cd
# MAGIC           ) as country_cd
# MAGIC         from
# MAGIC           ${config.tmngpdb_catalog}.bronze.tm_mailing_addr tmma
# MAGIC           inner join ${config.tmngpdb_catalog}.bronze.mailing_address ma on tmma.fk_mailing_address_gid = ma.mailing_address_gid
# MAGIC         where
# MAGIC           ma.address_type_ct = 'S'
# MAGIC           and !(
# MAGIC             ma.street_line_1_tx is null
# MAGIC             and ma.street_line_2_tx is null
# MAGIC             and ma.city_nm is null
# MAGIC             and ma.geographic_region_cd is null
# MAGIC             and ma.postal_cd is null
# MAGIC             and ma.country_cd is null
# MAGIC           )
# MAGIC       )
# MAGIC     group by
# MAGIC       fk_tm_party_role_id,
# MAGIC       party_no
# MAGIC     having
# MAGIC       dstnct_addr_cnt > 1
# MAGIC   )",
# MAGIC     '${config.src_sys_name}'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   ${config.data_quality_catalog}.silver.cmn_dq_vrfctn_query_rfrnc
# MAGIC where
# MAGIC   src_sys_name = '${config.src_sys_name}'

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {data_quality_catalog}.silver.cmn_proc_defn_rfrnc.")
