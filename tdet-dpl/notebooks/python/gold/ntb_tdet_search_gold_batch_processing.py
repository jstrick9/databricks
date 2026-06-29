# Databricks notebook source
# DBTITLE 1,Imports
import pytz
from pytz import timezone
import datetime

# COMMAND ----------

# DBTITLE 1,Set Runtime Environment, Configuration File, Job Control
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = f"../../config/{dbx_env}/tdet-conf.yaml"

job_name = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
    .split("/")[-1]
)
job_start_ts = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
print(f"{config_file=} {job_name=} {job_start_ts=}")

# COMMAND ----------

# DBTITLE 1,Get Common Functions and Parameters
# MAGIC %run ../../shared/ntb_common_func_and_params $config_file=config_file

# COMMAND ----------

# DBTITLE 1,Read Configuration File
configs = read_yaml(config_file)
tmngpdb_catalog = configs["schema"]["source_tmngpdb_catalog"]
tmintltm_catalog = configs["schema"]["source_tmintltm_catalog"]
reporting_catalog = configs["schema"]["source_reporting_catalog"]
worker_catalog = configs["schema"]["source_worker_catalog"]
tdet_catalog = configs["schema"]["trgt_catalog"]

# COMMAND ----------

# DBTITLE 1,Set Environment
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
spark.conf.set("config.tmintltm_catalog", tmintltm_catalog)
spark.conf.set("config.reporting_catalog", reporting_catalog)
spark.conf.set("config.worker_catalog", worker_catalog)
spark.conf.set("config.tdet_catalog", tdet_catalog)

# COMMAND ----------

# DBTITLE 1,Insert Job Control
begin_job_cntl(
    ctlg_db_name=f"{tdet_catalog}.silver", job_name=job_name, job_start_ts=job_start_ts
)

# COMMAND ----------

# DBTITLE 1,Generate All Party History
try:
    spark.sql("""
        create
        or replace temp view historical_parties as with trademarks as (
        select
            distinct tm.trademark_gid,
            tm.serial_num_tx,
            tm.registration_num as registration_number,
            concat(tm.legacy_status_cd, ' - ', sls.description_tx) as legacy_status_cd,
            date(tm.status_dt) as status_date,
            tm.external_reference_tx as docket_number,
            tm.standard_character_tx as mark_tx
        from
            ${config.tmngpdb_catalog}.bronze.trademark tm
            inner join ${config.tmngpdb_catalog}.bronze.stnd_legacy_status sls on tm.legacy_status_cd = sls.status_no
        ),
        trademark_party_roles as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.bar_information_tx,
            tpr.fk_interested_party_gid,
            tpr.fk_tm_party_role_cd
        from
            ${config.tmngpdb_catalog}.bronze.tm_party_role tpr
        ),
        hist_trademark_party_roles as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.fk_interested_party_gid,
            tpr.fk_tm_party_role_cd
        from
            ${config.tmngpdb_catalog}.bronze.tm_party_role_h tpr
        where
            tpr.action_ct != 'D'
        ),
        non_owner_email_base as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.fk_tm_party_role_cd,
            ea.electronic_addr_locator_tx as email,
            case
            when tmea.primary_in = 'Y' then 0
            else (
                dense_rank() over (
                partition by tm_party_role_id,
                cast(
                    split(ea.electronic_address_gid, ':') [1] as int
                )
                order by
                    cast(
                    split(ea.electronic_address_gid, ':') [2] as int
                    )
                )
            )
            end as party_no
        from
            trademark_party_roles tpr
            inner join ${config.tmngpdb_catalog}.bronze.tm_electronic_addr tmea on tpr.tm_party_role_id = tmea.fk_tm_party_role_id
            inner join ${config.tmngpdb_catalog}.bronze.electronic_address ea on ea.electronic_address_gid = tmea.fk_electronic_address_gid
        where
            tpr.fk_tm_party_role_cd != 'OWNER' 
            and ea.fk_electronic_addr_type_cd = 'EMAIL'
        ),
        historic_non_owner_email_base as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.fk_tm_party_role_cd,
            eah.electronic_addr_locator_tx as email,
            case
            when tmeah.primary_in = 'Y' then 0
            else (
                dense_rank() over (
                partition by tm_party_role_id,
                cast(
                    split(eah.electronic_address_gid, ':') [1] as int
                )
                order by
                    cast(
                    split(eah.electronic_address_gid, ':') [2] as int
                    )
                )
            )
            end as party_no
        from
            hist_trademark_party_roles tpr
            inner join ${config.tmngpdb_catalog}.bronze.tm_electronic_addr_h tmeah on tpr.tm_party_role_id = tmeah.fk_tm_party_role_id
            inner join ${config.tmngpdb_catalog}.bronze.electronic_address_h eah on tmeah.fk_electronic_address_gid = eah.electronic_address_gid
            -- and tmeah.cfk_transaction_instance_gid = eah.cfk_transaction_instance_gid change: US647284 hotfix
        where
            eah.action_ct != 'D'
            and tpr.fk_tm_party_role_cd != 'OWNER'
            and eah.fk_electronic_addr_type_cd = 'EMAIL'
        ),
        attorney_email as (
        select
            distinct fk_trademark_gid,
            email as attorney_email
        from
            non_owner_email_base
        where
            fk_tm_party_role_cd = 'AT'
        ),
        hist_attorney_email as (
        select
            distinct fk_trademark_gid,
            array_join(
            collect_set(email) over (partition by fk_trademark_gid),
            ';'
            ) as hist_at_email
        from
            historic_non_owner_email_base
        where
            fk_tm_party_role_cd = 'AT'
        ),
        correspondent_email as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            array_join(
            collect_set(
                case
                when party_no = 0
                and email is not null then email
                end
            ) over (
                partition by fk_trademark_gid,
                tm_party_role_id
            ),
            ';'
            ) as correspondent_email,
            array_join(
            collect_set(
                case
                when party_no != 0 then email
                end
            ) over (
                partition by fk_trademark_gid,
                tm_party_role_id
            ),
            ';'
            ) as secondary_cor_email
        from
            non_owner_email_base
        where
            fk_tm_party_role_cd = 'COR'
            and party_no < 6
        ),
        hist_correspondent_email as (
        select
            distinct fk_trademark_gid,
            array_join(
            collect_set(
                case
                when party_no = 0 then email
                end
            ) over (
                partition by fk_trademark_gid,
                tm_party_role_id
            ),
            ';'
            ) as hist_cr_email,
            array_join(
            collect_set(
                case
                when party_no != 0 then email
                end
            ) over (
                partition by fk_trademark_gid,
                tm_party_role_id
            ),
            ';'
            ) as hist_secondary_cr_email
        from
            historic_non_owner_email_base
        where
            fk_tm_party_role_cd = 'COR'
            and party_no < 6
        ),
        domestic_representative_email as (
        select
            distinct fk_trademark_gid,
            email as domestic_representative_email
        from
            non_owner_email_base
        where
            fk_tm_party_role_cd = 'DR'
        ),
        hist_domestic_representative_email as (
        select
            distinct fk_trademark_gid,
            array_join(
            collect_set(email) over (partition by fk_trademark_gid),
            ';'
            ) as hist_dr_email
        from
            historic_non_owner_email_base
        where
            fk_tm_party_role_cd = 'DR'
        ),
        mailing_info as (
        select
            distinct ma.mailing_address_gid,
            tmma.fk_tm_party_role_id,
            ma.name_line_2_tx as firm_name,
            trim(
            concat_ws(
                " ",
                ma.street_line_1_tx,
                ma.street_line_2_tx,
                ma.city_nm,
                ma.geographic_region_cd,
                ma.postal_cd,
                ma.country_cd
            )
            ) as address
        from
            ${config.tmngpdb_catalog}.bronze.tm_mailing_addr tmma
            inner join ${config.tmngpdb_catalog}.bronze.mailing_address ma on tmma.fk_mailing_address_gid = ma.mailing_address_gid
        where
            ma.address_type_ct = 'S'
            and !(
            ma.street_line_1_tx is null
            and ma.street_line_2_tx is null
            and ma.city_nm is null
            and ma.geographic_region_cd is null
            and ma.postal_cd is null
            and ma.country_cd is null
            )
        ),
        telecom_info as (
        select
            distinct tmta.fk_tm_party_role_id,
            ta.telecom_address_gid,
            ta.telecom_no
        from
            ${config.tmngpdb_catalog}.bronze.tm_telecom_addr tmta
            inner join ${config.tmngpdb_catalog}.bronze.telecom_address ta on tmta.fk_telecom_address_gid = ta.telecom_address_gid
        where
            ta.fk_telecom_type_cd = 'OFC'
            and ta.fk_telecom_format_cd = 'US'
            and ta.telecom_no is not null
        ),
        interested_party as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.bar_information_tx,
            tpr.fk_tm_party_role_cd,
            case
            when tpr.fk_tm_party_role_cd = 'COR' then trim(
                coalesce(ip.interested_party_nm, ma.name_line_1_tx)
            )
            else trim(ip.interested_party_nm)
            end as interested_party_nm
        from
            trademark_party_roles tpr
            left join ${config.tmngpdb_catalog}.bronze.interested_party ip on tpr.fk_interested_party_gid = ip.interested_party_gid
            left join ${config.tmngpdb_catalog}.bronze.tm_mailing_addr tmma on tpr.tm_party_role_id = tmma.fk_tm_party_role_id
            left join ${config.tmngpdb_catalog}.bronze.mailing_address ma on tmma.fk_mailing_address_gid = ma.mailing_address_gid
        where
            !(
                ip.interested_party_nm is null
                and ma.name_line_1_tx is null
            )
        ),
        interested_party_history as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.fk_tm_party_role_cd,
            trim(iph.interested_party_nm) as interested_party_nm,
            iph.last_mod_ts
        from
            hist_trademark_party_roles tpr
            inner join ${config.tmngpdb_catalog}.bronze.interested_party_h iph on tpr.fk_interested_party_gid = iph.interested_party_gid
        where
            iph.action_ct != 'D'
            and iph.interested_party_nm is not null
        ),
        correspondent_addr_name_history as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            trim(mah.name_line_1_tx) as interested_party_nm,
            mah.last_mod_ts
        from
            hist_trademark_party_roles tpr
            inner join ${config.tmngpdb_catalog}.bronze.tm_mailing_addr_h tmma on tpr.tm_party_role_id = tmma.fk_tm_party_role_id
            inner join ${config.tmngpdb_catalog}.bronze.mailing_address_h mah on tmma.fk_mailing_address_gid = mah.mailing_address_gid
        where
            tpr.fk_tm_party_role_cd = 'COR'
            and mah.name_line_1_tx is not null
            and mah.action_ct != 'D'
            and tmma.action_ct != 'D'
        ),
        correspondent_names as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            interested_party_nm as correspondent_name
        from
            interested_party
        where
            fk_tm_party_role_cd = 'COR'
        ),
        hist_correspondent_names as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            array_join(
            collect_set(
                coalesce(iph.interested_party_nm, tma.interested_party_nm)
            ) over (partition by tpr.fk_trademark_gid),
            ';'
            ) as hist_cr_nm
        from
            hist_trademark_party_roles tpr
            left join interested_party_history iph on tpr.tm_party_role_id = iph.tm_party_role_id
            left join correspondent_addr_name_history tma on tpr.tm_party_role_id = tma.tm_party_role_id
        where
            tpr.fk_tm_party_role_cd = 'COR'
        ),
        attorney_names as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            bar_information_tx,
            interested_party_nm as attorney_name
        from
            interested_party
        where
            fk_tm_party_role_cd = 'AT'
        ),
        hist_attorney_names as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            array_join(
            collect_set(interested_party_nm) over (partition by fk_trademark_gid),
            ';'
            ) as hist_attorney_nm
        from
            interested_party_history
        where
            fk_tm_party_role_cd = 'AT'
        ),
        domestic_representative_names as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            interested_party_nm as domestic_representative_name
        from
            interested_party
        where
            fk_tm_party_role_cd = 'DR'
        ),
        hist_domestic_representative_names as (
        select
            distinct fk_trademark_gid,
            tm_party_role_id,
            array_join(
            collect_set(interested_party_nm) over (partition by fk_trademark_gid),
            ';'
            ) as hist_dr_nm
        from
            interested_party_history
        where
            fk_tm_party_role_cd = 'DR'
        ),
        owner_base as (
        select
            distinct tpr.fk_trademark_gid,
            tpr.tm_party_role_id,
            tpr.fk_interested_party_gid,
            floor(tpr.party_role_sequence_no / 100) = max(floor(tpr.party_role_sequence_no / 100)) over (partition by tpr.fk_trademark_gid) as latest
        from
            ${config.tmngpdb_catalog}.bronze.tm_party_role tpr
            inner join ${config.tmngpdb_catalog}.bronze.tm_party_role_owner tpro on tpr.fk_trademark_gid = tpro.fk_trademark_gid
            and tpr.party_role_sequence_no = tpro.fk_party_role_sequence_no
        where
            tpr.fk_tm_party_role_cd = 'OWNER'
        ),
        owner_names as (
        select
            fk_trademark_gid,
            tm_party_role_id,
            trim(ip.interested_party_nm) as owner_name,
            ip.country_cd as owner_country
        from
            owner_base ob
            inner join ${config.tmngpdb_catalog}.bronze.interested_party ip on ob.fk_interested_party_gid = ip.interested_party_gid
        where
            latest = true
        ),
        hist_owner_names as (
        select
            ob.fk_trademark_gid,
            array_join(
            collect_set(trim(iph.interested_party_nm)) over (partition by ob.fk_trademark_gid),
            ';'
            ) as hist_owner_nm
        from
            owner_base ob
            inner join ${config.tmngpdb_catalog}.bronze.interested_party_h iph on ob.fk_interested_party_gid = iph.interested_party_gid
        where
            latest = false
        ),
        hist_owner_email as (
        select
            distinct ob.fk_trademark_gid,
            array_join(
            collect_set(eah.electronic_addr_locator_tx) over (partition by ob.fk_trademark_gid),
            ';'
            ) as hist_owner_email
        from
            owner_base ob
            inner join ${config.tmngpdb_catalog}.bronze.tm_electronic_addr_h tmeah on ob.tm_party_role_id = tmeah.fk_tm_party_role_id
            inner join ${config.tmngpdb_catalog}.bronze.electronic_address_h eah on tmeah.fk_electronic_address_gid = eah.electronic_address_gid
            -- and tmeah.cfk_transaction_instance_gid = eah.cfk_transaction_instance_gid | removed per US647284
        where
            eah.action_ct != 'D'
            and eah.fk_electronic_addr_type_cd = 'EMAIL'
        ),
        owner_email as (
        select
            distinct ob.fk_trademark_gid,
            ob.tm_party_role_id,
            ea.electronic_addr_locator_tx as owner_email
        from
            owner_base ob
            inner join ${config.tmngpdb_catalog}.bronze.tm_electronic_addr tmea on ob.tm_party_role_id = tmea.fk_tm_party_role_id
            inner join ${config.tmngpdb_catalog}.bronze.electronic_address ea on ea.electronic_address_gid = tmea.fk_electronic_address_gid
        where
            ob.latest = true
            and ea.fk_electronic_addr_type_cd = 'EMAIL'
        ),
        trademark_party_history as (
        select
            distinct tm.*,
            IFF(wn.owner_name = '', null, wn.owner_name) as owner_name,
            IFF(hwn.hist_owner_nm = '', null, hwn.hist_owner_nm) as hist_owner_nm,
            oma.address as owner_address,
            wn.owner_country,
            IFF(oe.owner_email = '', null, oe.owner_email) as owner_email,
            IFF(
            oeh.hist_owner_email = '',
            null,
            oeh.hist_owner_email
            ) as hist_owner_email,
            ota.telecom_no as owner_phone,
            an.attorney_name,
            an.bar_information_tx as attorney_membership_no,
            IFF(
            han.hist_attorney_nm = '',
            null,
            han.hist_attorney_nm
            ) as hist_attorney_nm,
            ama.address as attorney_address,
            ata.telecom_no as attorney_phone,
            IFF(ae.attorney_email = '', null, ae.attorney_email) as attorney_email,
            IFF(hae.hist_at_email = '', null, hae.hist_at_email) as hist_at_email,
            cn.correspondent_name,
            cma.address as correspondent_address,
            cma.firm_name,
            IFF(hcn.hist_cr_nm = '', null, hcn.hist_cr_nm) as hist_cr_nm,
            IFF(
            ce.correspondent_email = '',
            null,
            ce.correspondent_email
            ) as correspondent_email,
            IFF(hce.hist_cr_email = '', null, hce.hist_cr_email) as hist_cr_email,
            cta.telecom_no as correspondent_phone,
            IFF(
            ce.secondary_cor_email = '',
            null,
            ce.secondary_cor_email
            ) as secondary_cor_email,
            drn.domestic_representative_name,
            IFF(hdrn.hist_dr_nm = '', null, hdrn.hist_dr_nm) as hist_dr_nm,
            IFF(
            dre.domestic_representative_email = '',
            null,
            dre.domestic_representative_email
            ) as domestic_representative_email,
            IFF(
            hdre.hist_dr_email = '',
            null,
            hdre.hist_dr_email
            ) as hist_dr_email,
            drta.telecom_no as domestic_rep_phone
        from
            trademarks tm
            left join owner_names wn on tm.trademark_gid = wn.fk_trademark_gid
            left join hist_owner_names hwn on tm.trademark_gid = hwn.fk_trademark_gid
            left join owner_email oe on wn.tm_party_role_id = oe.tm_party_role_id
            left join hist_owner_email oeh on tm.trademark_gid = oeh.fk_trademark_gid
            left join telecom_info ota on wn.tm_party_role_id = ota.fk_tm_party_role_id
            left join mailing_info oma on wn.tm_party_role_id = oma.fk_tm_party_role_id
            left join correspondent_names cn on tm.trademark_gid = cn.fk_trademark_gid
            left join hist_correspondent_names hcn on tm.trademark_gid = hcn.fk_trademark_gid
            left join correspondent_email ce on tm.trademark_gid = ce.fk_trademark_gid
            left join hist_correspondent_email hce on tm.trademark_gid = hce.fk_trademark_gid
            left join mailing_info cma on cn.tm_party_role_id = cma.fk_tm_party_role_id
            left join telecom_info cta on cn.tm_party_role_id = cta.fk_tm_party_role_id
            left join attorney_names an on tm.trademark_gid = an.fk_trademark_gid
            left join hist_attorney_names han on tm.trademark_gid = han.fk_trademark_gid
            left join attorney_email ae on tm.trademark_gid = ae.fk_trademark_gid
            left join hist_attorney_email hae on tm.trademark_gid = hae.fk_trademark_gid
            left join mailing_info ama on an.tm_party_role_id = ama.fk_tm_party_role_id
            left join telecom_info ata on an.tm_party_role_id = ata.fk_tm_party_role_id
            left join domestic_representative_names drn on tm.trademark_gid = drn.fk_trademark_gid
            left join hist_domestic_representative_names hdrn on tm.trademark_gid = hdrn.fk_trademark_gid
            left join domestic_representative_email dre on tm.trademark_gid = dre.fk_trademark_gid
            left join hist_domestic_representative_email hdre on tm.trademark_gid = hdre.fk_trademark_gid
            left join telecom_info drta on drn.tm_party_role_id = drta.fk_tm_party_role_id
        )
        select
            *
        from
            trademark_party_history tmph
    """)

except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Generate Output
try:
    spark.sql(
        """
    create
    or replace temp view output as
    select
      distinct hp.serial_num_tx as serial_num,
      coalesce(hp.mark_tx, tml.literal_element_tx) as mark_tx,
      m.filing_date,
      tmfb.filed_bases,
      tmfb.current_bases,
      hp.registration_number,
      m.registration_date,
      hp.owner_name,
      hp.hist_owner_nm,
      hp.owner_address,
      hp.owner_country,
      hp.owner_email,
      hp.hist_owner_email,
      hp.owner_phone,
      hp.attorney_name,
      hp.hist_attorney_nm,
      hp.attorney_membership_no,
      hp.attorney_address,
      hp.attorney_email,
      hp.hist_at_email,
      hp.attorney_phone,
      hp.docket_number,
      hp.correspondent_name,
      hp.correspondent_address,
      hp.firm_name,
      hp.hist_cr_nm,
      hp.correspondent_email,
      hp.hist_cr_email,
      hp.correspondent_phone,
      hp.secondary_cor_email,
      hp.domestic_representative_name,
      hp.hist_dr_nm,
      hp.domestic_representative_email,
      hp.hist_dr_email,
      hp.domestic_rep_phone,
      b.exmr_eid as examiner_number,
      w.worker_nm as examiner_name,
      b.law_office,
      c.class_list,
      hp.legacy_status_cd as status,
      hp.status_date,
      og.og_issue_date,
      og.og_status,
      og.og_catg,
      -- Note: Users indicated that only 79 series numbers should have 'Y' even when non-79 series numbers have an IRN present
      -- We can use this in conjunction with 66A to catch very rare instances of serial numbers that are Madrid but do not have 79 series numbers
      NVL(case when tmfb.current_bases = '66(a)' or hp.serial_num_tx like '79%' then 'Y' else 'N' end, 'N') as intl_reg_num,
      -- USPTO Reference Numbers should still have 'N' flags if they exist, regardless of incoming or outgoing Madrid filing
      NVL(iar.international_us_ref_no, 'N') as international_us_ref_no,
      gs.specimen_url,
      -1 as create_user,
      current_date() as create_dt
    from
      historical_parties hp
      inner join ${config.reporting_catalog}.silver.bibliography b on hp.serial_num_tx = b.ser_num
      inner join (
        select
          ser_num,
          filing_dt as filing_date,
          registration_dt as registration_date
        from
          ${config.reporting_catalog}.silver.milestone
      ) m on hp.serial_num_tx = m.ser_num
      left join (
        select
          ser_num,
          array_join(collect_set(class), ';') as class_list
        from
          ${config.reporting_catalog}.silver.class
        group by
          ser_num
      ) c on hp.serial_num_tx = c.ser_num
      left join (
        select
          cfk_trademark_gid,
          max(fk_international_reg_gid) as intl_reg_num,
          max(fk_international_appl_gid) as international_us_ref_no
        from
          (
            select
              cfk_trademark_gid,
              case
                when fk_international_reg_gid is not null then 'Y'
                else 'N'
              end fk_international_reg_gid,
              case
                when fk_international_appl_gid is not null then 'Y'
                else 'N'
              end fk_international_appl_gid
            from
              ${config.tmintltm_catalog}.bronze.base_appl_intl_reg
            union
            select
              cfk_trademark_gid,
              case
                when fk_international_reg_gid is not null then 'Y'
                else 'N'
              end fk_international_reg_gid,
              'N' as fk_international_appl_gid
            from
              ${config.tmintltm_catalog}.bronze.international_reg_tm
          )
        group by
          cfk_trademark_gid
      ) iar on hp.trademark_gid = iar.cfk_trademark_gid
      left join (
        select distinct
          tmp.fk_trademark_gid,
          ogp.publication_dt as og_issue_date,
          tmp.legacy_og_status_cd as og_status,
          tmpsc.legacy_des_cd as og_catg
        from
          ${config.tmngpdb_catalog}.bronze.tm_publication tmp
        left join ${config.tmngpdb_catalog}.bronze.tm_publication_subct tmpsc 
          on tmp.tm_publication_gid = tmpsc.fk_tm_publication_gid
        left join ${config.tmngpdb_catalog}.bronze.og_publication_tm ogptm 
          on tmp.tm_publication_gid = ogptm.fk_tm_publication_gid
        left join ${config.tmngpdb_catalog}.bronze.og_publication ogp 
          on ogptm.fk_og_publication_gid = ogp.og_publication_gid
      ) og on hp.trademark_gid = og.fk_trademark_gid
      left join ${config.tmngpdb_catalog}.bronze.tm_literal tml on hp.trademark_gid = tml.fk_trademark_gid
      left join ${config.worker_catalog}.bronze.worker w on b.exmr_eid = w.worker_no
      left join (
        select
          serial_num_tx,
          concat_ws('; ',
            collect_set(case when specimen_website_address != '' or specimen_website_address != ' ' then specimen_website_address end)
          ) as specimen_url
        from
          tm.silver.goods_service
        where 
          specimen_website_address is not null
        group by
          serial_num_tx
      ) gs on hp.serial_num_tx = gs.serial_num_tx
      left join (
        select
          split(fk_trademark_gid, ':') [2] as serial_num_tx,
          max(
            case
              when current_in = 'Y' then fk_filing_basis_cd
              else null
            end
          ) as current_bases,
          max(
            case
              when filed_in = 'Y' then fk_filing_basis_cd
              else null
            end
          ) as filed_bases
        from
          ${config.tmngpdb_catalog}.bronze.tm_filing_basis
        group by
          fk_trademark_gid
      ) tmfb on hp.serial_num_tx = tmfb.serial_num_tx
  """)

except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Insert Records
try:
    spark.sql("""
        insert
            overwrite ${config.tdet_catalog}.gold.search
        select
            *
        from
            output
    """)

except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Run Optimize
try:
    spark.sql("optimize ${config.tdet_catalog}.gold.search")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Get Record Insert Count
try:
    output_table_count = spark.sql("select * from ${config.tdet_catalog}.gold.search").count()
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{tdet_catalog}.silver", job_name, job_start_ts, "failed", 0, e)
    raise

# COMMAND ----------

# DBTITLE 1,Update Job Control
end_job_cntl(f"{tdet_catalog}.silver",job_name,job_start_ts,"completed", output_table_count,"Job Completed Successfully")

# COMMAND ----------

# DBTITLE 1,Exit Notebook
dbutils.notebook.exit(f"Completed data load for {tdet_catalog}.gold.search")
