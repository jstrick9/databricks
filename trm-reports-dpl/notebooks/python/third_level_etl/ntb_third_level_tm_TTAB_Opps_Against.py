# Databricks notebook source
# MAGIC %md
# MAGIC ### **_ntb_third_level_tm_TTAB_Opps_Against_**

# COMMAND ----------

# DBTITLE 1,setting up env
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
trmrp_scope = common_configs['secrets']['trmrp_scope']

# edw_scope ='trm_edw_secret'
# reporting_catalog = 'trm_reporting_dev'
print(reporting_catalog)#,run_env)
data_layer = "bronze"

# COMMAND ----------

# DBTITLE 1,Start Job Control
job_name = 'ntb_third_level_tm_TTAB_Opps_Against'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,job_start_ts)

# COMMAND ----------

# This setting is for the "Determining location of DBIO file fragments. This operation can take some time." 
spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Inputs
ttab_query1 ="""select OPP#,
	APPLICATION#,
	FILING_DATE,
	TM_ENT_CODE,
	TM_ENTRY_DATE,
	TIME_TO_NOTICE,
	TIME_FROM_FILING 
from 
	(
	select To_Number(Null) Opp#,
		To_Number(a.NAME) Application#,
		To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') as Filing_Date,
		To_Char('OPNS') TM_ENT_CODE,
		To_Date(Null) TM_ENTRY_DATE,
		To_Number(Null) TIME_TO_NOTICE,
		Trunc(Least(5, SysDate - Trunc(SysDate, 'iw') + 1) - Least(5, To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw') + 1) + (Trunc(SysDate, 'iw') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw')) * 5 / 7) TIME_FROM_FILING 
	from queues a 
	where a.NAME Like '79%' 
		and a.STRVAR2 = 'OP' 
		and a.STRVAR13 = 'Madrid' Minus 

	select To_Number(Null) Opp#,
		To_Number(a.NAME) Application#,
		To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') as Filing_Date,
		To_Char('OPNS') TM_ENT_CODE,
		To_Date(Null) TM_ENTRY_DATE,
		To_Number(Null) TIME_TO_NOTICE,
		Trunc(Least(5, SysDate - Trunc(SysDate, 'iw') + 1) - Least(5, To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw') + 1) + (Trunc(SysDate, 'iw') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw')) * 5 / 7) TIME_FROM_FILING 
	from queues a, BUSINESS_EVENT@TRMNGP BE 
	where a.NAME = SubStr(BE.CFK_OBJECT_GID, 13, 8) 
		and a.NAME Like '79%' 
		and a.STRVAR2 = 'OP' 
		and a.STRVAR13 = 'Madrid' 
		and BE.FK_BUSINESS_EVENT_REASON_ID = 672 

	union

	select Min(c.FK_PROCEEDINGNUMBER0) Opp#,
		b.REF_SERIAL_NUMBER Application#,
		Min(c.ENTRY_DATE) Filing_Date,
		Min(To_Char('OPNS')) TM_ENT_CODE,
		Min(To_Date(Null)) TM_ENTRY_DATE,
		Min(To_Number(Null)) TIME_TO_NOTICE,
		Min(Trunc(Least(5, SysDate - Trunc(SysDate, 'iw') + 1) - Least(5, c.ENTRY_DATE - Trunc(c.ENTRY_DATE, 'iw') + 1) + (Trunc(SysDate, 'iw') - Trunc(c.ENTRY_DATE, 'iw')) * 5 / 7)) TIME_FROM_FILING 
	from Prosecution_History_Event c, party a, Property b, BUSINESS_EVENT@TRMNGP BE 
	where c.FK_PROCEEDINGNUMBER0 = a.FK_PROCEEDINGNUMBER0 
		and a.IDENTIFIER = b.FK_PARTYIDENTIFIER 
		and b.REF_SERIAL_NUMBER = SubStr(BE.CFK_OBJECT_GID, 13, 8) 
		and BE.EFFECTIVE_TS >= c.ENTRY_DATE 
		and b.REF_SERIAL_NUMBER Like '79%' 
		and a.ROLE = 'D' 
		and c.IDENTIFIER = 1 
		and c.FK_PROCEEDINGTYPE = 'OPP' 
		and BE.FK_BUSINESS_EVENT_REASON_ID = 668 
	group by b.REF_SERIAL_NUMBER Minus 

	select Min(c.FK_PROCEEDINGNUMBER0) Opp#,
		b.REF_SERIAL_NUMBER Application#,
		Min(c.ENTRY_DATE) Filing_Date,
		Min(To_Char('OPNS')) TM_ENT_CODE,
		Min(To_Date(Null)) TM_ENTRY_DATE,
		Min(To_Number(Null)) TIME_TO_NOTICE,
		Min(Trunc(Least(5, SysDate - Trunc(SysDate, 'iw') + 1) - Least(5, c.ENTRY_DATE - Trunc(c.ENTRY_DATE, 'iw') + 1) + (Trunc(SysDate, 'iw') - Trunc(c.ENTRY_DATE, 'iw')) * 5 / 7)) TIME_FROM_FILING 
	from Prosecution_History_Event c, party a, Property b, BUSINESS_EVENT@TRMNGP BE 
	where c.FK_PROCEEDINGNUMBER0 = a.FK_PROCEEDINGNUMBER0 
		and a.IDENTIFIER = b.FK_PARTYIDENTIFIER 
		and b.REF_SERIAL_NUMBER = SubStr(BE.CFK_OBJECT_GID, 13, 8) 
		and BE.EFFECTIVE_TS >= c.ENTRY_DATE 
		and b.REF_SERIAL_NUMBER Like '79%' 
		and a.ROLE = 'D' 
		and c.IDENTIFIER = 1 
		and c.FK_PROCEEDINGTYPE = 'OPP' 
		and BE.FK_BUSINESS_EVENT_REASON_ID = 672 
	group by b.REF_SERIAL_NUMBER
	) 
where TIME_FROM_FILING >= 0 

union

select Min(OPP#) as Opp#,
	APPLICATION#,
	Min(FILING_DATE) as FILING_DATE,
	TM_ENTRY_CODE,
	Min(TM_ENTRY_DATE) as TM_ENTRY_DATE,
	Min(TIME_TO_NOTICE) as TIME_TO_NOTICE,
	Min(TIME_FROM_FILING) as TIME_FROM_FILING 
from 
	(
	select To_Number(Null) Opp#,
		To_Number(a.NAME) Application#,
		To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') as Filing_Date,
		'OPNS' as TM_ENTRY_CODE,
		To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') TM_ENTRY_DATE,
		Least(5, To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') - Trunc(To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY'), 'iw') + 1) - Least(5, To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw') + 1) + (Trunc(To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY'), 'iw') - Trunc(To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD'), 'iw')) * 5 / 7 TIME_TO_NOTICE,
		To_Number(Null) TIME_FROM_FILING 
	from queues a, BUSINESS_EVENT@TRMNGP BE 
	where a.NAME = SubStr(BE.CFK_OBJECT_GID, 13, 8) 
		and To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') - To_Date(SubStr(a.STRVAR5, 0, 10), 'YYYY-MM-DD') >= 0 
		and a.NAME Like '79%' 
		and a.STRVAR13 = 'Madrid' 
		and BE.FK_BUSINESS_EVENT_REASON_ID = 672 

	union

	select c.FK_PROCEEDINGNUMBER0 Opp#,
		b.REF_SERIAL_NUMBER Application#,
		c.ENTRY_DATE Filing_Date,
		'OPNS' TM_ENTRY_CODE,
		To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') TM_ENTRY_DATE,
		Least(5, To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') - Trunc(To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY'), 'iw') + 1) - Least(5, c.ENTRY_DATE - Trunc(c.ENTRY_DATE, 'iw') + 1) + (Trunc(To_Date(SubStr(BE.EFFECTIVE_TS, 0, 9), 'DD-MON-YY'), 'iw') - Trunc(c.ENTRY_DATE, 'iw')) * 5 / 7 TIME_TO_NOTICE,
		To_Number(Null) TIME_FROM_FILING 
	from Prosecution_History_Event c, party a, Property b, BUSINESS_EVENT@TRMNGP BE 
	where c.FK_PROCEEDINGNUMBER0 = a.FK_PROCEEDINGNUMBER0 
		and a.IDENTIFIER = b.FK_PARTYIDENTIFIER 
		and b.REF_SERIAL_NUMBER = SubStr(BE.CFK_OBJECT_GID, 13, 8) 
		and BE.EFFECTIVE_TS >= c.ENTRY_DATE 
		and b.REF_SERIAL_NUMBER Like '79%' 
		and a.ROLE = 'D' 
		and c.IDENTIFIER = 1 
		and c.FK_PROCEEDINGTYPE = 'OPP' 
		and BE.FK_BUSINESS_EVENT_REASON_ID = 672
	) 
group by APPLICATION#, TM_ENTRY_CODE 
order by Opp# Desc, Filing_Date desc"""


ttab_query2 = """select phe.FK_PROCEEDINGNUMBER0 as Proceeding_Number,
	phe.TEXT as Latest_PH_Entry,
	phe.IDENTIFIER as PH_Identifier 
from prosecution_history_event phe 
where phe.FK_PROCEEDINGTYPE = 'OPP'"""

ttab_query3 = """ select p.number0 as Proceeding_Number, phe.entry_date as Institution_Date, p.ttab_status as TTAB_Status_Code, scr.ttab_status_text as TTAB_Status, p.ttab_status_date as TTAB_Status_Date from proceeding p, prosecution_history_event phe, ttab_status_code_reference scr where p.number0 = phe.fk_proceedingnumber0 and p.ttab_status = scr.ttab_status_code and p.type = 'OPP' and phe.entry_code = 128"""

ttab_query4 = """select tm.serial_num_tx as Serial_Number, ir.fk_international_reg_no as International_Registration, sls.description_tx as Application_Status
from international_reg_tm@trmngp irt, international_registration@trmngp ir, trademark@trmngp tm, stnd_legacy_status@trmngp sls where ir.international_reg_gid = irt.fk_international_reg_gid and irt.fk_trademark_gid = tm.trademark_gid and tm.legacy_status_cd = sls.status_no """

ttab_query5 =""" select tm.SERIAL_NUM_TX as Serial_Number, To_Date(SubStr(be.EFFECTIVE_TS, 0, 9), 'DD-MON-YY') as Irregularity_From_IB_Date 
from trademark@trmngp tm, BUSINESS_EVENT@TRMNGP be where tm.TRADEMARK_GID = be.CFK_OBJECT_GID and be.FK_BUSINESS_EVENT_REASON_ID = 16 
order by be.EFFECTIVE_TS desc"""

# inpt_10 = read_from_oracle(ttab_query1)
# inpt_34 = read_from_oracle(ttab_query2)
# inpt_32 = read_from_oracle(ttab_query3)
# inpt_11 = read_from_oracle(ttab_query4)
# inpt_17 = read_from_oracle(ttab_query5)
inpt_10 = read_data_from_oracle_conn_dsu_cmn(ttab_query1,trmrp_scope)
inpt_34 = read_data_from_oracle_conn_dsu_cmn(ttab_query2,trmrp_scope)
inpt_32 = read_data_from_oracle_conn_dsu_cmn(ttab_query3,trmrp_scope)
inpt_11 = read_data_from_oracle_conn_dsu_cmn(ttab_query4,trmrp_scope)
inpt_17 = read_data_from_oracle_conn_dsu_cmn(ttab_query5,trmrp_scope)



# COMMAND ----------

## need to change the column name
from pyspark.sql.functions import round
sel_13 = inpt_10.select(col("OPP#"),col("APPLICATION#"),col("FILING_DATE"),col("TM_ENT_CODE"),col("TM_ENTRY_DATE"),round(col("TIME_TO_NOTICE")).alias("TIME_TO_NOTICE"),col("TIME_FROM_FILING"))
sel_35 = inpt_34.select(col("PROCEEDING_NUMBER"),col("LATEST_PH_ENTRY"),col("PH_IDENTIFIER"))
sel_33 = inpt_32.select(col("PROCEEDING_NUMBER"),col("INSTITUTION_DATE"),col("TTAB_STATUS_CODE"),col("TTAB_STATUS"),col("TTAB_STATUS_DATE"))
sel_12 = inpt_11.select(col("SERIAL_NUMBER"),col("INTERNATIONAL_REGISTRATION"),col("APPLICATION_STATUS"))
sel_18 = inpt_17.select(col("SERIAL_NUMBER"),col("IRREGULARITY_FROM_IB_DATE"))

# COMMAND ----------

sort_38 = sel_35.orderBy(sel_35["PROCEEDING_NUMBER"].asc(),sel_35["PH_IDENTIFIER"].desc())
# Sort_38.display()
sort_39 = sel_33.orderBy(sel_33["PROCEEDING_NUMBER"].asc(),sel_33["INSTITUTION_DATE"].desc())

# COMMAND ----------

from pyspark.sql.functions import upper, regexp_replace

cleand_40_1 = sort_38.withColumn("LATEST_PH_ENTRY",
                            upper(
                                regexp_replace(
                                    "LATEST_PH_ENTRY", r"^\s+|\s+$", ""
                                )
                            ))

# COMMAND ----------

### Unique tool 
uniq_37 = cleand_40_1.dropDuplicates(["PROCEEDING_NUMBER"]).orderBy(["PROCEEDING_NUMBER"], ascending=[True, False])
uniq_36 = sort_39.dropDuplicates(["PROCEEDING_NUMBER"]).orderBy(["PROCEEDING_NUMBER"], ascending=[True, False])

# COMMAND ----------

### Join Tool 41

join_41 = (
    uniq_37.alias("l").
    join(
    uniq_36.alias("r"), uniq_37["PROCEEDING_NUMBER"] == uniq_36["PROCEEDING_NUMBER"]
)
    .select("l.PROCEEDING_NUMBER",
            "r.INSTITUTION_DATE",
            "r.TTAB_STATUS_CODE",
            "r.TTAB_STATUS",
            "r.TTAB_STATUS_DATE",
            "l.LATEST_PH_ENTRY"))

# COMMAND ----------

### Join Tool 26 left outer join
# sel_13 as left df
# join_41 as rght df 

join_26 = (
    sel_13.alias("L")
    .join(join_41.alias("R"),
          sel_13["OPP#"] == join_41["PROCEEDING_NUMBER"],
          how="leftouter")
    .select("L.OPP#",
            "L.APPLICATION#",
            "R.INSTITUTION_DATE",
            "L.FILING_DATE",
            "L.TM_ENT_CODE",
            "L.TM_ENTRY_DATE",
            "L.TIME_TO_NOTICE",
            "L.TIME_FROM_FILING",
            "R.TTAB_STATUS_CODE",
            "R.TTAB_STATUS",
            "R.TTAB_STATUS_DATE",
            "R.LATEST_PH_ENTRY")
    )


# COMMAND ----------

### Join Tool 14 inner join
# join_26.alias("L")
# sel_12.alias("R")
# sel_12 = inpt_12.select(col("SERIAL_NUMBER"),col("INTERNATIONAL_REGISTRATION"),col("APPLICATION_STATUS"))
join_14 = (
    join_26.alias("L")
    .join(sel_12.alias("R"),
          join_26["APPLICATION#"] == sel_12["SERIAL_NUMBER"]
          )
    .select("L.OPP#",
            "L.APPLICATION#",
            "R.INTERNATIONAL_REGISTRATION",
            "L.FILING_DATE",
            "L.TM_ENT_CODE",
            "L.TM_ENTRY_DATE",
            "L.TIME_TO_NOTICE",
            "L.TIME_FROM_FILING",
            "R.APPLICATION_STATUS",
            "L.INSTITUTION_DATE",
            "L.TTAB_STATUS_CODE",
            "L.TTAB_STATUS",
            "L.TTAB_STATUS_DATE",
            "L.LATEST_PH_ENTRY")
)

# COMMAND ----------

### Join Tool 19 Left Outer join
#join_14.alias("L")
# sel_18.alias("R") = inpt_17.select(col("SERIAL_NUMBER"),col("IRREGULARITY_FROM_IB_DATE"))
join_19 = (
    join_14.alias("L")
    .join(sel_18.alias("R"),join_14["APPLICATION#"] == sel_18["SERIAL_NUMBER"],
          how="leftouter")
    .select("L.OPP#",
            "L.APPLICATION#",
            "L.INTERNATIONAL_REGISTRATION",
            "L.FILING_DATE",
            "L.TM_ENT_CODE",
            "L.TM_ENTRY_DATE",
            "L.TIME_TO_NOTICE",
            "L.TIME_FROM_FILING",
            "L.APPLICATION_STATUS",
            "L.INSTITUTION_DATE",
            "L.TTAB_STATUS_CODE",
            "L.TTAB_STATUS",
            "L.TTAB_STATUS_DATE",
            "L.LATEST_PH_ENTRY",
            "R.IRREGULARITY_FROM_IB_DATE"
            )
)

# COMMAND ----------

join_19 = join_19.withColumnRenamed("IRREGULARITY_FROM_IB_DATE","Irregularity_Notice_Date") \
    .withColumnRenamed("TM_ENTRY_DATE","IB_NOTICE_DATE")\
        .withColumnRenamed("OPP#","Proceeding_Number")

# COMMAND ----------


from pyspark.sql.functions import col, when

form_21 = join_19.withColumn("Irregularity_From_IB",
                             when(col("Irregularity_Notice_Date").isNull(), "No") \
                             .otherwise("Yes"))

# COMMAND ----------

sel_16 = form_21.withColumnRenamed("APPLICATION#","APPLICATION_NUMBER") \
    .withColumnRenamed("INTERNATIONAL_REGISTRATION","Intl_Reg_Number")
    

# COMMAND ----------

cast_df = sel_16.withColumn("Proceeding_Number", col("Proceeding_Number").cast("bigint")) \
    .withColumn("APPLICATION_NUMBER", col("APPLICATION_NUMBER").cast("bigint")) \
        .withColumn("APPLICATION_NUMBER", col("APPLICATION_NUMBER").cast("bigint")) \
            .withColumn("Intl_Reg_Number", col("Intl_Reg_Number").cast("bigint")) \
                .withColumn("FILING_DATE",col("FILING_DATE").cast("date")) \
                    .withColumn("INSTITUTION_DATE",col("INSTITUTION_DATE").cast("date")) \
                        .withColumn("IB_NOTICE_DATE",col("IB_NOTICE_DATE").cast("date")) \
                            .withColumn("TIME_TO_NOTICE",col("TIME_TO_NOTICE").cast("bigint")) \
                                .withColumn("TIME_FROM_FILING",col("TIME_FROM_FILING").cast("bigint")) \
                                    .withColumn("TTAB_STATUS_DATE",col("TTAB_STATUS_DATE").cast("date")) \
                                        .withColumn("Irregularity_Notice_Date",col("Irregularity_Notice_Date").cast("date"))


# COMMAND ----------

# DBTITLE 1,Final Dataframe
## Final dataframe
sel_16 = cast_df.select(
            "Proceeding_Number",
            "APPLICATION_NUMBER",
            "Intl_Reg_Number",
            "FILING_DATE",
            "INSTITUTION_DATE",
            "IB_NOTICE_DATE",
            "TIME_TO_NOTICE",
            "TIME_FROM_FILING",
            "APPLICATION_STATUS",
            "TTAB_STATUS_CODE",
            "TTAB_STATUS",
            "TTAB_STATUS_DATE",
            "Irregularity_From_IB",
            "Irregularity_Notice_Date",
            "LATEST_PH_ENTRY"
            ).orderBy(col("Proceeding_Number").desc(),col("FILING_DATE").desc())


# COMMAND ----------

# MAGIC %md
# MAGIC ###  Write Data into Tables. 

# COMMAND ----------

try:
    sel_16.write.mode("overwrite").format("delta").insertInto(f'{reporting_catalog}.gold.TTAB_LEADERSHIP')
    recs_count = sel_16.count()
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'completed', recs_count,"job completed successfully")
    dbutils.notebook.exit(f"Completed Loading TTAB_LEADERSHIP Tables ")
except Exception as e:
    print("Exception message: {}".format(e))
    end_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts,'failed',0,e)
    raise
    dbutils.notebook.exit(f"Failed Loading TTAB_LEADERSHIP Table ")
