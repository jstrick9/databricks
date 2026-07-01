# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ./../first_level_etl/ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
run_env = common_configs['schema']['tmngpdb_src_catalog']
ttab_scope = common_configs['secrets']['ttab_scope']
edw_scope = common_configs['secrets']['edw_scope']

print(reporting_catalog,run_env,ttab_scope)

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA INPUTS: GET NECESSARY DATA ELEMENTS FROM VARIOUS SOURCES

# COMMAND ----------

# MAGIC %md
# MAGIC ## PULL AND PREPARE NECESSARY DATA FIELDS FROM THE PROSECUTION HISTORY TABLE

# COMMAND ----------

input_ph = spark.sql(f"select * from {reporting_catalog}.silver.stg_ttab_input_ph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET TTAB DATA ELEMENTS

# COMMAND ----------

ttab_query1="select p.NUMBER0,p.TYPE,pr.REF_SERIAL_NUMBER,ph.ENTRY_DATE FILING_DATE,pr.IDENTIFIER,pa.FK_PROCEEDINGNUMBER0 from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE = 'EXA' and ph.IDENTIFIER = 1 order by FILING_DATE"
ttab_query2="select p.NUMBER0, p.TYPE,pr.REF_SERIAL_NUMBER,ph.ENTRY_DATE FILING_DATE,pr.IDENTIFIER,pa.FK_PROCEEDINGNUMBER0 from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE in ('CAN', 'OPP') and pa.ROLE = 'D' order by p.NUMBER0, FILING_DATE"
ttab_query3="select p.NUMBER0,pr.REF_SERIAL_NUMBER,pr.REF_REG_NUMBER,ph.ENTRY_DATE FILING_DATE from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE = 'CON' and pa.ROLE = 'D' and ph.IDENTIFIER = 1 order by p.NUMBER0, FILING_DATE"

# for testing in dev:

# ttab_query1="select p.NUMBER0,p.TYPE,pr.REF_SERIAL_NUMBER,ph.ENTRY_DATE FILING_DATE,pr.IDENTIFIER,pa.FK_PROCEEDINGNUMBER0 from hive_metastore.alteryx_etldb_dev.proceeding p, hive_metastore.alteryx_etldb_dev.party pa, hive_metastore.alteryx_etldb_dev.property pr, hive_metastore.alteryx_etldb_dev.prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE = 'EXA' and ph.IDENTIFIER = 1 order by FILING_DATE"
# ttab_query2="select p.NUMBER0, p.TYPE,pr.REF_SERIAL_NUMBER,ph.ENTRY_DATE FILING_DATE,pr.IDENTIFIER,pa.FK_PROCEEDINGNUMBER0 from hive_metastore.alteryx_etldb_dev.proceeding p, hive_metastore.alteryx_etldb_dev.party pa, hive_metastore.alteryx_etldb_dev.property pr, hive_metastore.alteryx_etldb_dev.prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE in ('CAN', 'OPP') and pa.ROLE = 'D' order by p.NUMBER0, FILING_DATE"
# ttab_query3="select p.NUMBER0,pr.REF_SERIAL_NUMBER,pr.REF_REG_NUMBER,ph.ENTRY_DATE FILING_DATE from hive_metastore.alteryx_etldb_dev.proceeding p, hive_metastore.alteryx_etldb_dev.party pa, hive_metastore.alteryx_etldb_dev.property pr, hive_metastore.alteryx_etldb_dev.prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and p.TYPE = 'CON' and pa.ROLE = 'D' and ph.IDENTIFIER = 1 order by p.NUMBER0, FILING_DATE"

# COMMAND ----------

ip1_df_1026 = read_data_from_oracle_conn_dsu_cmn(ttab_query1,ttab_scope)
ip_df_1023 = read_data_from_oracle_conn_dsu_cmn(ttab_query2,ttab_scope)
ip_df_946 = read_data_from_oracle_conn_dsu_cmn(ttab_query3,ttab_scope)

# for testing in dev:

# ip1_df_1026=spark.sql(f"""{ttab_query1}""")
# ip_df_1023=spark.sql(f"""{ttab_query2}""")
# ip_df_946=spark.sql(f"""{ttab_query3}""")

# COMMAND ----------

# remove invalid dates
ip_df_1023 = ip_df_1023.withColumn(
    "FILING_DATE", col("FILING_DATE").cast(DateType())
).withColumn(
    "FILING_DATE", when(col("FILING_DATE") <= "1400-01-01", None).otherwise(col("FILING_DATE"))
)

# COMMAND ----------

ip_df_select1027 = ip1_df_1026.select(col("NUMBER0"),
                  col("TYPE"),
                  col("REF_SERIAL_NUMBER").cast(StringType()),
                  col("FILING_DATE").cast(DateType()),
                  col("IDENTIFIER"),
                  col("FK_PROCEEDINGNUMBER0").cast(StringType())) \
                      .withColumn("PRCD_NUM", substring(col("FK_PROCEEDINGNUMBER0"),3,6))

#### bug fix: substring values above were incorrect (formerly 5,6)

ip_df_sumrz1028 = ip_df_select1027.groupBy("TYPE","PRCD_NUM","REF_SERIAL_NUMBER").agg(min("FILING_DATE").alias("FILING_DATE"))
#-------------------------
ip_df_select1022 = ip_df_1023.select(col("NUMBER0"),
                  col("TYPE"),
                  col("REF_SERIAL_NUMBER").cast(StringType()),
                  col("FILING_DATE").cast(DateType()),
                  col("IDENTIFIER").cast(StringType()),
                  col("FK_PROCEEDINGNUMBER0").cast(StringType())) \
                      .withColumn("PRCD_NUM", substring(col("FK_PROCEEDINGNUMBER0"),6,5)) \
                          .groupBy("TYPE","FK_PROCEEDINGNUMBER0","REF_SERIAL_NUMBER").agg(min("FILING_DATE").alias("FILING_DATE"))

ip_df_filter1031_CAN = ip_df_select1022.filter(col("TYPE") == "CAN")
ip_df_filter1031 = ip_df_select1022.filter(col("TYPE") != "CAN")

#------------------------------
ip_df_select921 = ip_df_946.select(col("NUMBER0"),
                  col("REF_SERIAL_NUMBER").cast(StringType()),
                  col("REF_REG_NUMBER"),
                  col("FILING_DATE").cast(DateType())) \
                      .groupBy("REF_SERIAL_NUMBER","FILING_DATE").count() \
                          .drop("count")

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET LIVE REGISTRATIONS COUNT BY FISCAL YEAR

# COMMAND ----------

ip_post_reg_mil = spark.sql(f"""select * from {reporting_catalog}.silver.post_reg_milestone""")
# ip_post_reg_mil = spark.sql(f"""select * from trm_reporting_dev.silver.post_reg_milestone limit 100000""")

post_reg_mil_sumrz591 = ip_post_reg_mil.select("serial_number","registration_dt","expiration_dt").distinct()

post_reg_mil_trns592 = post_reg_mil_sumrz591.melt(ids=["serial_number"], values=["registration_dt","expiration_dt"], variableColumnName="LiveRegH_Name", valueColumnName="LiveRegH_Value"
)

post_reg_mil_frm593 = post_reg_mil_trns592.withColumn("LiveRegH_DT", when(col("LiveRegH_Name") == "registration_dt", col("LiveRegH_Value"))
                                                      .when(col("LiveRegH_Name") == "expiration_dt", date_add(col("LiveRegH_Value"),1))
                                                      .otherwise(None)) \
                                                          .withColumn("LiveRegH_Count", when(col("LiveRegH_Name") == "registration_dt",lit(1))
                                                      .when(col("LiveRegH_Name") == "expiration_dt", lit(-1))
                                                      .otherwise(lit(0))) \

post_reg_mil_frm593_1 = post_reg_mil_frm593.withColumn("LiveRegH_FY", when(month(post_reg_mil_frm593.LiveRegH_DT) > 9, (year(post_reg_mil_frm593.LiveRegH_DT) + 1))
                                                        .otherwise(year(post_reg_mil_frm593.LiveRegH_DT))) \
                                                            .filter(col("LiveRegH_DT").isNotNull())
                                                                #.orderBy(col("LiveRegH_DT").asc(),col("LiveRegH_Count").desc())

my_window = Window.orderBy("LiveRegH_DT", col("LiveRegH_Count").desc()).rowsBetween(Window.unboundedPreceding, 0)

post_reg_mil_run596 = post_reg_mil_frm593_1.withColumn('RunTot_LiveRegH_Count', _sum('LiveRegH_Count').over(my_window)) \
    .groupBy("LiveRegH_DT").agg(max("RunTot_LiveRegH_Count").alias("Max_RunTot_LiveRegH_Count"))

post_reg_mil_sumrz774 = post_reg_mil_run596.withColumn("REG_YR", when(month(post_reg_mil_run596.LiveRegH_DT) > 9, (year(post_reg_mil_run596.LiveRegH_DT) + 1))
                                                        .otherwise(year(post_reg_mil_run596.LiveRegH_DT))) \
                                                            .groupBy("REG_YR").agg(max("Max_RunTot_LiveRegH_Count").alias("LIVE_REG_COUNT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET NOTICE OF PUBS FOR BIG NUMBERS  

# COMMAND ----------

ph_select711 = input_ph.select(col("serial_number"),
                 col("ph_action_date"),
                 col("ph_action_code")).filter(col("ph_action_code") == "PUBO") \
                     .withColumn("PUBLISHED",lit(1)) \
                         .groupBy("SERIAL_NUMBER").agg(max("ph_action_date").alias("PUBLICATION_DATE"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## GET DEFAULT DATA ELEMENTS FOR CANCELLATIONS and OPPOSITIONS

# COMMAND ----------

ttab_query4 = "select pr.REF_SERIAL_NUMBER,PH.ENTRY_NUM,ph.ENTRY_DATE,ph.ENTRY_CODE,p.NUMBER0,ph.TEXT,p.TYPE from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and trim(ph.ENTRY_CODE) IN (144, 331)and p.TYPE in ('CAN', 'OPP') order by pr.REF_SERIAL_NUMBER, p.NUMBER0, ph.ENTRY_DATE"
ttab_query5 = "select pr.REF_SERIAL_NUMBER,PH.ENTRY_NUM,ph.ENTRY_DATE,ph.ENTRY_CODE,p.NUMBER0,ph.TEXT,p.TYPE from proceeding p, party pa, property pr, prosecution_historY_event ph where p.NUMBER0 = pa.FK_PROCEEDINGNUMBER0 and p.TYPE = pa.FK_PROCEEDINGTYPE and pa.IDENTIFIER = pr.FK_PARTYIDENTIFIER and p.NUMBER0 = ph.FK_PROCEEDINGNUMBER0 and trim(ph.ENTRY_CODE) IN (813, 809)and p.TYPE in ('CAN', 'OPP') order by pr.REF_SERIAL_NUMBER, p.NUMBER0, ph.ENTRY_DATE"

# COMMAND ----------

ip_df_1089 = read_data_from_oracle_conn_dsu_cmn(ttab_query4,ttab_scope)
ip_df_1094 = read_data_from_oracle_conn_dsu_cmn(ttab_query5,ttab_scope)

# COMMAND ----------

select_1090 = ip_df_1089.select(col("REF_SERIAL_NUMBER"),
                                col("ENTRY_NUM").alias("NOD_ENTRY_NUM"),
                                col("ENTRY_DATE").alias("NOD_DATE").cast(DateType()),
                                col("ENTRY_CODE").alias("NOD_CODE"),
                                col("NUMBER0").alias("PROCEEDING"),
                                col("TEXT").alias("NOD_DESCRIPTION"),
                                col("TYPE").alias("CASE_TYPE"))
select_1095 = ip_df_1094.select(col("REF_SERIAL_NUMBER").alias("Right_REF_SERIAL_NUMBER"),
                                col("ENTRY_NUM").alias("BD_ENTRY_NUM"),
                                col("ENTRY_DATE").alias("BD_DATE").cast(DateType()),
                                col("ENTRY_CODE").alias("BD_CODE"),
                                col("NUMBER0"),
                                col("TEXT").alias("BD_DECISION"),
                                col("TYPE").alias("Right_TYPE"))

join_1096 = \
(
    select_1090
        .join(select_1095,
             on = [col("PROCEEDING") == col("NUMBER0"),
                   col("REF_SERIAL_NUMBER") == col("Right_REF_SERIAL_NUMBER"),
                   col("CASE_TYPE") == col("Right_TYPE")],
             how = "inner"
             )
).drop("Right_REF_SERIAL_NUMBER") \
    .drop("NUMBER0") \
        .drop("Right_TYPE") \
            .filter((col("BD_DATE") > col("NOD_DATE")) & ((col("BD_ENTRY_NUM") - col("NOD_ENTRY_NUM")) == 1))

sumrz_1099 =join_1096.groupBy(col("CASE_TYPE"),
                              col("PROCEEDING"),
                              col("NOD_ENTRY_NUM"),
                              col("NOD_DATE"),
                              col("NOD_CODE"),
                              col("NOD_DESCRIPTION"),
                              col("BD_ENTRY_NUM"),
                              col("BD_DATE"),
                              col("BD_CODE"),
                              col("BD_DECISION")).count() \
                                  .drop("count")
filter_1091_T = sumrz_1099.filter(col("CASE_TYPE") == "CAN")
filter_1091_F = sumrz_1099.filter(col("CASE_TYPE") != "CAN")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Characteristic Data Elements

# COMMAND ----------

input_cde = spark.sql(f"select * from {reporting_catalog}.silver.stg_ttab_input_cde")
