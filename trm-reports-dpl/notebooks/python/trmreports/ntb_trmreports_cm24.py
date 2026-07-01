# Databricks notebook source
from pyspark.sql.functions import regexp_substr

# COMMAND ----------

dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmworker_catalog = common_configs['schema']['tmworker_catalog']

# COMMAND ----------

# DBTITLE 1,Start Job Control
# set current time for both while loop and job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
starttime = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = 'ntb_trmreports_cm24'

control_dt = begin_job_cntl(f'{reporting_catalog}.silver',job_name,starttime)

# COMMAND ----------

df_cm24 = spark.sql(f"""
Select
      distinct Serial_num_tx `Serial #`,
    legacy_status_cd Status,  
    date_format(status_dt,'dd-MMM-yyyy')  `Status Date`,
    W.Worker_nm Attorney,
    Location_desc_tx as lo_raw,
      --REGEXP_SUBSTR(Location_desc_tx, '[0-9]{3}') `Law Office`,
    coalesce(standard_character_tx,Literal_Element_tx) `Mark`,
      business_event_reason_cd `CM Code`,
    title_tx `CM Literal`, order_no,
      replace(replace(coalesce(string(document_component_tx), ' '), '<br/>', ' '), '<br>', ' ') `Photocomp Error`
 
FROM {tmngpdb_catalog}.bronze.trademark TM
LEFT JOIN {tmngpdb_catalog}.bronze.tm_literal LIT
ON TM.TRADEMARK_GID = LIT.FK_TRADEMARK_GID
LEFT JOIN  {tmngpdb_catalog}.bronze.tm_employee_assignment EA
ON  TM.TRADEMARK_GID = EA.FK_TRADEMARK_GID AND FK_TM_EMPLOYEE_ROLE_CD = 'EA'
LEFT JOIN
{tmworker_catalog}.bronze.worker W
ON EA.CFK_EMPLOYEE_NO = W.WORKER_NO
LEFT JOIN {tmngpdb_catalog}.bronze.tm_locations L
ON TM.TRADEMARK_GID = L.FK_TRADEMARK_GID
LEFT JOIN {tmngpdb_catalog}.bronze.internal_note INS
ON  TM.TRADEMARK_GID = INS.FK_TRADEMARK_GID
AND INS.CREATE_USER_ID IN (1111, 99910, 99561) AND CFK_COMPLETED_EMPLOYEE_NO IS NULL AND lower(INS.SUBJECT_TX) like '%error%'
LEFT JOIN {tmngpdb_catalog}.bronze.document_component DC
ON INS.FK_DOCUMENT_COMPONENT_ID = DC.DOCUMENT_COMPONENT_ID
LEFT JOIN
{tmngpdb_catalog}.bronze.tm_organization_location OL ON L.CFK_ASGND_EXAM_LAW_OFC_ORG_CD = OL.LOCATION_CD
LEFT JOIN {tmngpdb_catalog}.bronze.business_event BE
ON  TM.TRADEMARK_GID = BE.CFK_OBJECT_GID
INNER JOIN  {tmngpdb_catalog}.bronze.stnd_business_event_reason BER
ON BE.FK_BUSINESS_EVENT_REASON_ID = BUSINESS_EVENT_REASON_ID
WHERE  LEGACY_STATUS_CD IN ('692', '694')
AND (TM.TRADEMARK_GID, ORDER_NO)
         IN(SELECT TM.TRADEMARK_GID, MAX(ORDER_NO) FROM
            {tmngpdb_catalog}.bronze.trademark TM
            INNER JOIN
            {tmngpdb_catalog}.bronze.business_event BE
            ON  TM.TRADEMARK_GID = BE.CFK_OBJECT_GID
            INNER JOIN  {tmngpdb_catalog}.bronze.stnd_business_event_reason BER
            ON BE.FK_BUSINESS_EVENT_REASON_ID = BUSINESS_EVENT_REASON_ID  
            WHERE  LEGACY_STATUS_CD IN ('692', '694')
              AND BUSINESS_EVENT_REASON_CD NOT IN
               ('ADCHM','ALIEA','APETA','ASGNI','ASCKI',  
                         'ASDFI','CHLDM','CFITO','COARI','DOCKD',  
                        'FINCP','FINOP','FINPP','FINTP','FINVP',      
                        'GBONP','GPNXP','GP2NP','IRRXP','LNNXP',      
                         'OPNRP','OPNSP','OPNXP','PLGLA','REAPI',      
                        'RFNPP','RFNTP','RINXP','RRXXP','RTNXP',      
                        'TCCAI','RNWLP','WOARI','ZZAXZ','ZZBXZ',      
                       'ZZZXZ','ZZZYZ','ZZZZZ','AITUA','NREPP',      
                         'NWAPI','TUPSU','ARAAI','CRCVM','CORRI',    
                        'EXT1S','EXT2S','EXT3S','EXT4S','EXT5S',      
                        'ERTDI','APREA','WDRLS','EPENO','NOAME',      
                        'NOACE','NOADE','NOAMO','NOACO','NOADO',        
                        'MNDAO','MAFRO','MDSCO','MDSMO','MPMKO',      
                         'MDSCE','MDSME','MPMKE','AAUAE','EXRAE',    
                        'SUNAE','AAUAO','EXRAO','SUNAO','MREIO',      
                         'MREIE','NONPE','NPUBE','NPUBO','N12CE',    
                       'N12CO','MAB0O','MAB1O','MAB2O','MAB3O',      
                        'MAB4O','MAB5O','MAB6O','MAB7O','MAB8O',    
                        'MAB9O','MAB0E','MAB1E','MAB2E','MAB3E',      
                        'MAB4E','MAB5E','MAB6E','MAB7E','MAB8E',    
                         'MAB9E','NAS8O','NA85O','NA15O','NA71O',      
                         'NA75O','NAS8E','NA85E','NA15E','NA71E',      
                        'NA75E','NRS9O','NRS9E','NA89O','NA89E',    
                        'NREVO','NREVE','DENAO','DENAE','DENCO',    
                      'DENCE','LOPRP','LOPTI','WOAGI','WOAMI',    
                         'CRCRE','CRSNM','LIMGM','LIMEX','TCCAI',      
                         'INNPP','INNTP','HSCDI','CHANI','CRFAI',    
                         'ECDRI','EWORI','EWAFI','PUDNO',              
                        'CORNI', 'TRNCP','TRPPS','XXCRP','XXSSO','XXXPI',
                        'ADCHM','ALIEA','APETA','ASGNI','ASCKI' ,    
                        'ASDFI','CHLDM','CFITO','COARI','DOCKD'  ,
                         'FINCP','FINOP','FINPP','FINTP','FINVP'  ,  
                        'GBONP','GPNXP','GP2NP','IRRXP','LNNXP'    ,  
                        'OPNRP','OPNSP','OPNXP','PLGLA','REAPI'     ,
                         'RFNPP','RFNTP','RINXP','RRXXP','RTNXP',    
                         'TCCAI','RNWLP','ZZAXZ','ZZBXZ','APREA',      
                        'ZZZXZ','ZZZYZ','ZZZZZ','NWAPI','TUPSU',    
                        'AITUA','NREPP','ARAAI','CRCVM','CORRI',    
                        'EXT1S','EXT2S','EXT3S','EXT4S','EXT5S',      
                        'MREIO','DPCCD','ECRDI','CHANI','WDRLS',  
                        'EPENO','NOAME','CORNI',                    
                         'NOACE','NOADE','NOAMO','NOACO','NOADO',        
                        'MNDAO','MAFRO','MDSCO','MDSMO','MPMKO',    
                        'MDSCE','MDSME','MPMKE','AAUAE','EXRAE',      
                        'SUNAE','AAUAO','EXRAO','SUNAO','MREIO',      
                        'MREIE','NONPE','NPUBE','NPUBO','N12CE',    
                         'N12CO','MAB0O','MAB1O','MAB2O','MAB3O',      
                         'MAB4O','MAB5O','MAB6O','MAB7O','MAB8O' ,    
                         'MAB9O','MAB0E','MAB1E','MAB2E','MAB3E'  ,    
                         'MAB4E','MAB5E','MAB6E','MAB7E','MAB8E',      
                         'MAB9E','NAS8O','NA85O','NA15O','NA71O' ,    
                         'NA75O','NAS8E','NA85E','NA15E','NA71E',    
                        'NA75E','NRS9O','NRS9E','NA89O','NA89E',    
                         'NREVO','NREVE','DENAO','DENAE','DENCO',      
                         'DENCE','HSCDI','EWAFI','EWORI','PUDNO', 'ADCHM','AITUA','ALIEA','APETA','ARAAI'  ,    
                        'ASGNI','ASCKI','ASDFI','CFITO','CFITY'  ,    
                      'CHLDM','COARI','CORRI','CRCVM','DOCKD'  ,    
                        'EXT1S','EXT2S','EXT3S','EXT4S','EXT5S'  ,    
                         'FINCP','FINOP','FINPP','FINTP','FINVP'  ,    
                         'GBONP','GPNXP','GP2NP','IRRXP','LNNXP'  ,    
                        'MREIO','NREPP','OPNRP','OPNSP','OPNXP'  ,    
                        'PLGLA','REAPI','RFNPP','RFNTP','RINXP'  ,    
                         'RNWLP','RRXXP','RTNXP','TCCAI','WOAMI'  ,    
                         'ZZAXZ','ZZBXZ','ZZZXZ','ZZZYZ','ZZZZZ'  ,    
                         'DPCCD','PRA8I','PRA9I','PR12I','PR15I'  ,    
                        'PR23I','PR89I','RRPRI','APREA','ECRDI'  ,    
                        'C.7FI','C7PFI','C7RFI','AMD7I','8.AFI'  ,    
                         '12AFI','15AFI','815FI','8AFTI','89AFI'  ,    
                         '9.AFI','CHANI','FICRP','NOSUI','WDRLS'    ,    
                         'EPENO','WOAGI','WOARI','CORNI','LIMGM'  ,    
                        'LNSAP','RDNYO','RFILS','HSCDI','PUDNO'  ,    
                         'REM1E','REM2E','REM3E','REM4E'          ,    
                         'CORSI','CRCRE','CRRRP','CRSNM','CRSTM'  ,    
                         'FBCSP','FBNCE','FBNXP','INARP','INNAR'  ,    
                        'INNPP','INPCP','INPRP','INPSP','IRFBM'  ,    
                         'IRFIM','IRIVM','IRMPU','IROTM','IRRHM'  ,    
                        'LIMEI','LIMNI','LIMSI','LINXP','LNARP'  ,    
                         'MMPRI','MTNCP','RAPPO','RDENS','RFILI'  ,    
                        'RHRDI','RPRCS','RPRIB','TRDES','TRFLS'  ,    
                        'TRNCP','TRPPS','XXCRP','XXSSO','XXXPI'  ,    
                         'NCS8E','NCS8O','NCS7E','NCS7O','NCP7E'  ,    
                         'NCP7O','NC71E','NC71O','EWAFI','EWORI'
 
                        )
             GROUP BY TM.TRADEMARK_GID )
      --ORDER BY `Serial #` -- removing for write to table
      """)

# COMMAND ----------

# extract LO number
df_cm24 = df_cm24.withColumn(
    'Law Office', regexp_substr(col('lo_raw'), lit(r'[0-9]{3}'))
)

# COMMAND ----------

# rename columns and set ordering
df_cm24 = df_cm24.withColumnsRenamed(
    {'Serial #': 'serial_num',
     'Status': 'status',
     'Status Date': 'status_date',
     'Attorney': 'attorney',
     'Law Office': 'law_office',
     'Mark': 'mark',
     'CM Code': 'cm_code',
     'CM Literal': 'cm_literal',
     'order_no': 'order_no',
     'Photocomp Error': 'photocomp_error'     
     }
)

df_cm24 = df_cm24.select(
    'serial_num',
    'status',
    'status_date',
    'attorney',
    'law_office',
    'mark',
    'cm_code',
    'cm_literal',
    'order_no',
    'photocomp_error'
)

# COMMAND ----------

df_cm24.write.mode("overwrite").format("delta").insertInto(f"{reporting_catalog}.gold.cm24")

# COMMAND ----------

# end job control
recs_count = df_cm24.count()
end_job_cntl(f"{reporting_catalog}.silver", job_name, starttime,'completed', recs_count,"job completed successfully")
