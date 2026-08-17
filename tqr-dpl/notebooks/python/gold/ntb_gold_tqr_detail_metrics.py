# Databricks notebook source
##Purpose: This notebook export data from mysql event_inventory  to elastic index
#Author: Ravi Koppula

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT dbx_env DEFAULT "dev"

# COMMAND ----------

dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file = "../../../notebooks/config/"+dbutils.widgets.get("dbx_env").rstrip()+"/tqr-conf.yaml"
print(f'{config_file=}')

# dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])
ENV = dbx_env

# COMMAND ----------

# MAGIC %run ../shared/ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

from pyspark.sql.functions import lit
import pymysql
import pymysql.cursors
import pandas as pd

from typing import Final

# COMMAND ----------

common_configs = read_yaml(config_file)

tqr_catalog = common_configs['schema']['tqr_catalog']
mysql_secret_scope = common_configs['secrets']['mysql_scope']
json_path = common_configs['elastic']['json_path']
mysql_tqr_db = common_configs['schema']['mysql_tqr_db']
cdc_bucket = common_configs['cdc']['cdc_bucket']
spark.conf.set('conf.cdc_bucket', cdc_bucket)

dm_tqr_db = tqr_catalog+'.gold'
stg_tqr_db = tqr_catalog+'.silver'
#Job variables
job_name = 'ntb_gold_tqr_detail_metrics'
trgt_tbl_name = 'event_inventory'

#job start timestamp
job_start_ts = datetime.datetime.now()

print(f'{stg_tqr_db=},{dm_tqr_db=},{job_start_ts=}, {mysql_tqr_db=}, {cdc_bucket=}')
spark.sql(f"set dm_tqr_db = {dm_tqr_db}")
spark.sql(f"set stg_tqr_db = {stg_tqr_db}")


# COMMAND ----------

INFO: Dict[str, Dict[str, str]] = {
    "dev": {
        "username": "admin",
        "host": 'https://vpc-bdr-dev-opensearch-nfuge7lvdvpyssgskw3bkugeqy.us-east-1.es.amazonaws.com',
    },
    "test": {
        "username": "admin",
        "host": "https://vpc-bdr-test-opensearch-i25b326fjuiaimyrutv2fsav4y.us-east-1.es.amazonaws.com",
    },
    "prod": {
        "username": "E1N5219V80CM",
        "host": "https://vpc-tm-tqr-search-os-domain-prod-acc2nw26u2cjhs73tadmqmxmba.us-east-1.es.amazonaws.com",
    },
}

# COMMAND ----------

HOSTNAME: Final[str] = INFO[ENV]["host"]

USERNAME: Final[str] = dbutils.secrets.get(scope="opensearch_tqr_secret", key="username")
PASSWORD: Final[str] = dbutils.secrets.get(scope="opensearch_tqr_secret", key="password")

PORT: Final[str] = "443"
SSL: Final[str] = "true"
INDEX: Final[str] = "tqr_detail_metrics"
ID: Final[str] = "id"
 
CONFIGS: Final[Dict[str, str]] = {
    "opensearch.nodes.wan.only": "true",
    "opensearch.nodes": HOSTNAME,
    "opensearch.port": PORT,
    "opensearch.net.ssl": SSL,
    "opensearch.net.http.auth.user": USERNAME,
    "opensearch.net.http.auth.pass": PASSWORD,
    "opensearch.resource": INDEX,
    "opensearch.mapping.id": ID,
    # "opensearch.mapping.exclude": ID,
    "opensearch.batch.write.retry.wait": "60s",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Job Control

# COMMAND ----------

# DBTITLE 1,Create entry in job log table and get max dt from job control table
control_dt = begin_job_cntl(stg_tqr_db, job_name, job_start_ts)
if control_dt == 'None':
    control_dt = '1900-01-01'
print(f'{control_dt=}')
spark.sql("set control_dt =" + str(control_dt))

# COMMAND ----------

# DBTITLE 1,Define json parsing functions
def getsearchSufficient(serial_no):
	data = json.loads(serial_no.replace('"','\"'))
	searchSufficient='false'
	if("sufficient" in data['search']):
		if data['search']['sufficient']=='yes':
			searchSufficient='true'
	return   searchSufficient


def getSound(serial_no): 
    try:  
        data=json.loads(serial_no.replace('"','\"'))
        taggedSound=[]
        taggedunSound=[]
        for keys,values in data['detail'].items():
            for keys1,values1 in data['detail'][keys].items():
                if(keys1=='new'):
                    if data['detail'][keys]['new']['sound']=='sound':
                        taggedSound.append(keys)
                                               
        return ' '.join(list(set(taggedSound)))
    except Exception as ex:
        return "error"

def getUnSound(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        taggedunSound=[]
        for keys,values in data['detail'].items():
            for keys1,values1 in data['detail'][keys].items():
                if(keys1=='new'):
                    if data['detail'][keys]['new']['sound']=='unsound':
                        taggedunSound.append(keys)
           
        return   ' '.join(list(set(taggedunSound)))
    except Exception as ex:
        return "error"
        #return "0"

def getMissedtagged(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        taggedMissed=[]
        for keys,values in data['missedNewIssues'].items():
            if(keys=='issues'):
                for missedkeys,missedvalues in data['missedNewIssues']['issues'].items():
                    #print "{0} = {1}".format(key, data['detail'].items())
                    for missedkeys1,missedvalues1 in data['missedNewIssues']['issues'][missedkeys].items():
                        if data['missedNewIssues']['issues'][missedkeys]['type']=='missed':
                            taggedMissed.append(missedkeys)
            else:
                for missedkeys,missedvalues in data['missedNewIssues'].items():
                    for missedkeys1,missedvalues1 in data['missedNewIssues'][missedkeys].items():
                        if data['missedNewIssues'][missedkeys]['type']=='missed':
                            taggedMissed.append(missedkeys)
         
        return   ' '.join(list(set(taggedMissed)))
    except Exception as ex:
        return "error"

def getNewtagged(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        getNewtagged=[]
        for keys,values in data['missedNewIssues'].items():
            if(keys=='issues'):
                for missedkeys,missedvalues in data['missedNewIssues']['issues'].items():
                    #print "{0} = {1}".format(key, data['detail'].items())
                    for missedkeys1,missedvalues1 in data['missedNewIssues']['issues'][missedkeys].items():
                        if data['missedNewIssues']['issues'][missedkeys]['type']=='new':
                            getNewtagged.append(missedkeys)
            else:
                for missedkeys,missedvalues in data['missedNewIssues'].items():
                    for missedkeys1,missedvalues1 in data['missedNewIssues'][missedkeys].items():
                        if data['missedNewIssues'][missedkeys]['type']=='new':
                            getNewtagged.append(missedkeys)
        
        return   ' '.join(list(set(getNewtagged)))
    except Exception as ex:
        return "error"

def getWritingtaggedExcellent(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        taggedWritingExcellent=[]
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if (evidencekeys3 =='writing'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['writing'].items():
                                if (evidencekeys4 =='status'): 
                                    if data['detail'][Soundkeys]['new']['writing']['status']=='Excellent':
                                        taggedWritingExcellent.append(Soundkeys)
        
        													 
        return    ' '.join(list(set(taggedWritingExcellent)))
    except Exception as ex:
        return "error"

def getWritingtaggedDeficient(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        taggedWritingDeficient=[]
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if(evidencekeys3 =='writing'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['writing'].items():
                                if(evidencekeys4 =='status'): 
                              	    if data['detail'][Soundkeys]['new']['writing']['status']=='Deficient':
                                        taggedWritingDeficient.append(Soundkeys)
        return   ' '.join(list(set(taggedWritingDeficient)))
    except Exception as ex:
        return "error"

def getWritingtaggedSatisfactory(serial_no): 
    try:  
        data = json.loads(serial_no.replace('"','\"'))
        taggedWritingSatisfactory=[]
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if (evidencekeys3 =='writing'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['writing'].items():
                                if (evidencekeys4 =='status'): 
                                    if data['detail'][Soundkeys]['new']['writing']['status']=='Satisfactory':
                                        taggedWritingSatisfactory.append(Soundkeys)
        return  ' '.join(list(set(taggedWritingSatisfactory)))
    except Exception as ex:
        return "error"

def getEvidencetaggedSatisfactory(serial_no): 
    try:  
        taggedevidenceSatisfactory=[]
        data = json.loads(serial_no.replace('"','\"'))
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if(evidencekeys3 =='evidence'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['evidence'].items():
                                if(evidencekeys4 =='status'): 
                                    if data['detail'][Soundkeys]['new']['evidence']['status']=='Satisfactory':
                                        taggedevidenceSatisfactory.append(Soundkeys)
        return   ' '.join(list(set(taggedevidenceSatisfactory)))
    except Exception as ex:
        return "error"

def getEvidencetaggedExcellent(serial_no): 
    try:  
        taggedevidenceExcellent=[]
        data = json.loads(serial_no.replace('"','\"'))
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if(evidencekeys3 =='evidence'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['evidence'].items():
                                if(evidencekeys4 =='status'): 
                                    if data['detail'][Soundkeys]['new']['evidence']['status']=='Excellent':
                                        taggedevidenceExcellent.append(Soundkeys)
        return   ' '.join(list(set(taggedevidenceExcellent)))
    except Exception as ex:
        return "error"

def getEvidencetaggedDeficient(serial_no): 
    try:  
        taggedevidenceDeficient=[]
        data = json.loads(serial_no.replace('"','\"'))
        for Soundkeys,Soundvalues in data['detail'].items():
            for Soundkeys1,Soundvalues1 in data['detail'][Soundkeys].items():
                if(Soundkeys1 =='new'):
                    for evidencekeys3,evidencevalues3 in data['detail'][Soundkeys]['new'].items():
                        if(evidencekeys3 =='evidence'):
                            for evidencekeys4,evidencevalues4 in data['detail'][Soundkeys]['new']['evidence'].items():
                                if(evidencekeys4 =='status'): 
                                    if data['detail'][Soundkeys]['new']['evidence']['status']=='Deficient':
                                        taggedevidenceDeficient.append(Soundkeys)
        return   ' '.join(list(set(taggedevidenceDeficient)))
    except Exception as ex:
       return "error"

def getAdminSpecimen(data): 
    try:  
        data=json.loads(data.replace('"','\"'))
        adminSpecimenIssuesBag=[]
        for keys,values in data['detail'].items():
            if(keys=='8'):
                for keys1,values1 in data['detail'][keys].items():
                    for keys2,values2 in data['detail'][keys][keys1].items():
                        if(keys2=='issues'):
                            if(str(values2).strip('[]')!=None):
                                adminSpecimenIssuesBag.append(str(values2).strip('[]').replace("'","").replace("None",""))
                                               
        return ' '.join(list(set(adminSpecimenIssuesBag)))
    except Exception as ex:
        return "error"
        
saveSound = udf(getSound)
saveUnSound = udf(getUnSound)
saveMissedtagged = udf(getMissedtagged)
saveNewtagged = udf(getNewtagged)
saveWritingExcellent = udf(getWritingtaggedExcellent)
saveWritingSatisfactory = udf(getWritingtaggedSatisfactory)
saveWritingDeficient = udf(getWritingtaggedDeficient)
saveEvidenceExcellent = udf(getEvidencetaggedExcellent)
saveEvidenceSatisfactory = udf(getEvidencetaggedSatisfactory)
saveEvidenceDeficient = udf(getEvidencetaggedDeficient)
savesearchSufficient = udf(getsearchSufficient)
#saveStatus=udf(getStatus)
saveAdminSpecimenIssues = udf(getAdminSpecimen)

# COMMAND ----------

stnd_tagged_element_option_query = f"""(select * from stnd_tagged_element_option)"""
stnd_tagged_element_option_df = read_data_from_mysql_conn_dsu(stnd_tagged_element_option_query, mysql_tqr_db)
display(stnd_tagged_element_option_df)

# COMMAND ----------

def getData(appids):
    try:
        appids = appids.repartition(10)
        appids = appids.withColumn("soundTaggedElements",saveSound(appids[0]))
        appids = appids.withColumn("unsoundTaggedElements",saveUnSound(appids[0]))
        appids = appids.withColumn("missedTaggedElements",saveMissedtagged(appids[0]))
        appids = appids.withColumn("newTaggedElements",saveNewtagged(appids[0]))
        appids = appids.withColumn("writingExcellentTaggedElements",saveWritingExcellent(appids[0]))
        appids = appids.withColumn("writingSatisfactoryTaggedElements",saveWritingSatisfactory(appids[0]))
        appids = appids.withColumn("writingDeficientTaggedElements",saveWritingDeficient(appids[0]))
        appids = appids.withColumn("evidenceDeficientTaggedElements",saveEvidenceDeficient(appids[0]))
        appids = appids.withColumn("evidenceSatisfactoryTaggedElements",saveEvidenceSatisfactory(appids[0]))
        appids = appids.withColumn("evidenceExcellentTaggedElements",saveEvidenceExcellent(appids[0]))
        appids = appids.withColumn("searchSufficientIndicator",savesearchSufficient(appids[0]))
        appids = appids.withColumn("adminspecimenissuesbag",saveAdminSpecimenIssues(appids[0]))
        #appids = appids.withColumn("status",saveStatus)
        appids.createOrReplaceTempView("event_search")
        appids.count()
        #hive.executeUpdate("Drop table if exists tqr.tqr_detail_metrics_new ")
        tqr_detail_metrics_newDF=spark.sql("select cast(eventInventoryId as int) eventInventoryId,cast(qualityReviewId as int) qualityReviewId,cast(reviewTypeCode as int) reviewTypeCode ,serialNumber,sourceEventDate,examinerEmployeeNumber,organizationCode,searchPresentIndicator,reviewerEmployeeNumber,reviewStatusTs,assignedTs,completedTs,financialYear,financialQuarter,soundTaggedElements,unsoundTaggedElements,missedTaggedElements,newTaggedElements,writingExcellentTaggedElements,writingSatisfactoryTaggedElements,writingDeficientTaggedElements,evidenceDeficientTaggedElements,evidenceSatisfactoryTaggedElements,evidenceExcellentTaggedElements,cast(searchSufficientIndicator as boolean),case when cast(Length(trim(missedTaggedElements )) as int) > 0 then true else false end as missedIssuesIndicator,case when cast(Length(trim(newTaggedElements )) as int ) > 0 then true else false end as newIssuesIndicator,case when cast(Length(trim(missedTaggedElements)) as int ) > 0 or cast(Length(trim(unsoundTaggedElements)) as int ) > 0 then true else false end as overallDeficientIndicator,case when cast(Length(trim(evidenceDeficientTaggedElements)) as int ) > 0 then true else false end as evidenceDeficientIndicator,case when cast(Length(trim(evidenceSatisfactoryTaggedElements)) as int ) > 0 then true else false end as evidenceSatisfactoryIndicator,case when cast(Length(trim(evidenceExcellentTaggedElements)) as int ) > 0 then true else false end as evidenceExcellentIndicator,case when cast(Length(trim(writingDeficientTaggedElements)) as int ) > 0 then true else false end as writingDeficientIndicator,case when cast(Length(trim(writingSatisfactoryTaggedElements)) as int ) > 0 then true else false end as writingSatisfactoryIndicator,case when cast(Length(trim(writingExcellentTaggedElements)) as int ) > 0 then true else false end as writingExcellentIndicator,adminspecimenissuesbag,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'etl' as create_job_id,from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts ,'etl' as last_mod_user_id FROM event_search Where soundTaggedElements <> 'error' or writingSatisfactoryTaggedElements <> 'error' or writingDeficientTaggedElements <> 'error' or evidenceDeficientTaggedElements <> 'error' or evidenceSatisfactoryTaggedElements <> 'error' or evidenceExcellentTaggedElements <> 'error' or adminspecimenissuesbag  <> 'error'")
        
        tqr_detail_metrics_newDF.createOrReplaceTempView("tqr_detail_metrics_new")

        TaggedElementdf=spark.sql(f" Select * from {stg_tqr_db}.stnd_tagged_element " ) 
        TaggedElementdf.createOrReplaceTempView("STND_TAGGED_ELEMENT")

        stnd_tagged_element_option_query = f"""(select * from stnd_tagged_element_option)"""
        stnd_tagged_element_option_df = read_data_from_mysql_conn_dsu(stnd_tagged_element_option_query, mysql_tqr_db)

        stnd_tagged_element_option_df.createOrReplaceTempView("stnd_tagged_element_option")

        reportdf=spark.sql("select distinct qualityreviewid,trim(exp.soundTaggedElements) soundTaggedElements,trim(exp2.unsoundTaggedElements) unsoundTaggedElements,trim(exp3.missedTaggedElements) missedTaggedElements,trim(exp4.newTaggedElements) newTaggedElements,trim(exp5.writingExcellentTaggedElements) writingExcellentTaggedElements,trim(exp6.writingSatisfactoryTaggedElements) writingSatisfactoryTaggedElements, trim(exp7.writingDeficientTaggedElements) writingDeficientTaggedElements,trim(exp8.evidenceDeficientTaggedElements ) evidenceDeficientTaggedElements ,trim(exp9.evidenceSatisfactoryTaggedElements) evidenceSatisfactoryTaggedElements,trim(exp10.evidenceExcellentTaggedElements ) evidenceExcellentTaggedElements,trim(exp11.adminspecimenissuestag) adminspecimenissuestag from tqr_detail_metrics_new lateral view explode(split(soundTaggedElements,' ')) exp as soundTaggedElements lateral view explode(split(unsoundTaggedElements ,' ')) exp2 as unsoundTaggedElements lateral view explode(split(missedTaggedElements,' ')) exp3 as missedTaggedElements lateral view explode(split(newTaggedElements,' ')) exp4 as newTaggedElements lateral view explode(split(writingExcellentTaggedElements,' ')) exp5 as writingExcellentTaggedElements lateral view explode(split(writingSatisfactoryTaggedElements,' ')) exp6 as writingSatisfactoryTaggedElements lateral view explode(split(writingDeficientTaggedElements,' ')) exp7 as writingDeficientTaggedElements lateral view explode(split(evidenceDeficientTaggedElements,' ')) exp8 as evidenceDeficientTaggedElements lateral view explode(split(evidenceSatisfactoryTaggedElements,' ')) exp9 as evidenceSatisfactoryTaggedElements lateral view explode(split(evidenceExcellentTaggedElements,' ')) exp10 as evidenceExcellentTaggedElements  lateral view explode(split(adminspecimenissuesbag,' ')) exp11 as adminspecimenissuestag ")
        reportdf.createOrReplaceTempView("report")	
        print("reportdf----")
        # reportdf.show(10,False)
        dfFinal=spark.sql("""select distinct a.qualityreviewid,b.tagged_element_nm as soundTaggedElements,c.tagged_element_nm as unsoundTaggedElements,d.tagged_element_nm as missedTaggedElements,e.tagged_element_nm as newTaggedElements,f.tagged_element_nm as writingExcellentTaggedElements,g.tagged_element_nm as writingSatisfactoryTaggedElements,h.tagged_element_nm as writingDeficientTaggedElements,i.tagged_element_nm as evidenceDeficientTaggedElements,j.tagged_element_nm as evidenceSatisfactoryTaggedElements,k.tagged_element_nm as evidenceExcellentTaggedElements,l.option_nm as   adminspecimenissuesbag1,
        a.adminspecimenissuestag adminspecimenissuestag,l.tagged_element_option_id tagged_element_option_id from report a left join STND_TAGGED_ELEMENT b on COALESCE(a.soundtaggedelements,'x')=COALESCE(b.tagged_element_id,'x') left join STND_TAGGED_ELEMENT  c on COALESCE(a.unsoundTaggedElements,'x')=COALESCE(c.tagged_element_id,'x') left join STND_TAGGED_ELEMENT d on COALESCE(a.missedTaggedElements,'x')=COALESCE(d.tagged_element_id,'x') left join STND_TAGGED_ELEMENT e on COALESCE(a.newTaggedElements,'x')=COALESCE(e.tagged_element_id,'x') left join STND_TAGGED_ELEMENT  f on COALESCE(a.writingExcellentTaggedElements,'x')=COALESCE(f.tagged_element_id,'x') left join STND_TAGGED_ELEMENT g on COALESCE(a.writingSatisfactoryTaggedElements,'x')=COALESCE(g.tagged_element_id,'x') left join STND_TAGGED_ELEMENT h on COALESCE(a.writingDeficientTaggedElements,'x')=COALESCE(h.tagged_element_id,'x') left join STND_TAGGED_ELEMENT i on COALESCE(a.evidenceDeficientTaggedElements,'x')=COALESCE(i.tagged_element_id,'x') left join STND_TAGGED_ELEMENT j on COALESCE(a.evidenceSatisfactoryTaggedElements,'x')=COALESCE(j.tagged_element_id,'x') left join STND_TAGGED_ELEMENT k on COALESCE(a.evidenceExcellentTaggedElements,'x')=COALESCE(k.tagged_element_id,'x') left join stnd_tagged_element_option  l on COALESCE(regexp_replace(a.adminspecimenissuestag,",",""),'x')=COALESCE(l.tagged_element_option_id,'x') """)
        dfFinal.createOrReplaceTempView("PivotTable")
        print("dfFinal----")
        # dfFinal.show(10,False)
        PivotTabledf=spark.sql("""select qualityreviewid ,concat(concat_ws(',',collect_set(trim(soundTaggedElements))) )   AS soundTaggedElements,concat(concat_ws(',',collect_set(trim(unsoundTaggedElements)))) AS unsoundTaggedElements,concat(concat_ws(',',collect_set(trim(missedTaggedElements)))) AS missedTaggedElements,concat(concat_ws(',',collect_set(trim(newTaggedElements)) )) AS newTaggedElements,concat(concat_ws(',',collect_set(trim(writingExcellentTaggedElements))) ) AS writingExcellentTaggedElements,concat(concat_ws(',',collect_set(trim(writingSatisfactoryTaggedElements))) ) AS writingSatisfactoryTaggedElements,concat(concat_ws(',',collect_set(trim(writingDeficientTaggedElements))) ) AS writingDeficientTaggedElements,concat(concat_ws(',',collect_set(trim(evidenceDeficientTaggedElements))) ) AS evidenceDeficientTaggedElements,concat(concat_ws(',',collect_set(trim(evidenceSatisfactoryTaggedElements))) ) AS evidenceSatisfactoryTaggedElements,concat(concat_ws(',',collect_set(trim(evidenceExcellentTaggedElements)))) AS evidenceExcellentTaggedElements, concat(concat_ws(',',collect_set(trim(adminspecimenissuesbag1))) ) As adminspecimenissuesbag from PivotTable group by  qualityreviewid """)
        print("PivotTabledf---")
        print("PivotTabledf---",PivotTabledf.count())
         
        PivotTabledf.createOrReplaceTempView("PivotTableFinal")
        global QUALITY_METRIC_INDICATORList
        global REFUSAL_REQUIREMENTList
        global SUBSTANTIVEList
        global PROCEDURALList
        global SUBSTANTIVEERRORLIST 
        QUALITY_METRIC_INDICATORList=TaggedElementdf.select("tagged_element_id").filter("quality_metric_in==true").rdd.flatMap(lambda x: x).collect()
        REFUSAL_REQUIREMENTList=TaggedElementdf.select("tagged_element_id").filter("REFUSAL_REQUIREMENTS_IN == TRUE").rdd.flatMap(lambda x: x).collect()
        SUBSTANTIVEList=TaggedElementdf.select("tagged_element_id").filter("SUBSTANTIVE_IN == TRUE").rdd.flatMap(lambda x: x).collect()
        PROCEDURALList=TaggedElementdf.select("tagged_element_id").filter("PROCEDURAL_IN == TRUE").rdd.flatMap(lambda x: x).collect()
        SUBSTANTIVEERRORLIST=TaggedElementdf.select("tagged_element_id").filter("SUBSTANTIVE_ERROR_IN == TRUE").rdd.flatMap(lambda x: x).collect()

        formed_json_reviewdf=spark.sql("Select * from tqr_detail_metrics_new")
        print ("QUALITY_METRIC_INDICATORList:", QUALITY_METRIC_INDICATORList)
        
        def customFunction(row):
            qualityMetricDeficientIndicator='false'
            refusalsUnsoundIndicator='false'
            substantiveDeficientIndicator='false'
            proceduralDeficientIndicator='false'
            overallExcellentIndicator='false'
            substantiveerror='false'
            #print row['qualityreviewid']
            for p in QUALITY_METRIC_INDICATORList:
                if(str(p) in row['missedTaggedElements'].split()):
                    qualityMetricDeficientIndicator='true'
            for p in QUALITY_METRIC_INDICATORList:
                if (str(p) in row['unsoundTaggedElements'].split()):
                    qualityMetricDeficientIndicator='true'
            #refusalsUnsoundIndicator
            for p in REFUSAL_REQUIREMENTList:
                if (str(p) in row['unsoundTaggedElements'].split()):
                    refusalsUnsoundIndicator='true'
        #substantiveDeficientIndicator
            for p in SUBSTANTIVEList:
                if(str(p) in row['missedTaggedElements'].split()):
                    substantiveDeficientIndicator='true'
            for p in SUBSTANTIVEList:
                if(str(p) in row['unsoundTaggedElements'].split()):
                    substantiveDeficientIndicator='true'  
            #substantiveDeficientIndicator        
            for p in PROCEDURALList:
                if(str(p) in row['missedTaggedElements'].split()):
                    proceduralDeficientIndicator='true'
            for p in PROCEDURALList:
                if(str(p) in row['unsoundTaggedElements'].split()):
                    proceduralDeficientIndicator='true'
                if(str(qualityMetricDeficientIndicator).strip().lower()=='false' and len(str(row['writingDeficientTaggedElements']).strip())==0 and len(str(row['evidenceDeficientTaggedElements']).strip())==0 and (str(row['evidenceExcellentIndicator']).strip().lower()=='true' or  str(row['writingExcellentIndicator']).strip().lower()=='true') and len(str(row['writingSatisfactoryTaggedElements']).strip())==0 and len(str(row['evidenceSatisfactoryTaggedElements']).strip())==0 and str(row['overallDeficientIndicator']).strip().lower()=='false' and str(row['searchSufficientIndicator']).strip().lower()=='true'):
                    overallExcellentIndicator = 'true'	  
            for p in SUBSTANTIVEERRORLIST: # Added for Substantive Error
                if (str(p) in row['missedTaggedElements'].split()):
                    substantiveerror='true'
            for p in SUBSTANTIVEERRORLIST:
                if (str(p) in row['unsoundTaggedElements'].split()):
                    substantiveerror='true'
            return (row['eventInventoryId'],row['qualityReviewId'],row['reviewTypeCode'],row['serialNumber'],row['sourceEventDate'],row['examinerEmployeeNumber'], 
        row['organizationCode'],row['searchPresentIndicator'],row['reviewerEmployeeNumber'],row['reviewStatusTs'],
        row['assignedTs'],row['completedTs'],row['financialYear'],row['financialQuarter'] ,row['soundTaggedElements'],row['unsoundTaggedElements'],row['missedTaggedElements'],row['newTaggedElements'],row['writingExcellentTaggedElements']
                    ,row['writingSatisfactoryTaggedElements'],row['writingDeficientTaggedElements'],row['evidenceDeficientTaggedElements'],row['evidenceSatisfactoryTaggedElements'],row['evidenceExcellentTaggedElements'],row['searchSufficientIndicator'],row['missedIssuesIndicator'],row['newIssuesIndicator'],row['overallDeficientIndicator'],row['evidenceDeficientIndicator'],
                        row['evidenceSatisfactoryIndicator'],row['evidenceExcellentIndicator'],row['writingDeficientIndicator'],row['writingSatisfactoryIndicator'],
                        row['writingExcellentIndicator'],row['create_ts'],row['create_job_id'],row['last_mod_ts'],row['last_mod_user_id'],qualityMetricDeficientIndicator,refusalsUnsoundIndicator,substantiveDeficientIndicator,proceduralDeficientIndicator,overallExcellentIndicator,substantiveerror,row['adminspecimenissuesbag'] )

        formed_json_reviewdfMi = formed_json_reviewdf.rdd.map(lambda y: y.asDict()).map(customFunction)
        #formed_json_reviewdfMi.toDF().show(2)
        preFinaldf=formed_json_reviewdfMi.toDF(sampleRatio=100).selectExpr("_1 as eventInventoryId" , "_2 as qualityReviewId ", "_3 as reviewTypeCode","_4 as serialNumber","_5 as sourceEventDate","_6 as examinerEmployeeNumber", "_7 as organizationCode","_8 as searchPresentIndicator","_9 as reviewerEmployeeNumber","_10 as reviewStatusTs","_11 as assignedTs","_12 as completedTs","_13 as financialYear","_14 as financialQuarter" ,"_15 as soundTaggedElements","_16 as unsoundTaggedElements","_17 as missedTaggedElements","_18 as newTaggedElements","_19 as writingExcellentTaggedElements"            ,"_20 as writingSatisfactoryTaggedElements","_21 as writingDeficientTaggedElements","_22 as evidenceDeficientTaggedElements","_23 as evidenceSatisfactoryTaggedElements","_24 as evidenceExcellentTaggedElements","_25 as searchSufficientIndicator","_26 as missedIssuesIndicator","_27 as newIssuesIndicator","_28 as overallDeficientIndicator","_29 as evidenceDeficientIndicator","_30 as evidenceSatisfactoryIndicator","_31 as evidenceExcellentIndicator","_32 as writingDeficientIndicator","_33 as writingSatisfactoryIndicator","_34 as writingExcellentIndicator","_35 as create_ts","_36 as create_job_id","_37 as last_mod_ts","_38 as last_mod_user_id","_39 as qualityMetricDeficientIndicator","_40 as refusalsUnsoundIndicator","_41 as substantiveDeficientIndicator","_42 as proceduralDeficientIndicator","_43 as overallExcellentIndicator","_44 as substantiveerror","_45 as adminspecimenissuesbag")
        preFinaldf.createOrReplaceTempView("preFinalTable")
        #preFinaldf.filter(col("adminspecimenissuesbag") != None).show(10,False)
        # preFinaldf.show(2,False)
        
        Finaldf=spark.sql("Select a.eventInventoryId as eventInventoryIdentifier,a.qualityReviewId as qualityReviewIdentifier ,a.reviewTypeCode as reviewTypeCode,a.serialNumber as  trademarkSerialNumber ,a.sourceEventDate as eventDateTime,a.examinerEmployeeNumber as examinerEmployeeNumber ,a.organizationCode as organizationCode,cast(a.searchPresentIndicator as boolean) searchcompleteindicator ,a.reviewerEmployeeNumber reviewerEmployeeNumber ,a.reviewStatusTs lastReviewDateTime ,a.assignedTs assignDateTime ,a.completedTs as completeDateTime ,a.financialYear as financialYear,a.financialQuarter as financialQuarterNumber,b.missedTaggedElements missedTagElementNameBag,b.newTaggedElements newTagElementNameBag,b.unsoundTaggedElements unsoundTagElementNameBag,b.soundTaggedElements soundTagElementNameBag,b.evidenceDeficientTaggedElements evidenceDeficientTagElementNameBag,b.evidenceSatisfactoryTaggedElements evidenceSatisfactoryTagElementNameBag,b.evidenceExcellentTaggedElements evidenceExcellentTagElementNameBag, b.writingDeficientTaggedElements writingDeficientTagElementNameBag,b.writingSatisfactoryTaggedElements writingSatisfactoryTagElementNameBag,b.writingExcellentTaggedElements writingExcellentTagElementNameBag,a.searchSufficientIndicator searchSufficientIndicator,a.qualityMetricDeficientIndicator qualityMetricDeficientIndicator,a.missedIssuesIndicator missIssueIndicator,a.newIssuesIndicator newIssueIndicator,a.refusalsUnsoundIndicator refusalUnsoundIndicator,a.substantiveDeficientIndicator substantiveDeficientIndicator,a.proceduralDeficientIndicator proceduralDeficientIndicator,a.overallDeficientIndicator overallDeficientIndicator,a.overallExcellentIndicator overallExcellentIndicator,a.evidenceDeficientIndicator evidenceDeficientIndicator,a.evidenceSatisfactoryIndicator evidenceSatisfactoryIndicator,a.evidenceExcellentIndicator evidenceExcellentIndicator,a.writingDeficientIndicator writingDeficientIndicator,a.writingSatisfactoryIndicator writingSatisfactoryIndicator,a.writingExcellentIndicator writingExcellentIndicator,a.substantiveerror substantiveerrorindicator,CASE WHEN UPPER(CAST(a.missedIssuesIndicator AS STRING)) ='FALSE' and UPPER(CAST(a.newIssuesIndicator AS STRING))='FALSE' AND UPPER(CAST(a.searchSufficientIndicator AS STRING)) ='TRUE' AND UPPER(CAST(a.refusalsUnsoundIndicator AS STRING)) ='FALSE' AND (UPPER(CAST(a.evidenceDeficientIndicator AS STRING)) =='FALSE' OR UPPER(CAST(a.writingDeficientIndicator AS STRING)) =='FALSE') AND (UPPER(CAST(a.evidenceSatisfactoryIndicator AS STRING)) =='TRUE' OR (UPPER(CAST(a.writingsatisfactoryIndicator AS STRING)) =='TRUE')) then true else false  end as satisfactoryindicator,case when upper(cast(a.missedIssuesIndicator AS STRING)) == 'TRUE' then 'true' when upper(cast(a.newIssuesIndicator as STRING)) == 'TRUE' then 'true' when upper(cast(a.refusalsUnsoundIndicator as STRING)) == 'TRUE' then 'true' when (upper(cast(a.evidenceDeficientIndicator as STRING)) == 'TRUE' OR upper(cast(a.writingDeficientIndicator as STRING)) == 'TRUE') then 'true' when (upper(cast(a.evidenceExcellentIndicator as STRING)) == 'TRUE' OR upper(cast(a.writingExcellentIndicator as STRING)) == 'TRUE') then 'true' when (upper(cast(a.searchSufficientIndicator as STRING)) == 'FALSE') then 'true' else 'false' end as findingIndicator,from_utc_timestamp(current_timestamp(),'America/New_York') as createDateTime,'etl' as createUserIdentifier,from_utc_timestamp(current_timestamp(),'America/New_York') as lastModifiedDateTime ,'etl' as lastModifiedUserIdentifier,b.adminspecimenissuesbag adminspecimenissuesbag from  preFinalTable a inner join PivotTableFinal b on a.qualityReviewId = b.qualityReviewId ")

        Finaldf.createOrReplaceTempView("DesTable")
        #hive.executeUpdate("drop table tqr.DesTable")
        Finaldf.write.format("delta").mode("overwrite").saveAsTable(f"{dm_tqr_db}.DesTable2")
        # Finaldf.show(6,False)
        tqr_detail_metricsDF=spark.sql(f"select * from {dm_tqr_db}.tqr_detail_metrics")
        tqr_detail_metricsDF.createOrReplaceTempView("tqr_detail_metrics")
        a=spark.sql("select count(*) as CNT from DesTable where qualityReviewIdentifier in(select qualityreviewidentifier from tqr_detail_metrics)")
        cnt=a.collect()[0]["CNT"]
        print('----cnt----=',cnt)
        rec_cnt = cnt
        if cnt>0:
            print('--- Before rebuild tqr_detail_metrics ---')
            spark.sql(f"insert into {dm_tqr_db}.tqr_detail_metrics select * from {dm_tqr_db}.DesTable2" );
            #Finaldf.write.format(HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR).mode("append").option("table","tqr.tqr_detail_metrics").save()
            tqr_detail_metricsDF=spark.sql(f"select * from {dm_tqr_db}.tqr_detail_metrics")
            tqr_detail_metricsDF.createOrReplaceTempView("tqr_detail_metrics")
            storeddf=spark.sql("SELECT a.eventinventoryidentifier,a.qualityreviewidentifier,a.reviewtypecode,a.trademarkserialnumber,a.eventdatetime,a.examineremployeenumber,a.organizationcode,a.searchcompleteindicator,a.revieweremployeenumber,a.lastreviewdatetime,a.assigndatetime,a.completedatetime,a.financialyear,a.financialquarternumber,a.missedtagelementnamebag,a.newtagelementnamebag,a.unsoundtagelementnamebag,a.soundtagelementnamebag,a.evidencedeficienttagelementnamebag,a.evidencesatisfactorytagelementnamebag,a.evidenceexcellenttagelementnamebag,a.writingdeficienttagelementnamebag,a.writingsatisfactorytagelementnamebag,a.writingexcellenttagelementnamebag,a.searchsufficientindicator,a.qualitymetricdeficientindicator,a.mississueindicator,a.newissueindicator,a.refusalunsoundindicator,a.substantivedeficientindicator,a.proceduraldeficientindicator,a.overalldeficientindicator,a.overallexcellentindicator,a.evidencedeficientindicator,a.evidencesatisfactoryindicator,a.evidenceexcellentindicator,a.writingdeficientindicator,a.writingsatisfactoryindicator,a.writingexcellentindicator,a.substantiveerrorindicator,a.satisfactoryindicator,a.findingindicator,case when b.createdatetime is null then a.createdatetime else b.createdatetime end createdatetime,case when b.createuseridentifier is null then a.createuseridentifier else b.createuseridentifier end createuseridentifier,a.lastmodifieddatetime,a.lastmodifieduseridentifier , a.adminspecimenissuesbag from(select * from (select *,row_number() over(partition by qualityreviewidentifier order by createdatetime desc) rnk from tqr_detail_metrics) p1 where p1.rnk=1) a left outer join (select * from (select qualityreviewidentifier,createdatetime,createuseridentifier, row_number() over(partition by qualityreviewidentifier order by createdatetime desc) rnk from tqr_detail_metrics) p2 where p2.rnk=2) b on a.qualityreviewidentifier=b.qualityreviewidentifier")        
            storeddf.createOrReplaceTempView("temp")
            storeddf.write.format("delta").mode("overwrite").saveAsTable(f"{dm_tqr_db}.tqr_detail_metrics")
            print('---------storeddf-------',storeddf.count())

            print('-------tqr.tqr_detail_metrics overwritted----')
            print('--- After rebuild tqr_detail_metrics from storeddf and storeddf count =',storeddf.count())

        else:
            spark.sql(f" insert into {dm_tqr_db}.tqr_detail_metrics select * from {dm_tqr_db}.DesTable2" )
            print("---No Records--")


        df=spark.sql("select qualityreviewidentifier as qualityreviewidentifier,cast(searchsufficientindicator as boolean) as searchsufficientindicator,cast(qualitymetricdeficientindicator as boolean) as qualitymetricdeficientindicator,cast(mississueindicator as boolean) as mississueindicator,cast(newissueindicator as boolean) as newissueindicator,cast(refusalunsoundindicator as boolean) as refusalunsoundindicator,cast(substantivedeficientindicator as boolean) as substantivedeficientindicator,cast(proceduraldeficientindicator as boolean) as proceduraldeficientindicator,cast(overalldeficientindicator as boolean) as overalldeficientindicator,cast(overallexcellentindicator as boolean) as overallexcellentindicator,cast(evidencedeficientindicator as boolean) as evidencedeficientindicator,cast(evidencesatisfactoryindicator as boolean) as evidencesatisfactoryindicator,cast(evidenceexcellentindicator as boolean) as evidenceexcellentindicator,cast(writingdeficientindicator as boolean) as writingdeficientindicator,cast(writingsatisfactoryindicator as boolean) as writingsatisfactoryindicator,cast(writingexcellentindicator as boolean) as writingexcellentindicator,cast(findingindicator as boolean) as findingindicator,cast(satisfactoryindicator as boolean) satifactoryindicator,cast(substantiveerrorindicator as boolean) substantiveerrorindicator,'etl' as create_user_id,from_utc_timestamp(current_timestamp(),'America/New_York') as create_ts,'etl' as last_mod_user_id,from_utc_timestamp(current_timestamp(),'America/New_York') as last_mod_ts,'0' as lock_control_no from DesTable")
        pandas_df=df.toPandas()
        mysql_df=pd.DataFrame(columns=['fk_quality_review_id','metric_json_doc','create_user_id','create_ts','last_mod_user_id','last_mod_ts','lock_control_no'])
        pandas_df1=pandas_df.loc[:, ~pandas_df.columns.isin(['qualityreviewidentifier','create_user_id','create_ts','last_mod_user_id','last_mod_ts','lock_control_no'])]
        
        for index,row in pandas_df.iterrows() :
            data1=pd.DataFrame(pandas_df.iloc[index])
            #data=data.to_json(orient='records')
            #print pandas_df.iloc[1]
            data=(pandas_df1.iloc[index]).to_json(orient='columns')
            #print data,row['eventinventoryidentifier']
            mysql_df.at[index,'metric_json_doc']=data
            mysql_df.at[index,'fk_quality_review_id']=row['qualityreviewidentifier']
            mysql_df.at[index,'create_user_id']=row['create_user_id']
            mysql_df.at[index,'create_ts']=str(row['create_ts'])
            mysql_df.at[index,'last_mod_user_id']=row['last_mod_user_id']
            mysql_df.at[index,'last_mod_ts']=str(row['last_mod_ts'])
            mysql_df.at[index,'lock_control_no']=row['lock_control_no']
            data=''
            
        

        mysqlspark_df=spark.createDataFrame(mysql_df)

        load_mysql_table_dsu(mysqlspark_df, mysql_tqr_db,'mysql_temp',"overwrite")
        query2=f"INSERT INTO {mysql_tqr_db}.quality_review_metric(fk_quality_review_id,metric_json_doc,create_user_id,create_ts,last_mod_user_id,last_mod_ts,lock_control_no)SELECT sb.fk_quality_review_id,sb.metric_json_doc,sb.create_user_id,sb.create_ts,sb.last_mod_user_id,sb.last_mod_ts,sb.lock_control_no FROM mysql_temp as sb ON DUPLICATE KEY UPDATE metric_json_doc=sb.metric_json_doc,create_user_id=sb.create_user_id,create_ts=sb.create_ts,last_mod_user_id=sb.last_mod_user_id,last_mod_ts=sb.last_mod_ts,lock_control_no=sb.lock_control_no"
        s3_bucket = f'{cdc_bucket}'

        ssl_cert = 'eds/certs/ptacts/ca-bundle.trust.crt'
        ssl_tmp_filename = "/tmp/ca_bundle_trust.crt"

        # download SSL cert from S3 to local temp folder

        print(f"SSL CRT :: {s3_bucket} - {ssl_cert}")
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket_name=s3_bucket, key=ssl_cert)
        s3_object.download_file(ssl_tmp_filename)

        cnx=pymysql.connect(host=dbutils.secrets.get(scope=mysql_secret_scope, key="host"),
                            user=dbutils.secrets.get(scope=mysql_secret_scope, key="username"),
                            password=dbutils.secrets.get(scope=mysql_secret_scope, key="password"),
                            db=mysql_tqr_db,
                            ssl_ca=ssl_tmp_filename)

        cursor = cnx.cursor()

    
        cursor.execute(query2)	
        cursor.execute(f"drop table if exists {mysql_tqr_db}.mysql_temp")
        cursor.execute("commit")
        ##New close
        insmysqldf = spark.sql("select eventInventoryIdentifier as evntid,case when findingIndicator='true' then 1 else 0 end as findind from DesTable") 

        sql=""" UPDATE quality_review quality INNER JOIN temp_table_review  temp ON temp.evntid = quality.fk_event_inventory_id SET quality.finding_in = temp.findind,
        quality.last_mod_ts = now(),quality.last_mod_user_id = 'etl'; """

 
        print ('update_qualityreview count'+ str(insmysqldf.count()))
 
        load_mysql_table_dsu(insmysqldf, mysql_tqr_db,'temp_table_review','overwrite')
        cursor.execute(sql)
        cursor.execute("commit")
        cursor.execute("drop table temp_table_review")
        cnx.close()

    except Exception as e:
        print("Exception message: {}".format(e))
        end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,e)
        raise

# COMMAND ----------


def getDetailsdata(loadDF):
    loadDF = loadDF.withColumn('tagElementNumber',savedata(loadDF[0]).cast("string"))
    loadDF = loadDF.withColumn('eventDateTime', from_unixtime(timestamp=unix_timestamp(timestamp='eventDateTime' )))
    loadDF = loadDF.withColumn('assignDateTime', from_unixtime(timestamp=unix_timestamp(timestamp='assignDateTime' )))
    loadDF = loadDF.withColumn('completeDateTime', from_unixtime(timestamp=unix_timestamp(timestamp='completeDateTime' )))
    loadDF = loadDF.withColumn('lastReviewDateTime', from_unixtime(timestamp=unix_timestamp(timestamp='lastReviewDateTime')))
    loadDF = loadDF.withColumn('substantiveDeficientIndicator', (col("substantiveDeficientIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('refusalUnsoundIndicator', (col("refusalUnsoundIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('qualityMetricDeficientIndicator', (col("qualityMetricDeficientIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('proceduralDeficientIndicator', (col("proceduralDeficientIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('overallExcellentIndicator', (col("overallExcellentIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('evidenceDeficientIndicator', (col("evidenceDeficientIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('evidenceExcellentIndicator', (col("evidenceExcellentIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('writingDeficientIndicator', (col("writingDeficientIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('writingExcellentIndicator', (col("writingExcellentIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('findingIndicator', (col("findingIndicator").cast("boolean")))
    loadDF = loadDF.withColumn('substantiveerrorindicator', (col("substantiveerrorindicator").cast("boolean")))
    loadDF = loadDF.withColumn('satisfactoryindicator', (col("satisfactoryindicator").cast("boolean")))
    loadDF = loadDF.withColumn('id',saveid(loadDF[1]))
    loadDF.select('tagElementNumber').coalesce(1).write.mode("overwrite").format("text").save(json_path)
    parsejson=spark.read.json(json_path)
    schema=parsejson.schema.json()
    schema=StructType.fromJson(json.loads(schema))
    loadDF=loadDF.select(col("id").alias("id"),col("eventInventoryIdentifier").alias("eventInventoryIdentifier"), col("qualityReviewIdentifier").alias("qualityReviewIdentifier"), col("reviewTypeCode").alias("reviewTypeCode") ,col("trademarkSerialNumber").alias("trademarkSerialNumber"), col("eventDateTime").alias("eventDateTime"), col("examinerEmployeeNumber").alias("examinerEmployeeNumber"),col("organizationCode").alias("organizationCode"), col("searchCompleteIndicator").alias("searchCompleteIndicator"), col("reviewerEmployeeNumber").alias("reviewerEmployeeNumber"),col("lastReviewDateTime").alias("lastReviewDateTime"), col("assignDateTime").alias("assignDateTime"), col("completeDateTime").alias("completeDateTime"),col("financialYear").alias("financialYear"), col("missedTagElementNameBag").alias("missedTagElementNameBag"),col("newTagElementNameBag").alias("newTagElementNameBag"), col("unsoundTagElementNameBag").alias("unsoundTagElementNameBag"), col("soundTagElementNameBag").alias("soundTagElementNameBag"),col("evidenceDeficientTagElementNameBag").alias("evidenceDeficientTagElementNameBag"), col("evidenceSatisfactoryTagElementNameBag").alias("evidenceSatisfactoryTagElementNameBag"), col("evidenceExcellentTagElementNameBag").alias("evidenceExcellentTagElementNameBag"),col("writingDeficientTagElementNameBag").alias("writingDeficientTagElementNameBag"), col("writingSatisfactoryTagElementNameBag").alias("writingSatisfactoryTagElementNameBag"), col("writingExcellentTagElementNameBag").alias("writingExcellentTagElementNameBag"),col("searchSufficientIndicator").alias("searchSufficientIndicator"), col("qualityMetricDeficientIndicator").alias("qualityMetricDeficientIndicator"), col("missIssueIndicator").alias("missIssueIndicator"),col("newIssueIndicator").alias("newIssueIndicator"), col("refusalUnsoundIndicator").alias("refusalUnsoundIndicator"), col("substantiveDeficientIndicator").alias("substantiveDeficientIndicator"),col("proceduralDeficientIndicator").alias("proceduralDeficientIndicator"), col("overallDeficientIndicator").alias("overallDeficientIndicator"),col("overallExcellentIndicator").alias("overallExcellentIndicator"), col("evidenceDeficientIndicator").alias("evidenceDeficientIndicator"),col("evidenceSatisfactoryIndicator").alias("evidenceSatisfactoryIndicator"), col("evidenceExcellentIndicator").alias("evidenceExcellentIndicator"), col("writingDeficientIndicator").alias("writingDeficientIndicator"),col("writingExcellentIndicator").alias("writingExcellentIndicator"),col("writingSatisfactoryIndicator").alias("writingSatisfactoryIndicator"),col("findingIndicator").alias("findingIndicator"),col("substantiveerrorindicator").alias("substantiveErrorIndicator"),col("satisfactoryindicator").alias("satisfactoryIndicator"),F.from_json(col("tagElementNumber"),schema).alias("reviewFormJsonDoc"),col("adminspecimenissuesbag").alias("adminSpecimenIssuesBag"))
    #loadDF.show(2)
    loadDF.dropDuplicates()
    try:
        #batch size need to change for prod to 5000
        # loadDF.repartition(10).write.format("org.elasticsearch.spark.sql").option("es.nodes.wan.only","true").option("es.port","9200").option("es.net.ssl","true").option("es.nodes",ES_nodes).option("es.mapping.id","id").option("es.batch.write.retry.count","20").option("es.batch.write.retry.wait","100").option("es.batch.size.entries","100").option("es.batch.size.bytes","10mb").option("es.net.http.auth.user",ES_user).option("es.write.operation","upsert").option("es.update.retry.on.conflict","5").option("es.net.http.auth.pass",ES_pass).save(ES_index, mode="append")
        # print('Exported to Elastic Search')

        loadDF.write.format("org.opensearch.spark.sql").options(**CONFIGS).mode("append").save()

        print('Exported to Open Search')

    except Exception as ex:
        print('Error while exporting to Elastic:',traceback.format_exc())
        end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,ex)
        raise        

	

# COMMAND ----------

spark.sql("drop table if exists tqr_test.gold.DesTable2")
spark.sql("drop table if exists tqr_test.gold.temp_stored")

# COMMAND ----------

qlty_reviewdf=read_data_from_mysql_conn_dsu("select * from v_quality_review", mysql_tqr_db)
qlty_reviewdf.createOrReplaceTempView("qlty_review")


#cntl_dt_df=spark.sql("select max(load_ts) as maxdt from ${stg_tqr_db}.job_control where job_nm='ntb_gold_tqr_detail_metrics' ")
#cntl_dt_df=cntl_dt_df.withColumn('maxdt',to_utc_timestamp(cntl_dt_df.maxdt,timezone))
review_evnt_dt=control_dt

query="Select trim(cast(review_form_json_doc as string)),event_inventory_id eventInventoryId, \
    quality_review_id qualityReviewId, \
    fk_review_type_id reviewTypeCode, \
    serial_no serialNumber, \
    source_event_dt sourceEventDate, \
    examiner_employee_no examinerEmployeeNumber, \
    organization_cd organizationCode, \
    search_present_in searchPresentIndicator, \
    reviewer_employee_no reviewerEmployeeNumber, \
    latest_review_status_ts reviewStatusTs, \
    assigned_dt assignedTs, \
    completed_dt completedTs,COALESCE(fk_review_year_qt,YEAR(source_event_dt)) financialYear, QUARTER(source_event_dt) as financialQuarter  from  qlty_review  where review_form_json_doc is not null and latest_overall_review_status_cd in ('FINALIZED','C_FINALIZED') "

review_ins_query=query

# review_evnt_dt ='None'

if str(review_evnt_dt) =='None':
    review_ins_query=query
else:
    review_ins_query=query + " And  latest_review_status_ts >  '"+str(review_evnt_dt)+"'" 

print('review_ins_query ' + review_ins_query )
appids_temp=spark.sql(review_ins_query)
display(appids_temp)

#appids_temp.show(10,False)
if(appids_temp.count()>0):
    print("---Yes Records--")
    getData(appids_temp)
else:
    print("---No Records--")


"""
print("---inserting into tqr_detail_metrics at the end--")
if appids_temp.count()>0:
    print("inside appids_temp.count() > 0")
    spark.sql(f"insert overwrite table {dm_tqr_db}.tqr_detail_metrics select distinct * from {dm_tqr_db}.temp_stored" )
else:
    print("inside appids_temp.count() = 0")
    spark.sql(f"insert into {dm_tqr_db}.tqr_detail_metrics select distinct * from {dm_tqr_db}.DesTable2" )

print ("Number of serial_no processed successfully: " , str(appids_temp.count()))   
"""
spark.sql(f"drop table if exists {dm_tqr_db}.DesTable2")
spark.sql(f"drop table if exists {dm_tqr_db}.temp_stored")



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.functions import udf, lit, unix_timestamp, from_unixtime
from pyspark.sql.functions import col
from datetime import date,datetime
import json, traceback, datetime, hashlib, sys
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# COMMAND ----------

def get_id(row):
    #keys=str(row)
    keys=str(row).encode('utf-8')
    _id=hashlib.sha224(keys).hexdigest()
    return (_id)



def gettag(data):
    try:
        a=json.dumps(data)
        b=json.loads(a)
        parsejson=str(b).replace('"19":','"Fees":').replace('"18":','"Failure to Function - other - non specimen based":').replace('"17":','"Entity/Citizenships":').replace('"16":','"Drawing":').replace('"15":','"Decl./Verification ":').replace('"14":','"2e(5)":').replace('"13":','"2(e)(3)":').replace('"12":','"2(c)":').replace('"11":','"2(b)":').replace('"10":','"2(a)":').replace('"9":','"Substantive Specimen Issue":').replace('"8":','"Administrative  Specimen Issue":').replace('"7":','"Identifications":').replace('"6":','"Disclaimers":').replace('"5":','"2(e)(4)":').replace('"4":','"2(e)(2)":').replace('"3":','"2(e)(1)":').replace('"2":','"2(d)(pending)":').replace('"1":','"2(d)":').replace('"32":','"Other - Collective Marks":').replace('"46":','"Other - Partial refusal/requirement":').replace('"31":','"Other - Classification":').replace('"52":','"Other - Transliteration":').replace('"25":','"Other - Abandonment - other":').replace('"20":','"Filing Basis Issue(s) ":').replace('"21":','"Generic/2(f)Supp":').replace('"23":','"Not Inherently Distinct":').replace('"25":','"Other - Abandonment - other":').replace('"29":','"Other - Certification Marks":').replace('"45":','"Other - Multiple Class":')
        return parsejson
    except Exception as ex:
        return "data error"



savedata=udf(gettag)
saveid=udf(get_id)

# COMMAND ----------

from datetime import datetime
date_new = datetime.now()
qlty_rvwdf=read_data_from_mysql_conn_dsu("select * from quality_review", mysql_tqr_db)
qlty_rvwdf.createOrReplaceTempView("temp")

tqr_detail_metricsDF=spark.sql(f"select * from {dm_tqr_db}.tqr_detail_metrics")
tqr_detail_metricsDF.createOrReplaceTempView("tqr_detail_metrics")
report_query="select b.review_form_json_doc as form_json_doc,a.* from tqr_detail_metrics a join temp b on a.qualityreviewidentifier= b.quality_review_id  "

# review_evnt_dt ='None'

if str(review_evnt_dt) =='None':
    print("review_evnt_dt is empty")
else:
    report_query += " Where lastModifiedDateTime > '"+str(review_evnt_dt)+"'" 
print(f" report query --- {report_query}")
loadDF=spark.sql(report_query)
loadDF.createOrReplaceTempView("quality_review_view")
loadDF=loadDF.dropDuplicates()
exportCnt=loadDF.count()
print("records qualified for elastic export--",str(exportCnt))
getDetailsdata(loadDF) 
date_new = str(datetime.now().replace(microsecond=0))
#spark.sql(f"insert into {dm_tqr_db}.job_log select  'tqr_detail_metrics',cast('"+date_new+"' as  timestamp),CURRENT_TIMESTAMP ,'completed',"+str(exportCnt)+",'no of records exported to elastic for tqr detail metrics'")
#spark.sql(f"insert into {dm_tqr_db}.job_control select * from (select 'tqr_report' as job_name ,current_timestamp as loaded_dt ,current_timestamp,'etl',current_timestamp,'etl') tab ")


hcount=spark.sql(f"select count(*) total from {dm_tqr_db}.tqr_detail_metrics")

detail_metrics_count=str(hcount.collect()[0].__getitem__("total"))

quality_review_metricdf=read_data_from_mysql_conn_dsu("select * from quality_review_metric", mysql_tqr_db)
quality_review_metric_count=quality_review_metricdf.count()
try:
    # ElasticDF=spark.read.format("org.elasticsearch.spark.sql").option("es.mapping.date.rich","false").option("es.nodes.wan.only","true").option("es.read.field.exclude","reviewFormJsonDoc").option("es.port",ES_port).option("es.net.ssl","true").option("es.batch.write.retry.count","20").option("es.batch.write.retry.wait","100").option("es.batch.size.entries","100").option("es.batch.size.bytes","1mb").option("es.nodes",ES_nodes).option("es.net.http.auth.user",ES_user).option("es.net.http.auth.pass",ES_pass).load(ES_index)

    ElasticDF=spark.read.format("org.opensearch.spark.sql").options(**CONFIGS)
    Elastic_Count=ElasticDF.count()
except Exception as ex:
    Elastic_Count=0
    end_job_cntl(stg_tqr_db, job_name, job_start_ts,'failed',0,ex)
comment='Total count in tqr_detail_metrics is '+str(detail_metrics_count)+'Total count in Quality_Review_metrics is '+str(quality_review_metric_count)
print(comment)
end_job_cntl(stg_tqr_db, job_name, job_start_ts,'completed',exportCnt,"job completed successfully")

# COMMAND ----------

dbutils.notebook.exit(f"Completed Loading {dm_tqr_db}.{trgt_tbl_name}. Number of records appended: {exportCnt} ")
