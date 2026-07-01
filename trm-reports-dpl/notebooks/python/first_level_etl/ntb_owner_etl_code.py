# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Metadata  
# MAGIC Created by:  
# MAGIC Created on:  
# MAGIC Last updated by: Drew McPherson  
# MAGIC Last updated on: 2026-01-14  
# MAGIC
# MAGIC ##Changelog  
# MAGIC 2026-01-08 (Drew McPherson): Expanded country code / country name cleaning.  
# MAGIC 2026-01-14 (Drew McPherson): Tweaked country name values. Corrected issue where cleaning was skipped.  
# MAGIC 2026-05-05 (Drew McPherson): Further tweaks to country name values (e.g. coding new East Germany entries as Germany)  

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Setting environment
dbutils.widgets.text("dbx_env","dev")

# COMMAND ----------

# DBTITLE 1,config file widget
dbx_env = dbutils.widgets.get("dbx_env").rstrip()
config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name
print(f'{config_file=}')

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %run ./ntb_comm_imports_altx $config_file = config_file

# COMMAND ----------

# DBTITLE 1,Setting Config Param
common_configs = read_yaml(config_file)
reporting_catalog = common_configs['schema']['trgt_catalog']
tmngpdb_catalog = common_configs['schema']['tmngpdb_src_catalog']
tmintltm_catalog = common_configs['schema']['tmintltm_src_catalog']
schema_bronze = "bronze"
schema_silver = "silver"
table_silver= "owner"

# COMMAND ----------

# DBTITLE 1,Daily Tables
input_1 = spark.sql(
f"""SELECT
CAST(date_format(AM.FILING_DT, 'yyyyMMdd') AS INTEGER) AS AM_DT_FIL,
CAST(split(AM.TRADEMARK_GID, ':') [2] AS INTEGER) AS AM_SER_NUM,
CAST(
date_format(RI.NOTIFICATION_DT, 'yyyyMMdd') AS INTEGER
) AS RI_NOTIF_DT,
CAST(split(PY.PY_SER_NUM, ':') [2] AS INTEGER) AS PY_SER_NUM,
trim(PY.PY_CITY) as PY_CITY,
trim(PY.COUNTRY_CD) as COUNTRY_CD,
trim(PY.PY_ZIP_CD)as PY_ZIP_CD,
trim(PY.COUNTRY_NM) as CTRY_NM,
trim(PY.STATE_CD) as STATE_CD,
trim(PY.PY_CITIZENSHIP) as PY_CITIZENSHIP,
CAST(PY.PY_ENT_NUM AS INTEGER) AS PY_ENT_NUM,
CAST(PY.PY_ENTITY_TYPE AS INTEGER) AS PY_ENTITY_TYPE,
PY.PY_FLG_NAM_TEXT,
PY.PY_FLG_DBA_AKA,
PY.PY_FLG_ENTITY,
PY.PY_FLG_CMP_STMT,
PY.PY_FLG_ASGT_NAM,
CAST(PY.PY_PARTY_TYPE AS INTEGER) AS PY_PARTY_TYPE
FROM
(
SELECT
*
FROM
{tmngpdb_catalog}.{schema_bronze}.TRADEMARK
) AM
LEFT JOIN {tmintltm_catalog}.{schema_bronze}.INTERNATIONAL_REG_TM RI ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID
INNER JOIN (
SELECT
PR.fk_trademark_gid AS PY_SER_NUM,
MA.city_nm AS PY_CITY,
MA.COUNTRY_CD AS COUNTRY_CD,
MA.COUNTRY_NM as COUNTRY_NM,
MA.geographic_region_cd as STATE_CD,
MA.postal_cd AS PY_ZIP_CD,
IP.COUNTRY_CD AS PY_CITIZENSHIP,
PRO.joint_owner_sequence_no AS PY_ENT_NUM,
IP.fk_legal_entity_type_cd AS PY_ENTITY_TYPE,
0 AS PY_FLG_NAM_TEXT,
0 AS PY_FLG_DBA_AKA,
0 AS PY_FLG_ENTITY,
0 AS PY_FLG_CMP_STMT,
0 AS PY_FLG_ASGT_NAM,
SUBSTR(PR.party_role_sequence_no, 1, 2) AS PY_PARTY_TYPE --TM_PARTY_ROLE.party_role_sequence_no first 2 digits
FROM
{tmngpdb_catalog}.{schema_bronze}.tm_party_role PR
inner join {tmngpdb_catalog}.{schema_bronze}.tm_party_role_owner PRO ON PR.fk_trademark_gid = PRO.fk_trademark_gid
AND PR.party_role_sequence_no = PRO.fk_party_role_sequence_no
left join {tmngpdb_catalog}.{schema_bronze}.interested_party IP ON PR.fk_interested_party_gid = IP.interested_party_gid
--and IP.COUNTRY_ROLE_CT IN ('ORGN', 'INC', 'CIT')
left join (
SELECT
TMA.fk_tm_party_role_id,
MA.*
FROM
{tmngpdb_catalog}.{schema_bronze}.tm_mailing_addr TMA
inner join {tmngpdb_catalog}.{schema_bronze}.mailing_address MA ON TMA.fk_mailing_address_gid = ma.mailing_address_gid
) MA ON PR.tm_party_role_id = MA.fk_tm_party_role_id
WHERE
PR.fk_tm_party_role_cd = 'OWNER'
AND MA.address_type_ct = 'S'
) PY ON AM.TRADEMARK_GID = PY.PY_SER_NUM"""
)

input_2 = spark.sql(
f"""SELECT
CAST(date_format(AM.FILING_DT, 'yyyyMMdd') AS INTEGER) AS AM_DT_FIL,
CAST(split(AM.TRADEMARK_GID, ':') [2] AS INTEGER) AS AM_SER_NUM,
CAST(
date_format(RI.NOTIFICATION_DT, 'yyyyMMdd') AS INTEGER
) AS RI_NOTIF_DT,
CAST(split(PY.PY_SER_NUM, ':') [2] AS INTEGER) AS PY_SER_NUM,
0 AS PY_FLG_ADDR_1,
PY.PY_ADDR_1,
0 AS PY_FLG_ADDR_2,
PY.PY_ADDR_2,
CAST(PY.PY_PARTY_TYPE AS INTEGER) AS PY_PARTY_TYPE,
CAST(PY.PY_ENT_NUM AS INTEGER) AS PY_ENT_NUM,
' ' as LAST_MODIFIED_DATE
FROM
(
SELECT
*
FROM
{tmngpdb_catalog}.{schema_bronze}.TRADEMARK
) AM
LEFT JOIN {tmintltm_catalog}.{schema_bronze}.INTERNATIONAL_REG_TM RI ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID
INNER JOIN (
SELECT
PR.fk_trademark_gid AS PY_SER_NUM,
TRIM(MA.STREET_LINE_1_TX) AS PY_ADDR_1,
TRIM(MA.STREET_LINE_2_TX) AS PY_ADDR_2,
PRO.joint_owner_sequence_no AS PY_ENT_NUM,
SUBSTR(PR.party_role_sequence_no, 1, 2) AS PY_PARTY_TYPE
FROM
{tmngpdb_catalog}.{schema_bronze}.tm_party_role PR
inner join {tmngpdb_catalog}.{schema_bronze}.tm_party_role_owner PRO ON PR.fk_trademark_gid = PRO.fk_trademark_gid
AND PR.party_role_sequence_no = PRO.fk_party_role_sequence_no
inner join (
SELECT
TMA.fk_tm_party_role_id,
MA.*
FROM
{tmngpdb_catalog}.{schema_bronze}.tm_mailing_addr TMA
inner join {tmngpdb_catalog}.{schema_bronze}.mailing_address MA ON TMA.fk_mailing_address_gid = ma.mailing_address_gid
) MA ON PR.tm_party_role_id = MA.fk_tm_party_role_id
WHERE
PR.fk_tm_party_role_cd = 'OWNER'
AND MA.address_type_ct = 'S'
) PY ON AM.TRADEMARK_GID = PY.PY_SER_NUM"""
)

input_3 = spark.sql(
f"""SELECT
CAST(date_format(AM.FILING_DT, 'yyyyMMdd') AS INTEGER) AS AM_DT_FIL,
CAST(split(AM.TRADEMARK_GID, ':') [2] AS INTEGER) AS AM_SER_NUM,
CAST(
date_format(RI.NOTIFICATION_DT, 'yyyyMMdd') AS INTEGER
) AS RI_NOTIF_DT,
CAST(split(PY.PY_SER_NUM, ':') [2] AS INTEGER) AS PY_SER_NUM,
TRIM(PY.INTERESTED_PARTY_NM) as PY_NAM_1,
'' as PY_NAM_2,
'' as PY_NAM_3,
CAST(PY.PY_ENT_NUM AS INTEGER) AS PY_ENT_NUM,
CAST(PY.PY_ENTITY_TYPE AS INTEGER) AS PY_ENTITY_TYPE,
CAST(PY.PY_PARTY_TYPE AS INTEGER) AS PY_PARTY_TYPE
FROM
{tmngpdb_catalog}.{schema_bronze}.TRADEMARK AM
LEFT JOIN {tmintltm_catalog}.{schema_bronze}.INTERNATIONAL_REG_TM RI ON AM.TRADEMARK_GID = RI.CFK_TRADEMARK_GID
INNER JOIN (
SELECT
PR.fk_trademark_gid AS PY_SER_NUM,
IP.INTERESTED_PARTY_NM,
PRO.joint_owner_sequence_no AS PY_ENT_NUM,
IP.fk_legal_entity_type_cd AS PY_ENTITY_TYPE,
SUBSTR(PR.party_role_sequence_no, 1, 2) AS PY_PARTY_TYPE --TM_PARTY_ROLE.party_role_sequence_no first 2 digits
FROM
{tmngpdb_catalog}.{schema_bronze}.tm_party_role PR
inner join {tmngpdb_catalog}.{schema_bronze}.tm_party_role_owner PRO ON PR.fk_trademark_gid = PRO.fk_trademark_gid
AND PR.party_role_sequence_no = PRO.fk_party_role_sequence_no
INNER join {tmngpdb_catalog}.{schema_bronze}.interested_party IP ON PR.fk_interested_party_gid = IP.interested_party_gid
--and IP.COUNTRY_ROLE_CT IN ('ORGN', 'INC', 'CIT')
WHERE
PR.fk_tm_party_role_cd = 'OWNER'
) PY ON AM.TRADEMARK_GID = PY.PY_SER_NUM"""
)

input_4 = spark.sql(
f"""SELECT
'EM' || PR.party_role_sequence_no as VT_TEXT_TYPE,
EM.AUTHORIZED_EMAIL_IN || EM.VT_TEXT AS VT_TEXT,
CAST(split(PR.FK_TRADEMARK_GID, ':') [2] AS INTEGER) AS VT_SER_NUM,
1 AS VT_ENT_NUM
FROM
(
SELECT
PR.FK_TRADEMARK_GID,
PR.party_role_sequence_no,
PR.TM_PARTY_ROLE_ID,
PRO.joint_owner_sequence_no AS PY_ENT_NUM
FROM
{tmngpdb_catalog}.{schema_bronze}.TM_PARTY_ROLE PR
inner join {tmngpdb_catalog}.{schema_bronze}.tm_party_role_owner PRO ON PR.fk_trademark_gid = PRO.fk_trademark_gid
AND PR.party_role_sequence_no = PRO.fk_party_role_sequence_no
INNER join {tmngpdb_catalog}.{schema_bronze}.interested_party IP ON PR.fk_interested_party_gid = IP.interested_party_gid
WHERE
PR.fk_tm_party_role_cd = 'OWNER'
) PR
INNER JOIN (
SELECT
distinct TEA.FK_TM_PARTY_ROLE_ID,
EA.electronic_addr_locator_tx AS VT_TEXT,
TEA.AUTHORIZED_EMAIL_IN
FROM
{tmngpdb_catalog}.{schema_bronze}.TM_ELECTRONIC_ADDR TEA
INNER JOIN {tmngpdb_catalog}.{schema_bronze}.ELECTRONIC_ADDRESS EA ON TEA.FK_ELECTRONIC_ADDRESS_GID = EA.ELECTRONIC_ADDRESS_GID
) EM ON PR.TM_PARTY_ROLE_ID = EM.FK_TM_PARTY_ROLE_ID"""
)

# COMMAND ----------

# DBTITLE 1,Country Code Crosswalk
ctry_cd_dct = {
  "AF": "AFGHANISTAN",
  "AL": "ALBANIA",
  "DZ": "ALGERIA",
  "AS": "AMERICAN SAMOA",
  "AD": "ANDORRA",
  "AO": "ANGOLA",
  "AI": "ANGUILLA",
  "AQ": "ANTARCTICA",
  "AG": "ANTIGUA AND BARBUDA",
  "AR": "ARGENTINA",
  "AM": "ARMENIA",
  "AW": "ARUBA",
  "AU": "AUSTRALIA",
  "AT": "AUSTRIA",
  "AZ": "AZERBAIJAN",
  "BS": "BAHAMAS",
  "BH": "BAHRAIN",
  "BD": "BANGLADESH",
  "BB": "BARBADOS",
  "BY": "BELARUS",
  "BE": "BELGIUM",
  "BZ": "BELIZE",
  "BJ": "BENIN",
  "BM": "BERMUDA",
  "AX": "ALAND ISLANDS",
  "BT": "BHUTAN",
  "BO": "BOLIVIA",
  "BQ": "BONAIRE: SINT EUSTATIUS AND SABA",
  "BA": "BOSNIA AND HERZEGOVINA",
  "BW": "BOTSWANA",
  "BV": "BOUVET ISLAND",
  "BR": "BRAZIL",
  "IO": "BRITISH INDIAN OCEAN TERRITORY",
  "BN": "BRUNEI DARUSSALAM",
  "BG": "BULGARIA",
  "BF": "BURKINA FASO",
  "BI": "BURUNDI",
  "CV": "CABO VERDE",
  "KH": "CAMBODIA",
  "CM": "CAMEROON",
  "CA": "CANADA",
  "KY": "CAYMAN ISLANDS",
  "CF": "CENTRAL AFRICAN REPUBLIC",
  "TD": "CHAD",
  "CL": "CHILE",
  "CN": "CHINA",
  "CX": "CHRISTMAS ISLAND",
  "CC": "COCOS ISLANDS",
  "CO": "COLOMBIA",
  "KM": "COMOROS",
  "CD": "CONGO, DEMOCRATIC REPUBLIC",
  "CG": "CONGO, REPUBLIC",
  "CK": "COOK ISLANDS",
  "CR": "COSTA RICA",
  "HR": "CROATIA",
  "CU": "CUBA",
  "CW": "CURACAO",
  "CY": "CYPRUS",
  "CZ": "CZECHIA",
  "CI": "COTE D'IVOIRE",
  "DK": "DENMARK",
  "DJ": "DJIBOUTI",
  "DM": "DOMINICA",
  "DO": "DOMINICAN REPUBLIC",
  "EC": "ECUADOR",
  "EG": "EGYPT",
  "SV": "EL SALVADOR",
  "GQ": "EQUATORIAL GUINEA",
  "ER": "ERITREA",
  "EE": "ESTONIA",
  "SZ": "ESWATINI",
  "ET": "ETHIOPIA",
  "FK": "FALKLAND ISLANDS",
  "FO": "FAROE ISLANDS",
  "FJ": "FIJI",
  "FI": "FINLAND",
  "FR": "FRANCE",
  "GF": "FRENCH GUIANA",
  "PF": "FRENCH POLYNESIA",
  "TF": "FRENCH SOUTHERN TERRITORIES",
  "GA": "GABON",
  "GM": "GAMBIA",
  "GE": "GEORGIA",
  "DE": "GERMANY",
  "GH": "GHANA",
  "GI": "GIBRALTAR",
  "GR": "GREECE",
  "GL": "GREENLAND",
  "GD": "GRENADA",
  "GP": "GUADELOUPE",
  "GU": "GUAM",
  "GT": "GUATEMALA",
  "GG": "GUERNSEY",
  "GN": "GUINEA",
  "GW": "GUINEA-BISSAU",
  "GY": "GUYANA",
  "HT": "HAITI",
  "HM": "HEARD ISLAND AND MCDONALD ISLANDS",
  "VA": "VATICAN CITY / HOLY SEE",
  "HN": "HONDURAS",
  "HK": "HONG KONG",
  "HU": "HUNGARY",
  "IS": "ICELAND",
  "IN": "INDIA",
  "ID": "INDONESIA",
  "IR": "IRAN",
  "IQ": "IRAQ",
  "IE": "IRELAND",
  "IM": "ISLE OF MAN",
  "IL": "ISRAEL",
  "IT": "ITALY",
  "JM": "JAMAICA",
  "JP": "JAPAN",
  "JE": "JERSEY",
  "JO": "JORDAN",
  "KZ": "KAZAKHSTAN",
  "KE": "KENYA",
  "KI": "KIRIBATI",
  "KP": "KOREA (THE DEMOCRATIC PEOPLE'S REPUBLIC OF)",
  "KR": "KOREA (REPUBLIC OF)",
  "KW": "KUWAIT",
  "KG": "KYRGYZSTAN",
  "LA": "LAO PEOPLE'S DEMOCRATIC REPUBLIC",
  "LV": "LATVIA",
  "LB": "LEBANON",
  "LS": "LESOTHO",
  "LR": "LIBERIA",
  "LY": "LIBYA",
  "LI": "LIECHTENSTEIN",
  "LT": "LITHUANIA",
  "LU": "LUXEMBOURG",
  "MO": "MACAO",
  "MG": "MADAGASCAR",
  "MW": "MALAWI",
  "MY": "MALAYSIA",
  "MV": "MALDIVES",
  "ML": "MALI",
  "MT": "MALTA",
  "MH": "MARSHALL ISLANDS",
  "MQ": "MARTINIQUE",
  "MR": "MAURITANIA",
  "MU": "MAURITIUS",
  "YT": "MAYOTTE",
  "MX": "MEXICO",
  "FM": "MICRONESIA",
  "MD": "MOLDOVA",
  "MC": "MONACO",
  "MN": "MONGOLIA",
  "ME": "MONTENEGRO",
  "MS": "MONTSERRAT",
  "MA": "MOROCCO",
  "MZ": "MOZAMBIQUE",
  "MM": "MYANMAR",
  "NA": "NAMIBIA",
  "NR": "NAURU",
  "NP": "NEPAL",
  "NL": "NETHERLANDS",
  "NC": "NEW CALEDONIA",
  "NZ": "NEW ZEALAND",
  "NI": "NICARAGUA",
  "NE": "NIGER",
  "NG": "NIGERIA",
  "NU": "NIUE",
  "NF": "NORFOLK ISLAND",
  "MK": "NORTH MACEDONIA",
  "MP": "NORTHERN MARIANA ISLANDS",
  "NO": "NORWAY",
  "OM": "OMAN",
  "PK": "PAKISTAN",
  "PW": "PALAU",
  "PS": "WEST BANK / GAZA STRIP / PNA",
  "PA": "PANAMA",
  "PG": "PAPUA NEW GUINEA",
  "PY": "PARAGUAY",
  "PE": "PERU",
  "PH": "PHILIPPINES",
  "PN": "PITCAIRN",
  "PL": "POLAND",
  "PT": "PORTUGAL",
  "PR": "PUERTO RICO",
  "QA": "QATAR",
  "RO": "ROMANIA",
  "RU": "RUSSIAN FEDERATION",
  "RW": "RWANDA",
  "RE": "REUNION",
  "BL": "SAINT BARTHELEMY",
  "SH": "SAINT HELENA: ASCENSION AND TRISTAN DA CUNHA",
  "KN": "SAINT KITTS AND NEVIS",
  "LC": "SAINT LUCIA",
  "MF": "SAINT MARTIN (FRENCH PART)",
  "PM": "SAINT PIERRE AND MIQUELON",
  "VC": "SAINT VINCENT AND THE GRENADINES",
  "WS": "SAMOA",
  "SM": "SAN MARINO",
  "ST": "SAO TOME AND PRINCIPE",
  "SA": "SAUDI ARABIA",
  "SN": "SENEGAL",
  "RS": "SERBIA",
  "SC": "SEYCHELLES",
  "SL": "SIERRA LEONE",
  "SG": "SINGAPORE",
  "SX": "SINT MAARTEN (DUTCH PART)",
  "SK": "SLOVAKIA",
  "SI": "SLOVENIA",
  "SB": "SOLOMON ISLANDS",
  "SO": "SOMALIA",
  "ZA": "SOUTH AFRICA",
  "GS": "SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS",
  "SS": "SOUTH SUDAN",
  "ES": "SPAIN",
  "LK": "SRI LANKA",
  "SD": "SUDAN",
  "SR": "SURINAME",
  "SJ": "SVALBARD AND JAN MAYEN",
  "SE": "SWEDEN",
  "CH": "SWITZERLAND",
  "SY": "SYRIA",
  "TW": "TAIWAN",
  "TJ": "TAJIKISTAN",
  "TZ": "TANZANIA",
  "TH": "THAILAND",
  "TL": "TIMOR-LESTE",
  "TG": "TOGO",
  "TK": "TOKELAU",
  "TO": "TONGA",
  "TT": "TRINIDAD AND TOBAGO",
  "TN": "TUNISIA",
  "TM": "TURKMENISTAN",
  "TC": "TURKS AND CAICOS ISLANDS",
  "TV": "TUVALU",
  "TR": "TURKEY",
  "UG": "UGANDA",
  "UA": "UKRAINE",
  "AE": "UNITED ARAB EMIRATES",
  "GB": "UNITED KINGDOM",
  "UM": "UNITED STATES MINOR OUTLYING ISLANDS",
  "US": "UNITED STATES OF AMERICA",
  "UY": "URUGUAY",
  "UZ": "UZBEKISTAN",
  "VU": "VANUATU",
  "VE": "VENEZUELA",
  "VN": "VIET NAM",
  "VG": "VIRGIN ISLANDS (BRITISH)",
  "VI": "VIRGIN ISLANDS (U.S.)",
  "WF": "WALLIS AND FUTUNA",
  "EH": "WESTERN SAHARA",
  "YE": "YEMEN",
  "ZM": "ZAMBIA",
  "ZW": "ZIMBABWE",
  "DD": "GERMANY",
  "SU": "UNION OF SOVIET SOCIALIST REPUBLICS",
  "GC": "PATENT OFFICE FOR ARAB STATES OF THE GULF (GCCPO)",
  "AP": "ARMED FORCES IN THE PACIFIC",
  "PX": "PALESTINIAN TERRITORY",
  "EU": "EUROPEAN UNION",
  "OA": "AFRICAN INTELLECTUAL PROPERTY ORGANIZATION (OAPI)",
  "CS": "SERBIA AND MONTENEGRO",
  "AN": "NETHERLAND ANTILLES",
  "ZZ": "OTHER"
}

ctry_lst = []
for i in ctry_cd_dct.items():
    ctry_lst.append(i)

df_ctry = spark.createDataFrame(ctry_lst, schema=StructType([StructField('country_cd', StringType()), StructField('country_or_area_name', StringType())]))

# COMMAND ----------

# DBTITLE 1,Country Code Clean
df_country_clean = input_1.withColumn("COUNTRY_CD", upper(col("COUNTRY_CD")))

df_country_clean = df_country_clean.withColumn("COUNTRY_CD",
when(col("COUNTRY_CD") == "XP", "ZZ")
.when(col("COUNTRY_CD") == "XOX", "ZZ")
.when(col("COUNTRY_CD") == "RH", "ZW")
.when(col("COUNTRY_CD") == "FQX", "ZZ")
.when(col("COUNTRY_CD") == "PXX", "ZZ")
.when(col("COUNTRY_CD") == "JS", "ZZ")
.when(col("COUNTRY_CD") == "JX", "ZZ")
.when(col("COUNTRY_CD") == "NH", "VU")
.when(col("COUNTRY_CD") == "TP", "TL")
.when(col("COUNTRY_CD") == "ZR", "CD")
.when(col("COUNTRY_CD") == "EP", "EU")
.when(col("COUNTRY_CD") == "EA", "ZZ")
.when(col("COUNTRY_CD") == "EN", "ZZ")
.when(col("COUNTRY_CD") == "HV", "BF")
.when(col("COUNTRY_CD") == "LQX", "ZZ")
.when(col("COUNTRY_CD") == "XPX", "ZZ")
.when(col("COUNTRY_CD") == "", lit(None))
.when(col("COUNTRY_CD").isNull(), lit(None))
.otherwise(col("COUNTRY_CD"))
)

# COMMAND ----------

# DBTITLE 1,Country Name Cleaning
from pyspark.sql.functions import create_map, lit, col

# Flatten the dictionary into a list of key-value pairs for create_map
mapping_expr = create_map([lit(x) for x in sum(ctry_cd_dct.items(), ())])

# Replace CTRY_NM based on country_cd using the mapping
df_country_clean = df_country_clean.withColumn(
    "CTRY_NM",
    mapping_expr[col("country_cd")]
)

# COMMAND ----------

# DBTITLE 1,Country or Area Name Clean
df_215 = df_country_clean.join(df_ctry, "country_cd", "left").withColumn(
    'country_or_area_name', when(col('country_or_area_name').isNull(), col("CTRY_NM")).otherwise(col('country_or_area_name'))
)

# COMMAND ----------

invalid_country_codes = ['XK', 'EM', 'WO', 'YU', 'CS', 'FX', 'QP', 'PZ', 'BX', 'ZZ', 'OO', 'BC']

invalid_countries = df_215.filter(col('country_cd').isin(invalid_country_codes))

countries = df_215.filter(~col('country_cd').isin(invalid_country_codes) | (col('country_cd').isNull())).withColumn(
    'country_or_area_name', when(upper(col('country_or_area_name')).isin(['NOT PROVIDED', 'NOT IN LIST', 'OTHER', 'STATELESS']), lit(None)).otherwise(col('country_or_area_name'))
).withColumn(
    'country_or_area_name', initcap(trim(col('country_or_area_name')))
)

# COMMAND ----------

from functools import reduce

# clean up after title casing
clean_dct = {
    'Of': 'of',
    'And': 'and',
    'Timor-leste': 'Timor-Leste',
    'Myanmar': 'MyanMar',
    "D'ivoire": "d'Ivoire",
    'Pdr': 'PDR',
    'The': 'the',
    'dutch': 'Dutch',
    'british': 'British',
    'U.s.s.r': 'U.S.S.R',
    'keeling': 'Keeling',
    'malvinas': 'Malvinas',
    'St.helena': 'St.Helena',
    'oapi': 'OAPI',
    'gcpo': 'GCCPO',
    'epo': 'Epo'
}

replace_expr = reduce(
    lambda a, b: regexp_replace(a, rf"\b{b[0]}\b", b[1]),
    clean_dct.items(),
    col("country_or_area_name")
)

countries = countries.withColumn("country_or_area_name", replace_expr)

# COMMAND ----------

addresses_raw = (
    input_2
    .select(
        col("PY_ADDR_1"),
        col("PY_ADDR_2"),
        col("PY_FLG_ADDR_1"),
        col("PY_FLG_ADDR_2"),
        col("PY_PARTY_TYPE"),
        col("PY_SER_NUM").alias("SERIAL_NUMBER"),
        col("PY_ENT_NUM"),
        col("LAST_MODIFIED_DATE").alias("LAST_MODIFIED_DT")# Review_comment: do not see this alais in Alteryx wf
        #input5 has last_modified_date column with hardcode value (''), there is join input 5 dataframe with this input2 #dataframe in cmd10 so to pull both dates, rename is done in this code. Further we are pulling only one last modified #date in cmd11. As per the code in altryx, we have to pull date from input 2 instead of input 5.
    )
)

# COMMAND ----------

addresses = addresses_raw.select(
    col("SERIAL_NUMBER").alias("ADDRESS_SERIAL_NUMBER"),
    col("PY_PARTY_TYPE").alias("ADDRESS_PY_PARTY_TYPE"),
    col("PY_ADDR_1").alias("ADDRESS_1"),
    col("PY_ADDR_2").alias("ADDRESS_2"),
    col("PY_ENT_NUM").alias("ADDRESS_PY_ENT_NUM"),
    col("LAST_MODIFIED_DT").alias("LAST_MODIFIED_DATE"),  ## I have added this alias
)
# .withColumn(
#         "ADDRESS_2", 
#         concat(regexp_extract("ADDRESS_2", r"(.*\\l)(\\u.*)", 1), regexp_extract("ADDRESS_2", r"(.*\\l)(\\u.*)", 2)))

# COMMAND ----------

# MAGIC %md
# MAGIC # City, address1, address2
# MAGIC

# COMMAND ----------

names = input_3.select(
    col("PY_SER_NUM").alias("NAME_SERIAL_NUMBER"),
    col("PY_NAM_1").alias("NAME"),
    col("PY_ENT_NUM").alias("NAME_PY_ENT_NUM"),
    col("PY_PARTY_TYPE").alias("NAME_PY_PARTY_TYPE"),
)

# COMMAND ----------

# join owners to their addresses
names_and_addresses = (
    addresses
    .join(
        other = names,
        on = [
            addresses.ADDRESS_SERIAL_NUMBER == names.NAME_SERIAL_NUMBER,
            addresses.ADDRESS_PY_PARTY_TYPE == names.NAME_PY_PARTY_TYPE,
            addresses.ADDRESS_PY_ENT_NUM == names.NAME_PY_ENT_NUM
        ],
        how = "inner"
    )
)

# COMMAND ----------

# join address, name, and country info for each owner
final_join =  (
    countries
    .join(
        other = names_and_addresses,
        on = [
            countries.PY_SER_NUM == names_and_addresses.ADDRESS_SERIAL_NUMBER,
            countries.PY_PARTY_TYPE == names_and_addresses.ADDRESS_PY_PARTY_TYPE,
            countries.PY_ENT_NUM == names_and_addresses.ADDRESS_PY_ENT_NUM,
        ],
        how = "inner"
    )
).select(
        col("ADDRESS_SERIAL_NUMBER").alias("SERIAL_NUMBER"),
        col("ADDRESS_PY_PARTY_TYPE").alias("PARTY_TYPE"),
        col("NAME"),
        col("PY_ENT_NUM"), ## Added later to do the oderby on Owner Dataframe
        col("ADDRESS_1"),
        col("ADDRESS_2"),
        col("PY_CITY").alias("CITY"),
        col("PY_ZIP_CD").alias("POSTAL_CD"),
        col("PY_CITIZENSHIP").alias("CITIZENSHIP"),
        col("PY_ENTITY_TYPE").alias("ENTITY_TYPE"),
        col("CTRY_NM"),
        col("COUNTRY_CD").alias('CTRY_CD'),
        col("country_or_area_name"),
        col("LAST_MODIFIED_DATE"),
        col("STATE_CD")
    )

# COMMAND ----------

# determine the current owner using the highest party type number (interested_party sequence no)
max_party_lookup = (
    final_join.groupBy(col("SERIAL_NUMBER")).agg(
        max(col("Party_Type")).alias("Max_PARTY_TYPE")
    )
).withColumn("Current_Owner", lit("Y")).select(col("SERIAL_NUMBER").alias("MAX_SERIAL_NUMBER"), col("Max_PARTY_TYPE"), col("Current_Owner"))

# COMMAND ----------

# join the current owner lookup to the existing dataframe to determine which owner is current (per max party type no)
current_owner = (
    final_join
    .join(
        other = max_party_lookup,
        on = [
            final_join.SERIAL_NUMBER == max_party_lookup.MAX_SERIAL_NUMBER,
            final_join.PARTY_TYPE == max_party_lookup.Max_PARTY_TYPE
        ],
        how = "left"
    )
)

# COMMAND ----------

# determine owner num by sequentially ordering by owner party_type per serial_number
partition = Window.partitionBy("SERIAL_NUMBER", "PARTY_TYPE").orderBy("PY_ENT_NUM")

owner_num = (
    current_owner
    .withColumn(
        "Owner_Num",
        row_number().over(partition)
    )
)

# COMMAND ----------

# select emails where the email isn't an attorney, a correspondent, or a domestic rep
email_raw = (
    input_4
    .select(
        col("VT_TEXT_TYPE"),
        col("VT_TEXT"),
        col("VT_SER_NUM"),
        col("VT_ENT_NUM"),
    )
).where(
        ~(col("VT_TEXT_TYPE").contains("EMAT"))
        & ~(col("VT_TEXT_TYPE").contains("EMCR"))
        & ~(col("VT_TEXT_TYPE").contains("EMDR"))
    )

# COMMAND ----------

em_win = Window().partitionBy("VT_SER_NUM", "VT_TEXT_TYPE").orderBy("VT_ENT_NUM", "VT_TEXT")
rank_win = Window().partitionBy("VT_SER_NUM", "VT_TEXT_TYPE").orderBy(col("VT_ENT_NUM").desc(), col("VT_TEXT").desc())

# COMMAND ----------

# email_sort= email_raw.orderBy(col("VT_SER_NUM"),col("VT_TEXT_TYPE"),col("VT_ENT_NUM"))

# COMMAND ----------

# concatente emails in order
collect_email = email_raw.withColumn(
    "VT_TEXT", concat_ws("", collect_list(col("VT_TEXT")).over(em_win))
).withColumn(
    "rank", row_number().over(rank_win)
)

collect_email = collect_email.filter(col("rank") == 1).drop("rank")

# COMMAND ----------

# # collect emails
# collect_email = (
#     email_raw
#     .groupBy(
#         col("VT_SER_NUM"),
#         col("VT_TEXT_TYPE")
#     )
#     .agg(
#         concat_ws(
#             "",
#             collect_list(col("VT_TEXT"))
#         ).alias("VT_TEXT")
#     )
# )

# COMMAND ----------

email = (
    collect_email.withColumn(
        "OWNER_EMAIL_AUTH", regexp_extract(col("VT_TEXT"), r"(?i)([Y|N])(.+[@].+[.].+)", 1)
    ).withColumn(
        "OWNER_EMAIL", regexp_extract(col("VT_TEXT"), r"(?i)([Y|N])(.+[@].+[.].+)", 2)
    )
    # EM4501 -> 45
    .withColumn(
        "EMAIL_PARTY_TYPE", 
        # regexp_extract(col("VT_TEXT_TYPE"), r"(\\d\\d)(\\d\\d)", 1)
        substring(col("VT_TEXT_TYPE"), 3, 2).cast(IntegerType())
    )
    # EM4501 -> 01
    .withColumn(
        "EMAIL_OWNER_NUMBER",
        substring(col("VT_TEXT_TYPE"), 5, 2).cast(IntegerType())
        # regexp_extract(
        #     col("VT_TEXT_TYPE"),
        #     r"(\\d\\d)(\\d\\d)",
        #     2
        # )
    )
)

# COMMAND ----------

# replace empty strings with null
email = email.withColumn("owner_email", when(col("owner_email") == '', lit(None)).otherwise(col("owner_email")))

# COMMAND ----------

# join owner name, address, num, and country to email info
final_output = (
    owner_num
    .join(
        other = email,
        on = [
            owner_num.SERIAL_NUMBER == email.VT_SER_NUM,
            owner_num.PARTY_TYPE == email.EMAIL_PARTY_TYPE,
            owner_num.Owner_Num == email.EMAIL_OWNER_NUMBER
        ],
        how = "left" 
    )
)

# COMMAND ----------

cleansed = (
    final_output
    .select(
        col("SERIAL_NUMBER").alias("ser_num"),
        col("current_owner"),
        col("party_type"),
        col("name").cast(StringType()),
        col("address_1"),
        col("address_2").cast(StringType()),
        col("city"),
        col("postal_cd"),
        col("citizenship"),
        col("entity_type"),
        col("ctry_nm"),
        col("ctry_cd"),
        col("country_or_area_name"),
        col("last_modified_date").cast(TimestampType()),
        col("state_cd"),
        col("max_party_type"),
        col("owner_num"),
        col("OWNER_EMAIL")
    )
    .withColumn("create_ts", current_timestamp())
    .withColumn("create_user_id", lit("-1"))
    .withColumn("update_ts", current_timestamp())
    .withColumn("update_user_id", lit("-1"))
)

# COMMAND ----------

invalid_countries = invalid_countries.select(
    col("py_ser_num").alias("ser_num"),
    col("py_city").alias("city"),
    col("country_cd").alias("ctry_cd"),
    col("py_zip_cd").alias("postal_cd"),
    'ctry_nm',
    'country_or_area_name',
    'state_cd',
    col('py_citizenship').alias("citizenship"),
    "py_ent_num",
    "py_entity_type",
    "py_party_type"
).withColumn(
    "create_ts", current_timestamp()
).withColumn(
    "create_user_id", lit("-1")
).withColumn(
    "update_ts", current_timestamp()
).withColumn(
    "update_user_id", lit("-1")
)

# COMMAND ----------

# reverse prior fillna operations (presumably done to preserve nulls after join)
cleansed = cleansed.replace('null', None, subset=['ctry_cd'])
cleansed = cleansed.replace('', None, subset=['ctry_cd'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write data in dataframe

# COMMAND ----------

print(reporting_catalog,schema_silver,table_silver)

# COMMAND ----------

# write to main table
cleansed.write.mode("overwrite").format("delta").saveAsTable(f'{reporting_catalog}.{schema_silver}.{table_silver}')

# write to error table
invalid_countries.write.mode("overwrite").format("delta").saveAsTable(f'{reporting_catalog}.{schema_silver}.owner_invalid_countries')
