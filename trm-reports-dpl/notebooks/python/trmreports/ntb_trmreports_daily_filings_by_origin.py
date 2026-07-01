# Databricks notebook source
# DBTITLE 1,Import
from typing import Final, Dict
import requests
import time
from datetime import datetime, timedelta
import io
import pandas as pd
from zoneinfo import ZoneInfo

# COMMAND ----------

# DBTITLE 1,Environment Settings
dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# DBTITLE 1,Import Shared Functions
# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

# DBTITLE 1,Set Configuration
common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]

print(reporting_catalog, tmngpdb_catalog)

# COMMAND ----------

# DBTITLE 1,Globals
ENGINE_OPTIONS: Final = {
    "options": {
        "strings_to_urls": False,
        "strings_to_formulas": False,
    }
}
send_from: str = "trademark_analytics@uspto.gov"
send_to: str = "benjamin.fielstra@uspto.gov"
send_to_cc: str = "benjamin.fielstra@uspto.gov"
subject: str = "Daily Filings By Origin Report"
attachment_name: str = "Daily Filings By Origin Report.xlsx"
server: str = "mailer.uspto.gov"

# COMMAND ----------

# DBTITLE 1,Begin Job
job_name = "ntb_trmreports_daily_filings_by_origin"
# control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# DBTITLE 1,Declare Year Cutoff and Lookback
# MAGIC %sql
# MAGIC declare or replace variable num_years int default 5;
# MAGIC
# MAGIC declare or replace variable decimal_places int default 3;
# MAGIC
# MAGIC declare or replace variable current_fy_start date = case
# MAGIC   when month(current_date) >= 10 then make_date(year(current_date), 10, 1)
# MAGIC   else make_date(year(current_date) - 1, 10, 1)
# MAGIC end;
# MAGIC
# MAGIC declare or replace variable current_fy = year(current_fy_start) + 1;
# MAGIC
# MAGIC declare or replace variable current_fy_end date = make_date(current_fy, 9, 30);
# MAGIC
# MAGIC declare or replace variable previous_fy = current_fy - 1;
# MAGIC
# MAGIC declare or replace variable previous_fy_start date = current_fy_start - interval 1 year;
# MAGIC
# MAGIC declare or replace variable previous_fy_end date = current_fy_start - interval 1 day;
# MAGIC
# MAGIC declare or replace variable cutoff date = make_date(current_fy - num_years - 1, 10, 1);
# MAGIC
# MAGIC declare or replace variable drop_column int = year(cutoff);
# MAGIC
# MAGIC select
# MAGIC   num_years,
# MAGIC   previous_fy,
# MAGIC   current_fy,
# MAGIC   previous_fy_start,
# MAGIC   previous_fy_end,
# MAGIC   current_fy_start,
# MAGIC   current_fy_end,
# MAGIC   cutoff,
# MAGIC   drop_column

# COMMAND ----------

# DBTITLE 1,Region Data
# MAGIC %sql
# MAGIC create or replace temp view regions as
# MAGIC with africa as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("AFRICA", "AFRICA", "ALGERIA"),
# MAGIC       ("AFRICA", "AFRICA", "ANGOLA"),
# MAGIC       ("AFRICA", "AFRICA", "BENIN"),
# MAGIC       ("AFRICA", "AFRICA", "BOTSWANA"),
# MAGIC       ("AFRICA", "AFRICA", "BURKINA FASO"),
# MAGIC       ("AFRICA", "AFRICA", "BURUNDI"),
# MAGIC       ("AFRICA", "AFRICA", "CABO VERDE"),
# MAGIC       ("AFRICA", "AFRICA", "CAMEROON"),
# MAGIC       ("AFRICA", "AFRICA", "CENTRAL AFRICAN REPUBLIC"),
# MAGIC       ("AFRICA", "AFRICA", "CHAD"),
# MAGIC       ("AFRICA", "AFRICA", "COMOROS"),
# MAGIC       ("AFRICA", "AFRICA", "CONGO, REPUBLIC OF"),
# MAGIC       ("AFRICA", "AFRICA", "CONGO"),
# MAGIC       ("AFRICA", "AFRICA", "CÔTE D'IVOIRE"),
# MAGIC       ("AFRICA", "AFRICA", "COTE D'IVOIRE"),
# MAGIC       ("AFRICA", "AFRICA", "DJIBOUTI"),
# MAGIC       ("AFRICA", "AFRICA", "EGYPT"),
# MAGIC       ("AFRICA", "AFRICA", "EQUATORIAL GUINEA"),
# MAGIC       ("AFRICA", "AFRICA", "ERITREA"),
# MAGIC       ("AFRICA", "AFRICA", "ESWATINI"),
# MAGIC       ("AFRICA", "AFRICA", "ETHIOPIA"),
# MAGIC       ("AFRICA", "AFRICA", "GABON"),
# MAGIC       ("AFRICA", "AFRICA", "GAMBIA"),
# MAGIC       ("AFRICA", "AFRICA", "GHANA"),
# MAGIC       ("AFRICA", "AFRICA", "GUINEA"),
# MAGIC       ("AFRICA", "AFRICA", "GUINEA-BISSAU"),
# MAGIC       ("AFRICA", "AFRICA", "IVORY COAST"),
# MAGIC       ("AFRICA", "AFRICA", "KENYA"),
# MAGIC       ("AFRICA", "AFRICA", "LESOTHO"),
# MAGIC       ("AFRICA", "AFRICA", "LIBERIA"),
# MAGIC       ("AFRICA", "AFRICA", "LIBYA"),
# MAGIC       ("AFRICA", "AFRICA", "MADAGASCAR"),
# MAGIC       ("AFRICA", "AFRICA", "MALAWI"),
# MAGIC       ("AFRICA", "AFRICA", "MALI"),
# MAGIC       ("AFRICA", "AFRICA", "MAURITANIA"),
# MAGIC       ("AFRICA", "AFRICA", "MAURITIUS"),
# MAGIC       ("AFRICA", "AFRICA", "MAYOTTE"),
# MAGIC       ("AFRICA", "AFRICA", "MOROCCO"),
# MAGIC       ("AFRICA", "AFRICA", "MOZAMBIQUE"),
# MAGIC       ("AFRICA", "AFRICA", "NAMIBIA"),
# MAGIC       ("AFRICA", "AFRICA", "NIGER"),
# MAGIC       ("AFRICA", "AFRICA", "NIGERIA"),
# MAGIC       ("AFRICA", "AFRICA", "REUNION"),
# MAGIC       ("AFRICA", "AFRICA", "RWANDA"),
# MAGIC       ("AFRICA", "AFRICA", "SAO TOME AND PRINCIPE"),
# MAGIC       ("AFRICA", "AFRICA", "SENEGAL"),
# MAGIC       ("AFRICA", "AFRICA", "SEYCHELLES"),
# MAGIC       ("AFRICA", "AFRICA", "SIERRA LEONE"),
# MAGIC       ("AFRICA", "AFRICA", "SOMALIA"),
# MAGIC       ("AFRICA", "AFRICA", "SOUTH AFRICA"),
# MAGIC       ("AFRICA", "AFRICA", "SOUTH SUDAN"),
# MAGIC       ("AFRICA", "AFRICA", "SUDAN"),
# MAGIC       ("AFRICA", "AFRICA", "TANZANIA"),
# MAGIC       ("AFRICA", "AFRICA", "TANZANIA, UNITED REPUBLIC OF"),
# MAGIC       ("AFRICA", "AFRICA", "TOGO"),
# MAGIC       ("AFRICA", "AFRICA", "TUNISIA"),
# MAGIC       ("AFRICA", "AFRICA", "UGANDA"),
# MAGIC       ("AFRICA", "AFRICA", "WESTERN SAHARA"),
# MAGIC       ("AFRICA", "AFRICA", "ZAMBIA"),
# MAGIC       ("AFRICA", "AFRICA", "ZIMBABWE")
# MAGIC ),
# MAGIC asia as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("ASIA", "ASIA", "AFGHANISTAN"),
# MAGIC       ("ASIA", "ASIA", "ARMENIA"),
# MAGIC       ("ASIA", "ASIA", "AZERBAIJAN"),
# MAGIC       ("ASIA", "ASIA", "BAHRAIN"),
# MAGIC       ("ASIA", "ASIA", "BANGLADESH"),
# MAGIC       ("ASIA", "ASIA", "BHUTAN"),
# MAGIC       ("ASIA", "ASIA", "BRUNEI DARUSSALAM"),
# MAGIC       ("ASIA", "ASIA", "CAMBODIA"),
# MAGIC       ("CHINA", "CHINA", "CHINA"),
# MAGIC       ("ASIA", "ASIA", "CYPRUS"),
# MAGIC       ("ASIA", "ASIA", "GEORGIA"),
# MAGIC       ("ASIA", "ASIA", "HONG KONG"),
# MAGIC       ("ASIA", "ASIA", "HONG KONG SPECIAL ADMINISTRATIVE REGION OF THE PEOPLE'S REPUBLIC OF CHINA"),
# MAGIC       (
# MAGIC         "ASIA",
# MAGIC         "ASIA",
# MAGIC         "THE HONG KONG SPECIAL ADMINISTRATIVE REGION OF THE PEOPLE'S REPUBLIC OF CHINA"
# MAGIC       ),
# MAGIC       ("ASIA", "ASIA", "INDIA"),
# MAGIC       ("ASIA", "ASIA", "INDONESIA"),
# MAGIC       ("ASIA", "ASIA", "IRAN"),
# MAGIC       ("ASIA", "ASIA", "IRAN, ISLAMIC REPUBLIC OF"),
# MAGIC       ("ASIA", "ASIA", "IRAQ"),
# MAGIC       ("ASIA", "ASIA", "ISRAEL"),
# MAGIC       ("ASIA", "ASIA", "JAPAN"),
# MAGIC       ("ASIA", "ASIA", "JORDAN"),
# MAGIC       ("ASIA", "ASIA", "KAZAKHSTAN"),
# MAGIC       ("ASIA", "ASIA", "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF"),
# MAGIC       ("ASIA", "ASIA", "KOREA, REPUBLIC OF"),
# MAGIC       ("ASIA", "ASIA", "KOREA, SOUTH"),
# MAGIC       ("ASIA", "ASIA", "KYRGYZSTAN"),
# MAGIC       ("ASIA", "ASIA", "LAO PEOPLE'S DEMOCRATIC REPUBLIC"),
# MAGIC       ("ASIA", "ASIA", "LAOS"),
# MAGIC       ("ASIA", "ASIA", "LEBANON"),
# MAGIC       ("ASIA", "ASIA", "MACAO"),
# MAGIC       ("ASIA", "ASIA", "MACAU"),
# MAGIC       ("ASIA", "ASIA", "MALAYSIA"),
# MAGIC       ("ASIA", "ASIA", "MALDIVES"),
# MAGIC       ("ASIA", "ASIA", "MONGOLIA"),
# MAGIC       ("ASIA", "ASIA", "MYANMAR"),
# MAGIC       ("ASIA", "ASIA", "NEPAL"),
# MAGIC       ("ASIA", "ASIA", "NORTH KOREA"),
# MAGIC       ("ASIA", "ASIA", "OMAN"),
# MAGIC       ("ASIA", "ASIA", "PAKISTAN"),
# MAGIC       ("ASIA", "ASIA", "PALESTINE, STATE OF"),
# MAGIC       ("ASIA", "ASIA", "PALESTINIAN TERRITORY, OCCUPIED"),
# MAGIC       ("ASIA", "ASIA", "PHILIPPINES"),
# MAGIC       ("ASIA", "ASIA", "QATAR"),
# MAGIC       ("ASIA", "ASIA", "REPUBLIC OF KOREA"),
# MAGIC       ("ASIA", "ASIA", "SAUDI ARABIA"),
# MAGIC       ("ASIA", "ASIA", "SINGAPORE"),
# MAGIC       ("ASIA", "ASIA", "SRI LANKA"),
# MAGIC       ("ASIA", "ASIA", "SYRIA"),
# MAGIC       ("ASIA", "ASIA", "SYRIAN ARAB REPUBLIC"),
# MAGIC       ("ASIA", "ASIA", "TAIWAN"),
# MAGIC       ("ASIA", "ASIA", "TAJIKISTAN"),
# MAGIC       ("ASIA", "ASIA", "THAILAND"),
# MAGIC       ("ASIA", "ASIA", "TIMOR-LESTE"),
# MAGIC       ("ASIA", "ASIA", "TÜRKİYE"),
# MAGIC       ("ASIA", "ASIA", "TÜRKİYE"),
# MAGIC       ("ASIA", "ASIA", "TURKMENISTAN"),
# MAGIC       ("ASIA", "ASIA", "UNITED ARAB EMIRATES"),
# MAGIC       ("ASIA", "ASIA", "UZBEKISTAN"),
# MAGIC       ("ASIA", "ASIA", "VIET NAM"),
# MAGIC       ("ASIA", "ASIA", "VIETNAM"),
# MAGIC       ("ASIA", "ASIA", "WEST BANK/GAZA"),
# MAGIC       ("ASIA", "ASIA", "YEMEN")
# MAGIC ),
# MAGIC europe as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("EUROPE", "EUROPE", "ALAND ISLANDS"),
# MAGIC       ("EUROPE", "EUROPE", "ALBANIA"),
# MAGIC       ("EUROPE", "EUROPE", "ANDORRA"),
# MAGIC       ("EUROPE", "EUROPE", "AUSTRIA"),
# MAGIC       ("EUROPE", "EUROPE", "BELARUS"),
# MAGIC       ("EUROPE", "EUROPE", "BELGIUM"),
# MAGIC       ("EUROPE", "EUROPE", "BOSNIA AND HERZEGOVINA"),
# MAGIC       ("EUROPE", "EUROPE", "BULGARIA"),
# MAGIC       ("EUROPE", "EUROPE", "CROATIA"),
# MAGIC       ("EUROPE", "EUROPE", "CZECH REPUBLIC"),
# MAGIC       ("EUROPE", "EUROPE", "CZECHIA"),
# MAGIC       ("EUROPE", "EUROPE", "DENMARK"),
# MAGIC       ("EUROPE", "EUROPE", "ESTONIA"),
# MAGIC       ("EUROPE", "EUROPE", "FAROE ISLANDS"),
# MAGIC       ("EUROPE", "EUROPE", "FED REP GERMANY"),
# MAGIC       ("EUROPE", "EUROPE", "FINLAND"),
# MAGIC       ("EUROPE", "EUROPE", "FRANCE"),
# MAGIC       ("EUROPE", "EUROPE", "GERMAN DEMOCRATIC REPUBLIC"),
# MAGIC       ("EUROPE", "EUROPE", "GERMANY"),
# MAGIC       ("EUROPE", "EUROPE", "GIBRALTAR"),
# MAGIC       ("EUROPE", "EUROPE", "GREECE"),
# MAGIC       ("EUROPE", "EUROPE", "GUERNSEY"),
# MAGIC       ("EUROPE", "EUROPE", "HUNGARY"),
# MAGIC       ("EUROPE", "EUROPE", "ICELAND"),
# MAGIC       ("EUROPE", "EUROPE", "IRELAND"),
# MAGIC       ("EUROPE", "EUROPE", "ISLE OF MAN"),
# MAGIC       ("EUROPE", "EUROPE", "ITALY"),
# MAGIC       ("EUROPE", "EUROPE", "JERSEY"),
# MAGIC       ("EUROPE", "EUROPE", "KOSOVO"),
# MAGIC       ("EUROPE", "EUROPE", "LATVIA"),
# MAGIC       ("EUROPE", "EUROPE", "LIECHTENSTEIN"),
# MAGIC       ("EUROPE", "EUROPE", "LITHUANIA"),
# MAGIC       ("EUROPE", "EUROPE", "LUXEMBOURG"),
# MAGIC       ("EUROPE", "EUROPE", "MACEDONIA"),
# MAGIC       ("EUROPE", "EUROPE", "MALTA"),
# MAGIC       ("EUROPE", "EUROPE", "MOLDOVA"),
# MAGIC       ("EUROPE", "EUROPE", "MONACO"),
# MAGIC       ("EUROPE", "EUROPE", "MONTENEGRO"),
# MAGIC       ("EUROPE", "EUROPE", "NETHERLANDS"),
# MAGIC       ("EUROPE", "EUROPE", "NORTH MACEDONIA"),
# MAGIC       ("EUROPE", "EUROPE", "NORWAY"),
# MAGIC       ("EUROPE", "EUROPE", "POLAND"),
# MAGIC       ("EUROPE", "EUROPE", "PORTUGAL"),
# MAGIC       ("EUROPE", "EUROPE", "REPUBLIC MOLDOVA"),
# MAGIC       ("EUROPE", "EUROPE", "REPUBLIC OF MOLDOVA"),
# MAGIC       ("EUROPE", "EUROPE", "ROMANIA"),
# MAGIC       ("EUROPE", "EUROPE", "RUSSIA"),
# MAGIC       ("EUROPE", "EUROPE", "RUSSIAN FEDERATION"),
# MAGIC       ("EUROPE", "EUROPE", "SAN MARINO"),
# MAGIC       ("EUROPE", "EUROPE", "SERBIA"),
# MAGIC       ("EUROPE", "EUROPE", "SLOVAKIA"),
# MAGIC       ("EUROPE", "EUROPE", "SLOVENIA"),
# MAGIC       ("EUROPE", "EUROPE", "SPAIN"),
# MAGIC       ("EUROPE", "EUROPE", "SINT MAARTEN"),
# MAGIC       ("EUROPE", "EUROPE", "ST. MAARTEN"),
# MAGIC       ("EUROPE", "EUROPE", "SWEDEN"),
# MAGIC       ("EUROPE", "EUROPE", "SWITZERLAND"),
# MAGIC       ("EUROPE", "EUROPE", "THE FORMER YUGOSLAV REPUBLIC OF MACEDONIA"),
# MAGIC       ("EUROPE", "EUROPE", "TURKEY"),
# MAGIC       ("EUROPE", "EUROPE", "UKRAINE"),
# MAGIC       ("EUROPE", "EUROPE", "UNITED KINGDOM"),
# MAGIC       ("EUROPE", "EUROPE", "VATICAN CITY"),
# MAGIC       ("EUROPE", "EUROPE", "HOLY SEE (VATICAN CITY STATE)"),
# MAGIC       ("EUROPE", "EUROPE", "EUROPEAN UNION")
# MAGIC ),
# MAGIC north_america as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ANGUILLA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ANTIGUA & BARBUDA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ANTIGUA AND BARBUDA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ARUBA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BAHAMAS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BARBADOS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BELIZE"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BERMUDA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BES ISLANDS (BONAIRE, SAINT EUSTATIUS AND SABA)"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "BRITISH VIRGIN ISLANDS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "CANADA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "CAYMAN ISLANDS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "COSTA RICA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "CUBA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "CURACAO"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "CURAÇAO"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "DOMINICA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "DOMINICAN REPUBLIC"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ECUADOR"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "EL SALVADOR"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "GRENADA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "GUADELOUPE"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "GUATEMALA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "HAITI"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "HONDURAS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "JAMAICA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "MARTINIQUE"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "MEXICO"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "MONTSERRAT"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "NICARAGUA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "PANAMA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "PUERTO RICO"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "SAINT KITTS AND NEVIS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "SAINT LUCIA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "SAINT VINCENT AND GRENADINES"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "SAINT VINCENT AND THE GRENADINES"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ST. LUCIA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "ST. MAARTEN"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "SINT MAARTEN"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "TRINIDAD AND TOBAGO"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "TURKS AND CAICOS ISLANDS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "TURKS/CAICOS ISLANDS"),
# MAGIC       ("NORTH AMERICA", "UNITED STATES", "UNITED STATES"),
# MAGIC       ("NORTH AMERICA", "UNITED STATES", "UNITED STATES OF AMERICA"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "US VIRGIN ISLANDS"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "VIRGIN ISLANDS, BRITISH"),
# MAGIC       ("NORTH AMERICA", "AMERICAS", "WEST INDIES")
# MAGIC ),
# MAGIC south_america as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "ARGENTINA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "BOLIVIA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "BOLIVIA (PLURINATIONAL STATE OF)"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "BRAZIL"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "CHILE"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "COLOMBIA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "FALKLAND ISLANDS"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "FALKLAND ISLANDS (MALVINAS)"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "FRENCH GUIANA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "GUYANA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "PARAGUAY"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "PERU"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "SURINAME"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "URUGUAY"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "VENEZUELA"),
# MAGIC       ("SOUTH AMERICA", "AMERICAS", "VENEZUELA, BOLIVARIAN REPUBLIC OF")
# MAGIC ),
# MAGIC oceania as (
# MAGIC   select
# MAGIC     col1 as continent,
# MAGIC     col2 as `grouping`,
# MAGIC     col3 as country
# MAGIC   from
# MAGIC     values
# MAGIC       ("OCEANIA", "ASIA", "AUSTRALIA"),
# MAGIC       ("OCEANIA", "ASIA", "COCOS (KEELING) ISLANDS"),
# MAGIC       ("OCEANIA", "ASIA", "CHRISTMAS ISLAND"),
# MAGIC       ("OCEANIA", "ASIA", "COOK ISLANDS"),
# MAGIC       ("OCEANIA", "ASIA", "FIJI"),
# MAGIC       ("OCEANIA", "ASIA", "FRENCH POLYNESIA"),
# MAGIC       ("OCEANIA", "ASIA", "KIRIBATI"),
# MAGIC       ("OCEANIA", "ASIA", "MARSHAL ISLANDS"),
# MAGIC       ("OCEANIA", "ASIA", "MARSHALL ISLANDS"),
# MAGIC       ("OCEANIA", "ASIA", "MICRONESIA, FEDERATED STATES OF"),
# MAGIC       ("OCEANIA", "ASIA", "MIRCRONESIA"),
# MAGIC       ("OCEANIA", "ASIA", "NAURU"),
# MAGIC       ("OCEANIA", "ASIA", "NEW ZEALAND"),
# MAGIC       ("OCEANIA", "ASIA", "NIUE"),
# MAGIC       ("OCEANIA", "ASIA", "PALAU"),
# MAGIC       ("OCEANIA", "ASIA", "PAPUA NEW GUINEA"),
# MAGIC       ("OCEANIA", "ASIA", "SAMOA"),
# MAGIC       ("OCEANIA", "ASIA", "SOLOMON ISLANDS"),
# MAGIC       ("OCEANIA", "ASIA", "TUVALU"),
# MAGIC       ("OCEANIA", "ASIA", "VANUATU")
# MAGIC ),
# MAGIC origins as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     africa
# MAGIC   union
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     asia
# MAGIC   union
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     europe
# MAGIC   union
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     north_america
# MAGIC   union
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     south_america
# MAGIC   union
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     oceania
# MAGIC )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   origins;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   regions
# MAGIC order by
# MAGIC   rand()
# MAGIC limit 5;

# COMMAND ----------

# DBTITLE 1,Filing Date View
# MAGIC %sql
# MAGIC create or replace temp view filing_dates as
# MAGIC with dates as (
# MAGIC   select
# MAGIC     explode(sequence(cutoff, current_date, interval 1 day)) filing_date
# MAGIC )
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   dates join regions;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   filing_dates
# MAGIC limit 5;

# COMMAND ----------

# DBTITLE 1,Generate Daily Country Counts
spark.sql(
    f"""
  select
    distinct
    fd.ser_num case_number,
    nvl(fd.fixed_count, 0) class_count,
    m.filing_dt filing_date,
    upper(fd.ctry_nm) country
  from
    {reporting_catalog}.gold.filings_dashboard fd
    join {reporting_catalog}.silver.milestone m on fd.ser_num = m.ser_num
  where
    m.filing_dt >= cutoff
    """
).createOrReplaceTempView("countries")

spark.sql("select * from countries").limit(5).show(vertical=True, truncate=False)

# COMMAND ----------

# DBTITLE 1,Filing Aggregates Views
# MAGIC %sql
# MAGIC create or replace temp view origin_counts as
# MAGIC select distinct
# MAGIC   fd.grouping `region`,
# MAGIC   fd.filing_date,
# MAGIC   case
# MAGIC     when month(fd.filing_date) >= 10 then year(fd.filing_date) + 1
# MAGIC     else year(fd.filing_date)
# MAGIC   end `Fiscal Year`,
# MAGIC   case
# MAGIC     when month(fd.filing_date) between 10 and 12 then 1
# MAGIC     when month(fd.filing_date) between 1 and 3 then 2
# MAGIC     when month(fd.filing_date) between 4 and 6 then 3
# MAGIC     else 4
# MAGIC   end `Fiscal Year Quarter`,
# MAGIC   month(fd.filing_date) `Month`,
# MAGIC   date_format(fd.filing_date, 'MM/dd') month_day,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when cr.filing_date is not null then class_count
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) over (partition by fd.filing_date, fd.grouping) daily_counts,
# MAGIC   avg(
# MAGIC     case
# MAGIC       when
# MAGIC         case
# MAGIC           when month(fd.filing_date) between 10 and 12 then 1
# MAGIC           when month(fd.filing_date) between 1 and 3 then 2
# MAGIC           when month(fd.filing_date) between 4 and 6 then 3
# MAGIC           else 4
# MAGIC         end = 4
# MAGIC       then
# MAGIC         sum(
# MAGIC           case
# MAGIC             when cr.filing_date is not null then class_count
# MAGIC             else 0
# MAGIC           end
# MAGIC         ) over (partition by fd.filing_date, fd.grouping)
# MAGIC     end
# MAGIC   ) over (
# MAGIC       partition by
# MAGIC         fd.grouping,
# MAGIC         case
# MAGIC           when month(fd.filing_date) >= 10 then year(fd.filing_date) + 1
# MAGIC           else year(fd.filing_date)
# MAGIC         end
# MAGIC     ) fourth_quarter_average,
# MAGIC   avg(
# MAGIC     case
# MAGIC       when
# MAGIC         fd.filing_date between cutoff and previous_fy_end
# MAGIC       then
# MAGIC         sum(
# MAGIC           case
# MAGIC             when cr.filing_date is not null then class_count
# MAGIC             else 0
# MAGIC           end
# MAGIC         ) over (partition by fd.filing_date, fd.grouping)
# MAGIC     end
# MAGIC   ) over (
# MAGIC       partition by fd.grouping, month(fd.filing_date), day(fd.filing_date)
# MAGIC     ) average_historical_counts_on_this_day
# MAGIC from
# MAGIC   filing_dates fd
# MAGIC     left join countries cr
# MAGIC       on fd.country = cr.country
# MAGIC       and fd.filing_date = cr.filing_date;
# MAGIC
# MAGIC create or replace temp view daily_region_filings as
# MAGIC select distinct
# MAGIC   cr.`Fiscal Year`,
# MAGIC   cr.`Fiscal Year Quarter`,
# MAGIC   cr.`Month`,
# MAGIC   cr.`region`,
# MAGIC   cr.filing_date,
# MAGIC   cr.month_day,
# MAGIC   cr.daily_counts,
# MAGIC   cr.fourth_quarter_average,
# MAGIC   pq.fourth_quarter_average previous_fourth_quarter_average,
# MAGIC   round((pq.fourth_quarter_average + cr.average_historical_counts_on_this_day) / 2) projected,
# MAGIC   round(
# MAGIC     avg(cr.daily_counts) over (
# MAGIC         partition by cr.`region`
# MAGIC         order by cr.filing_date
# MAGIC         rows between 6 preceding and current row
# MAGIC       )
# MAGIC   ) 7_day_average,
# MAGIC   round(
# MAGIC     avg(cr.daily_counts) over (
# MAGIC         partition by
# MAGIC           cr.region,
# MAGIC           case
# MAGIC             when month(cr.filing_date) >= 10 then year(cr.filing_date) + 1
# MAGIC             else year(cr.filing_date)
# MAGIC           end
# MAGIC         order by cr.filing_date
# MAGIC         rows between 6 preceding and current row
# MAGIC       )
# MAGIC   ) fy_7_day_average
# MAGIC from
# MAGIC   origin_counts cr
# MAGIC     left join origin_counts pq
# MAGIC       on cr.`Fiscal Year` = pq.`Fiscal Year` + 1
# MAGIC       and cr.region = pq.region;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   daily_region_filings
# MAGIC order by
# MAGIC   region,
# MAGIC   filing_date
# MAGIC limit 5;

# COMMAND ----------

# DBTITLE 1,Create Historical Averages
# MAGIC %sql
# MAGIC create or replace temp view averages as
# MAGIC select distinct
# MAGIC   `region`,
# MAGIC   filing_date,
# MAGIC   date_format(filing_date, 'MM/dd') month_day,
# MAGIC   case
# MAGIC     when month(filing_date) >= 10 then year(filing_date) + 1
# MAGIC     else year(filing_date)
# MAGIC   end `Fiscal Year`,
# MAGIC   case
# MAGIC     when month(filing_date) between 10 and 12 then 1
# MAGIC     when month(filing_date) between 1 and 3 then 2
# MAGIC     when month(filing_date) between 4 and 6 then 3
# MAGIC     else 4
# MAGIC   end `Fiscal Quarter`,
# MAGIC   dense_rank() over (
# MAGIC       partition by
# MAGIC         case
# MAGIC           when month(filing_date) >= 10 then year(filing_date) + 1
# MAGIC           else year(filing_date)
# MAGIC         end,
# MAGIC         case
# MAGIC           when month(filing_date) between 10 and 12 then 1
# MAGIC           when month(filing_date) between 1 and 3 then 2
# MAGIC           when month(filing_date) between 4 and 6 then 3
# MAGIC           else 4
# MAGIC         end
# MAGIC       order by month(filing_date)
# MAGIC     ) `Fiscal Month`,
# MAGIC   daily_counts
# MAGIC from
# MAGIC   origin_counts;
# MAGIC
# MAGIC create or replace temp view average_redux as
# MAGIC select
# MAGIC   `region`,
# MAGIC   month_day,
# MAGIC   `Fiscal Year`,
# MAGIC   daily_counts
# MAGIC from
# MAGIC   averages;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   average_redux

# COMMAND ----------

# DBTITLE 1,Generate Daily Average Year Range
year_range = spark.sql("select year(cutoff) + 1 `start`, current_fy `end`").collect()[0]
start, end = year_range.start, year_range.end
year_columns = ", ".join([str(year) for year in range(start, end)])
pivot_stmt = f"pivot (round(avg(daily_counts)) for `Fiscal Year` in ({year_columns}))"
print(f"Pivot for yearly averages will be based on: [{year_columns}]")
print(f"Predicate: {pivot_stmt}")

# COMMAND ----------

# DBTITLE 1,Create Temp View Averages
drop_column = str(spark.sql("select drop_column").collect()[0].drop_column)
regions = [
    value.region
    for value in spark.sql("select distinct `region` from origin_counts").collect()
]
views = []
for region in regions:
    view = f"{region.replace(' ', '_').lower()}_averages"
    views.append(view)
    print(f"Creating region view: {region} as `{view}` using: ")
    pivot_query = f"""
    with base as (
        select 
            * 
        from 
            average_redux 
        where 
            `region` = '{region}'
    )
    select 
        *
    from 
        base 
    {pivot_stmt}
    """
    print(pivot_query)
    spark.sql(pivot_query).drop(col(drop_column)).createOrReplaceTempView(view)
    print(f"Created {region} view: `{view}`. Example records: ")
    spark.sql(f"select * from {view}").limit(2).show(vertical=True, truncate=False)
if len(views) != len(regions):
    raise ValueError(
        "All views must be successfully created. One or more regions failed to instantiate a view."
    )

# COMMAND ----------

# DBTITLE 1,Generate Origin Weights for Averages
# MAGIC %sql
# MAGIC create or replace temp view weights as
# MAGIC select distinct
# MAGIC   `region`,
# MAGIC   sum(daily_counts) over (partition by `region`)
# MAGIC   / sum(daily_counts) over (partition by null) coefficient
# MAGIC from
# MAGIC   daily_region_filings
# MAGIC where
# MAGIC   `Fiscal Year` = previous_fy
# MAGIC   and `Fiscal Year Quarter` = 4;
# MAGIC
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   weights;

# COMMAND ----------

# DBTITLE 1,Sanity Check
index_calculation = (
    "round(7_day_average / sum(7_day_average) over (partition by null) * 366, 3)"
)

world_index_calculation = "round(7_day_average_weighted / sum(7_day_average_weighted) over (partition by null) * 366, 3)"

args = [
    "*",
    f"{index_calculation} index",
    f"round(sum({index_calculation}) over (order by drf.filing_date) / 366, 3) cumulative_index",
]

world_args = [
    "*",
    f"{world_index_calculation} index",
    f"round(sum({world_index_calculation}) over (order by drf.filing_date) / 366, 3) cumulative_index",
]

# world = (
#     spark.sql(
#         """
#     select distinct
#         filing_date,
#         sum(drf.daily_counts) over (partition by drf.filing_date) daily_counts,
#         round(sum(average_on_this_day * coefficient) over (partition by drf.filing_date)) average_on_this_day_weighted,
#         round(sum(projected * coefficient) over (partition by drf.filing_date)) projected_weighted,
#         round(sum(7_day_average * coefficient) over (partition by drf.filing_date)) 7_day_average_weighted,
#         round(sum(fy_7_day_average * coefficient) over (partition by drf.filing_date)) fy_7_day_average_weighted
#     from
#         daily_region_filings drf
#         join weights w
#             on drf.`region` = w.`region`
#     order by
#         filing_date
#     """
#     )
#     .where("filing_date >= current_fy_start")
#     .selectExpr(*world_args)
# )

africa = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'AFRICA' 
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)

americas = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'AMERICAS' 
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)
asia = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'ASIA' 
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)
china = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'CHINA' 
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)
united_states = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'UNITED STATES'
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)

europe = (
    spark.sql(
        """
        select * except(`Fiscal Year`, `Fiscal Year Quarter`, `Month`) 
        from daily_region_filings drf 
        where `region` = 'EUROPE' 
        order by drf.filing_date
        """
    )
    .where("filing_date >= current_fy_start")
    .selectExpr(*args)
)

country_dataframes = (united_states, china, europe, asia, americas, africa)
view_names = (
    "united_states_averages",
    "china_averages",
    "europe_averages",
    "asia_averages",
    "americas_averages",
    "africa_averages",
)
sheets = ("United States", "China", "Europe", "Asia", "Americas", "Africa")
dataframes = []

for view_name, dataframe in zip(view_names, country_dataframes):
    sheet_name = view_name.replace("_", " ").title().replace(" Averages", "")
    print(f"Joining country averages, `{view_name}`, by day to region: {sheet_name}...")
    a = dataframe
    b = spark.sql(f"select * from {view_name}")
    report = a.join(other=b, on=[a["month_day"] == b["month_day"]], how="left")
    dataframes.append(report)

# COMMAND ----------

# DBTITLE 1,Create Multisheet Excel
with BytesIO() as stream:
    with pd.ExcelWriter(
        stream,
        engine="xlsxwriter",
        engine_kwargs=ENGINE_OPTIONS,
    ) as writer:
        for sheet, dataframe in zip(sheets, dataframes):
            dataframe.toPandas().to_excel(
                excel_writer=writer, index=False, sheet_name=sheet
            )
            writer.sheets[sheet].autofit()

    email_data: bytes = stream.getvalue()
print(f"FIRST 10: {email_data[:10]}")

# COMMAND ----------

# DBTITLE 1,Send Email
msg = MIMEMultipart()
msg["From"]: str = send_from
msg["To"]: str = COMMASPACE.join(send_to.split(","))
msg["Cc"]: str = COMMASPACE.join(send_to_cc.split(","))
msg["Subject"]: str = subject

text: str = f"""
Hi,

Please see the attached document regarding daily filing activity by origin. 

This report is responsible for showing trademark application filings per day for the current fiscal year and past 5 years preceding.

If you'd like to request a change in the behavior of this report, please contact the Trademark Data Analytics team via email at trademark_analytics@uspto.gov.
"""

msg.attach(MIMEText(text))
part = MIMEApplication(email_data)
encoders.encode_base64(part)
part.add_header(
    "Content-Disposition",
    "attachment",
    filename=attachment_name,
)
msg.attach(part)

smtp = smtplib.SMTP(server)
rcpt = send_to.split(",") + (send_to_cc.split(",") if send_to_cc else [])
smtp.sendmail(send_from, rcpt, msg.as_string())
smtp.close()

# COMMAND ----------

# DBTITLE 1,End Job
end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed by sending the report successfully.")
