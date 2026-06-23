# Databricks notebook source
from pyspark.sql.functions import (
    lit,
    udf,
    min,
    row_number,
    monotonically_increasing_id,
    format_string,
    date_format,
    floor,
    when,
    expr,
    to_date,
    collect_set,
    collect_list,
    explode_outer,
    split,
    concat,
    desc,
    struct,
    array_sort,
    regexp_extract,
    rtrim,
    substring,
    length,
    expr,
    year
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
    LongType,
    TimestampType,
    DateType
) 

from pyspark.sql import (
    Window,
    DataFrame
)

import textwrap
import os
import re
from delta.tables import DeltaTable

# COMMAND ----------

def split_us_or_international_code(codes: str) -> list:
    """
    Split us code from classification collection
    into new array column. US code contains 3 characters.
    """

    chunks = []
    code_length = 3

    if not codes:
        return chunks

    for position in range(0, len(codes), code_length):
        chunk = codes[position : position + code_length]
        chunks.append(chunk)

    return chunks


split_us_or_international_code_udf = udf(split_us_or_international_code, ArrayType(StringType()))

# COMMAND ----------

def get_attorney_name(name_change_df: DataFrame, case_file_header_df: DataFrame) -> DataFrame:
    """
    Merge attorney name if exists for associated serial number.
    """
    ATTORNEY_CODE = "AT0000"

    attorney_name_df = (
        name_change_df.filter(col("vt_text_type") == ATTORNEY_CODE)
            .select(
                col("vt_ser_num").alias("serial-number"),
                col("vt_text").alias("attorney-name")
            )
    )

    return case_file_header_df.join(attorney_name_df, on="serial-number", how="left")

def get_domestic_representative_name(name_change_df: DataFrame, case_file_header_df: DataFrame) -> DataFrame:
    """
    Merge domestic representative name if exists for associated serial number.
    """
    DOMESTIC_REPRESENTATIVE_CODE = "DR0000"

    domestic_representative_name_df = (
        name_change_df.filter(col("vt_text_type") == DOMESTIC_REPRESENTATIVE_CODE)
            .select(
                col("vt_ser_num").alias("serial-number"),
                col("vt_text").alias("domestic-representative-name")
            )
    )

    return case_file_header_df.join(domestic_representative_name_df, on="serial-number", how="left")

# COMMAND ----------

def sort_struct_field_ascending(df: DataFrame, base_struct_field: str, nested_struct_field: str, ascending_field: str) -> DataFrame:
    """
    Sorting dataframe struct nested field into ascending order, treating field as a number.
    """

    sort_struct_field = array_sort(
        col(f"{base_struct_field}.{nested_struct_field}"),
        lambda x, y: (x[ascending_field] - y[ascending_field]).cast(IntegerType()),
    )

    df = df.withColumn(
        base_struct_field,
        col(base_struct_field).withField(f"`{nested_struct_field}`", sort_struct_field),
    )

    return df

# COMMAND ----------

def sort_struct_field(df: DataFrame, base_struct_field: str, nested_struct_field: str, ascending_field: str, descending_field: str) -> DataFrame:
    """
    Sorting dataframe struct nested fields into ascending and descending order, treating fields as a numbers.
    """

    field_sort = expr(
        f"""
        array_sort(
            `{base_struct_field}`.`{nested_struct_field}`,
            (x, y) -> CASE
                WHEN CAST(x.`{ascending_field}` AS INT) < CAST(y.`{ascending_field}` AS INT) THEN -1
                WHEN CAST(x.`{ascending_field}` AS INT) > CAST(y.`{ascending_field}` AS INT) THEN 1
                WHEN CAST(x.`{ascending_field}` AS INT) = CAST(y.`{ascending_field}` AS INT) THEN
                    CASE
                        WHEN CAST(x.`{descending_field}` AS INT) > CAST(y.`{descending_field}` AS INT) THEN -1
                        WHEN CAST(x.`{descending_field}` AS INT) < CAST(y.`{descending_field}` AS INT) THEN 1
                        ELSE 0
                    END
                ELSE 0
            END
        )
        """)

    df = df.withColumn(
        base_struct_field,
        col(base_struct_field).withField(f"`{nested_struct_field}`", field_sort),
    )

    return df

# COMMAND ----------

def get_name_change_data(df: DataFrame) -> DataFrame:
    """
    Extract name-change-explanation field based on name change code,
    and prepare dataframe for merging to case file owners data.
    """


    NAME_CHANGE_CODE = "NC"

    df = df.filter(col("vt_text_type").like(f"%{NAME_CHANGE_CODE}%")).select(
        col("vt_ser_num").alias("serial-number"),
        col("vt_text_type"),
        col("name_change_text").alias("name-change-explanation"),
    )
    df = df.withColumn(
        "PARTY_TYPE", regexp_extract(col("vt_text_type"), r"NC(\d{2})", 1)
    ).drop("vt_text_type")

    return df

# COMMAND ----------

def process_owners_nationality_data(df: DataFrame) -> DataFrame:
    """
    Setup state, country and other fields on nationality and case-file-owner level.
    """

    df = (
        df.withColumn(
            "CITIZEN_OTHER",
            when(
                col("CITIZEN_COUNTRY").isNotNull(),
                None
            ).otherwise(col("CITIZEN_OTHER"))
        )
        .withColumnRenamed("CITIZEN_STATE", "nationality_state")
        .withColumnRenamed("CITIZEN_COUNTRY", "nationality_country")
        .withColumnRenamed("CITIZEN_OTHER", "nationality_other")
    )

    df = (
        df.withColumn(
            "owner_state",
            when(
                (col("STE_CTRY_CD").isNotNull()) & (length(col("STE_CTRY_CD")) == 2),
                col("STE_CTRY_CD"),
            ).otherwise(None),
        )
        .withColumn(
            "owner_country",
            when(
                col("nationality_country").isNotNull(),
                col("nationality_country")
            ).otherwise(
                when(
                    (col("STE_CTRY_CD").isNotNull())
                    & (length(col("STE_CTRY_CD")) == 3)
                    & (col("STE_CTRY_CD").endswith("X")),
                    substring(col("STE_CTRY_CD"), 1, 2),
                ).otherwise(None)
            )
        )
        .withColumn(
            "owner_other",
            when(
                (col("STE_CTRY_CD").isNotNull())
                & (length(col("STE_CTRY_CD")) == 3)
                & (~col("STE_CTRY_CD").endswith("X")),
                col("STE_CTRY_CD"),
            ).otherwise(None),
        )
    )

    return df

# COMMAND ----------

def validate_xml_content_against_dtd(content: str) -> str:
    """
    Make sure that mandatory fields exist in XML file if they have no value.
    With the following format <tag-name/>
    """

    # international-registration
    # madrid-international-filing-record
    # madrid-history-event
    # case-file-owner
    return (content
        .replace("<international-registration-number></international-registration-number>", "<international-registration-number/>")
        .replace("<international-registration-date></international-registration-date>", "<international-registration-date/>")
        .replace("<international-publication-date></international-publication-date>", "<international-publication-date/>")
        .replace("<international-renewal-date></international-renewal-date>", "<international-renewal-date/>")
        .replace("<international-status-code></international-status-code>", "<international-status-code/>")
        .replace("<international-status-date></international-status-date>", "<international-status-date/>")
        .replace("<priority-claimed-in></priority-claimed-in>", "<priority-claimed-in/>")
        .replace("<first-refusal-in></first-refusal-in>", "<first-refusal-in/>")
        .replace("<entry-number></entry-number>", "<entry-number/>")
        .replace("<reference-number></reference-number>", "<reference-number/>")
        .replace("<original-filing-date-uspto></original-filing-date-uspto>", "<original-filing-date-uspto/>")
        .replace("<code></code>", "<code/>")
        .replace("<date></date>", "<date/>")
        .replace("<description-text></description-text", "<description-text/>")
        .replace("<name-change-explanation></name-change-explanation>", "<name-change-explanation/>"))

# COMMAND ----------

def get_part_file_content(xml_file_path: str) -> str:
    """
    Extract part file content.
    Content is used to merge all part files into single partition xml file.
    """

    with open(xml_file_path, "r", encoding="utf-8") as fs:
        content = fs.read()

    content = (
        content.replace(
            "<" + '?xml version="1.0" encoding="UTF-8" standalone="yes"?>', ""
        )
        .replace("<action-keys>", "")
        .replace("</action-keys>", "")
    )
    content = re.sub(r"^\s*\n", "", content, flags=re.MULTILINE)
    content = textwrap.indent(content, "            ")
    content = validate_xml_content_against_dtd(content=content)

    return content

# COMMAND ----------

def extract_info_from_partition(root_path: str) -> str:
    """
    Extract last-mod-dt or action-key from partitioned file path.
    """

    parent_folder = os.path.basename(root_path)
    return parent_folder.split("=")[1]

# COMMAND ----------

def is_valid(character: str) -> bool:
    """
    Based on the condition determines if following character
    is in range of valid or invalid characters is returned
    bool value true, or false if it is not.
    """

    code = ord(character)

    return (
        code == 0x9
        or code == 0xA
        or code == 0xD
        or (0x20 <= code <= 0xD7FF)
        or (0xE000 <= code <= 0xFFFD)
        or (0x10000 <= code <= 0x10FFFF)
    )


def escape_invalid_text(text: str, replace_char: str = "") -> str:
    """
    Replaces invalid xml character from the string.
    """

    EMPTY_STRING = ""

    if not text:
        return EMPTY_STRING

    return "".join(c if is_valid(c) else replace_char for c in text)


remove_invalid_xml_characters_udf = udf(lambda s: escape_invalid_text(s, ""), StringType())


def remove_invalid_xml_characters(df: DataFrame) -> DataFrame:
    """
    Removes invalid characters from each column of DataFrame
    to be able to generate XML files.
    """

    STRING_TYPE = "string"

    for column_name, dtype in df.dtypes:
        if dtype != STRING_TYPE:
            continue

        df = df.withColumn(
            column_name, remove_invalid_xml_characters_udf(col(column_name))
        )

    return df

# COMMAND ----------

def get_creation_datetime(creation_date: str, historical: str) -> str:
    """
    Build creation datetime to format YYYYmmddHHMM from
    creation_date which uses format yymmdd.
    """

    creation_time = datetime.datetime.now().strftime("%H%M")
    
    if historical == "true":
        creation_date = datetime.datetime.now().date()
    else:
        creation_date = datetime.datetime.strptime(creation_date, "%y%m%d")

    return f"{creation_date.strftime('%Y%m%d')}{creation_time}"
