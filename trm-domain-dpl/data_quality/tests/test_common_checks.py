import sys

"""
Unit tests for all common DQ check functions.
"""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from custom_checks.common_checks import (
    is_not_null, is_unique, regex_match, values_in_0_or_1,
    all_caps, valid_iso_country_code, valid_email_format,
    valid_ph_action_code, fiscal_year_matches_date, created_before_last_modified
)

class TestIsNotNull:
    def test_non_null_passes(self, spark):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = df.withColumn("fails", is_not_null("name")).collect()[0]["fails"]
        assert result == False

    def test_null_fails(self, spark):
        # FIX: Force schema inference for pure None columns
        df = spark.createDataFrame([(None,)], schema="name STRING")
        result = df.withColumn("fails", is_not_null("name")).collect()[0]["fails"]
        assert result == True

    def test_empty_string_passes(self, spark):
        df = spark.createDataFrame([("",)], ["name"])
        result = df.withColumn("fails", is_not_null("name")).collect()[0]["fails"]
        assert result == False

class TestIsUnique:
    def test_unique_values_pass(self, spark):
        df = spark.createDataFrame([("A",), ("B",), ("C",)], ["code"])
        results = df.withColumn("fails", is_unique("code")).collect()
        assert all(r["fails"] == False for r in results)

    def test_duplicates_fail(self, spark):
        df = spark.createDataFrame([("A",), ("A",), ("B",)], ["code"])
        results = df.withColumn("fails", is_unique("code")).collect()
        a_rows = [r for r in results if r["code"] == "A"]
        b_rows = [r for r in results if r["code"] == "B"]
        assert all(r["fails"] == True for r in a_rows)
        assert all(r["fails"] == False for r in b_rows)

class TestRegexMatch:
    def test_valid_8_digit_serial(self, spark):
        df = spark.createDataFrame([("90123456",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num", regex=r"^[0-9]{8}$")).collect()[0]["fails"]
        assert result == False

    def test_invalid_short_serial(self, spark):
        df = spark.createDataFrame([("12345",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num", regex=r"^[0-9]{8}$")).collect()[0]["fails"]
        assert result == True

    def test_invalid_alpha_serial(self, spark):
        df = spark.createDataFrame([("ABCD1234",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num", regex=r"^[0-9]{8}$")).collect()[0]["fails"]
        assert result == True

    def test_invalid_too_long(self, spark):
        df = spark.createDataFrame([("123456789",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num", regex=r"^[0-9]{8}$")).collect()[0]["fails"]
        assert result == True

    def test_no_regex_provided_fails_all(self, spark):
        df = spark.createDataFrame([("90123456",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num")).collect()[0]["fails"]
        assert result == True

    def test_unknown_regex_name_fails_all(self, spark):
        df = spark.createDataFrame([("90123456",)], ["ser_num"])
        result = df.withColumn("fails", regex_match("ser_num", regex_name="DOES_NOT_EXIST")).collect()[0]["fails"]
        assert result == True

class TestValuesIn0Or1:
    def test_zero_passes(self, spark):
        df = spark.createDataFrame([(0,)], ["flag"])
        result = df.withColumn("fails", values_in_0_or_1("flag")).collect()[0]["fails"]
        assert result == False

    def test_one_passes(self, spark):
        df = spark.createDataFrame([(1,)], ["flag"])
        result = df.withColumn("fails", values_in_0_or_1("flag")).collect()[0]["fails"]
        assert result == False

    def test_two_fails(self, spark):
        df = spark.createDataFrame([(2,)], ["flag"])
        result = df.withColumn("fails", values_in_0_or_1("flag")).collect()[0]["fails"]
        assert result == True

    def test_negative_fails(self, spark):
        df = spark.createDataFrame([(-1,)], ["flag"])
        result = df.withColumn("fails", values_in_0_or_1("flag")).collect()[0]["fails"]
        assert result == True

class TestAllCaps:
    def test_uppercase_passes(self, spark):
        df = spark.createDataFrame([("HELLO WORLD",)], ["name"])
        result = df.withColumn("fails", all_caps("name")).collect()[0]["fails"]
        assert result == False

    def test_lowercase_fails(self, spark):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = df.withColumn("fails", all_caps("name")).collect()[0]["fails"]
        assert result == True

    def test_mixed_case_fails(self, spark):
        df = spark.createDataFrame([("Hello World",)], ["name"])
        result = df.withColumn("fails", all_caps("name")).collect()[0]["fails"]
        assert result == True

    def test_numbers_pass(self, spark):
        df = spark.createDataFrame([("12345",)], ["name"])
        result = df.withColumn("fails", all_caps("name")).collect()[0]["fails"]
        assert result == False

class TestValidIsoCountryCode:
    def test_valid_us(self, spark):
        df = spark.createDataFrame([("US",)], ["ctry_cd"])
        result = df.withColumn("fails", valid_iso_country_code("ctry_cd")).collect()[0]["fails"]
        assert result == False

    def test_null_passes(self, spark):
        # FIX: Schema inference string type
        df = spark.createDataFrame([(None,)], schema="ctry_cd STRING")
        result = df.withColumn("fails", valid_iso_country_code("ctry_cd")).collect()[0]["fails"]
        assert result == False

    def test_invalid_narnia(self, spark):
        df = spark.createDataFrame([("NARNIA",)], ["ctry_cd"])
        result = df.withColumn("fails", valid_iso_country_code("ctry_cd")).collect()[0]["fails"]
        assert result == True

    def test_invalid_lowercase(self, spark):
        df = spark.createDataFrame([("us",)], ["ctry_cd"])
        result = df.withColumn("fails", valid_iso_country_code("ctry_cd")).collect()[0]["fails"]
        assert result == True

class TestFiscalYearMatchesDate:
    def test_correct_fy_before_october(self, spark):
        df = spark.createDataFrame([(2024, "2024-03-15")], ["fy", "dt"])
        df = df.withColumn("dt", F.to_date("dt"))
        result = df.withColumn("fails", fiscal_year_matches_date("fy", "dt")).collect()[0]["fails"]
        assert result == False

    def test_correct_fy_after_october(self, spark):
        df = spark.createDataFrame([(2025, "2024-10-15")], ["fy", "dt"])
        df = df.withColumn("dt", F.to_date("dt"))
        result = df.withColumn("fails", fiscal_year_matches_date("fy", "dt")).collect()[0]["fails"]
        assert result == False

    def test_wrong_fy_after_october(self, spark):
        df = spark.createDataFrame([(2024, "2024-10-15")], ["fy", "dt"])
        df = df.withColumn("dt", F.to_date("dt"))
        result = df.withColumn("fails", fiscal_year_matches_date("fy", "dt")).collect()[0]["fails"]
        assert result == True

    def test_september_is_same_fy(self, spark):
        df = spark.createDataFrame([(2024, "2024-09-30")], ["fy", "dt"])
        df = df.withColumn("dt", F.to_date("dt"))
        result = df.withColumn("fails", fiscal_year_matches_date("fy", "dt")).collect()[0]["fails"]
        assert result == False

class TestCreatedBeforeLastModified:
    def test_created_before_modified_passes(self, spark):
        df = spark.createDataFrame([("2024-01-01", "2024-06-01")], ["create_ts", "last_mod_ts"])
        df = df.withColumn("create_ts", F.to_timestamp("create_ts")) \
               .withColumn("last_mod_ts", F.to_timestamp("last_mod_ts"))
        result = df.withColumn("fails", created_before_last_modified()).collect()[0]["fails"]
        assert result == False

    def test_same_timestamp_passes(self, spark):
        df = spark.createDataFrame([("2024-01-01", "2024-01-01")], ["create_ts", "last_mod_ts"])
        df = df.withColumn("create_ts", F.to_timestamp("create_ts")) \
               .withColumn("last_mod_ts", F.to_timestamp("last_mod_ts"))
        result = df.withColumn("fails", created_before_last_modified()).collect()[0]["fails"]
        assert result == False

    def test_created_after_modified_fails(self, spark):
        df = spark.createDataFrame([("2024-06-01", "2024-01-01")], ["create_ts", "last_mod_ts"])
        df = df.withColumn("create_ts", F.to_timestamp("create_ts")) \
               .withColumn("last_mod_ts", F.to_timestamp("last_mod_ts"))
        result = df.withColumn("fails", created_before_last_modified()).collect()[0]["fails"]
        assert result == True