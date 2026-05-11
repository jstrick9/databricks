import sys
if '/Workspace/Users/joshua.strickland@uspto.gov/data_quality' not in sys.path:
    sys.path.insert(0, '/Workspace/Users/joshua.strickland@uspto.gov/data_quality')

"""
Unit tests for canonicalization transforms.
Ensures transforms NEVER destroy data — only standardize formats.
"""
import pytest
from pyspark.sql import functions as F
from transforms.common_transforms import (
    canonicalize_serial_number,
    empty_string_to_null,
    uppercase_and_trim,
    clean_email,
    canonicalize_dates,
    safe_yyyymmdd_to_date,
    standardize_street_address,
)


# ===================================================================
# canonicalize_serial_number
# ===================================================================
class TestSerialNumberCanonicalization:
    def test_pads_short_number(self, spark):
        df = spark.createDataFrame([("12345",)], ["ser_num"])
        result = canonicalize_serial_number(df, "ser_num").collect()[0]["ser_num"]
        assert result == "00012345"

    def test_already_8_digits(self, spark):
        df = spark.createDataFrame([("90123456",)], ["ser_num"])
        result = canonicalize_serial_number(df, "ser_num").collect()[0]["ser_num"]
        assert result == "90123456"

    def test_does_not_truncate_long_number(self, spark):
        """CRITICAL: Over-length strings must NOT be silently truncated."""
        df = spark.createDataFrame([("123456789",)], ["ser_num"])
        result = canonicalize_serial_number(df, "ser_num").collect()[0]["ser_num"]
        assert result == "123456789"

    def test_single_digit(self, spark):
        df = spark.createDataFrame([("5",)], ["ser_num"])
        result = canonicalize_serial_number(df, "ser_num").collect()[0]["ser_num"]
        assert result == "00000005"

    def test_null_handling(self, spark):
        """Null serial numbers should remain null."""
        df = spark.createDataFrame([(None,)], schema="ser_num STRING")
        result = canonicalize_serial_number(df, "ser_num").collect()[0]["ser_num"]
        assert result is None


# ===================================================================
# empty_string_to_null
# ===================================================================
class TestEmptyStringToNull:
    def test_empty_becomes_null(self, spark):
        df = spark.createDataFrame([("",)], ["name"])
        result = empty_string_to_null(df).collect()[0]["name"]
        assert result is None

    def test_whitespace_becomes_null(self, spark):
        df = spark.createDataFrame([("   ",)], ["name"])
        result = empty_string_to_null(df).collect()[0]["name"]
        assert result is None

    def test_real_value_preserved(self, spark):
        df = spark.createDataFrame([("ACME Corp",)], ["name"])
        result = empty_string_to_null(df).collect()[0]["name"]
        assert result == "ACME Corp"

    def test_non_string_columns_untouched(self, spark):
        """Integer columns should not be affected."""
        df = spark.createDataFrame([(42, "")], ["age", "name"])
        result = empty_string_to_null(df).collect()[0]
        assert result["age"] == 42
        assert result["name"] is None

    def test_multiple_string_columns(self, spark):
        df = spark.createDataFrame([("", "hello", "  ")], ["a", "b", "c"])
        result = empty_string_to_null(df).collect()[0]
        assert result["a"] is None
        assert result["b"] == "hello"
        assert result["c"] is None


# ===================================================================
# uppercase_and_trim
# ===================================================================
class TestUppercaseAndTrim:
    def test_lowercase_uppercased(self, spark):
        df = spark.createDataFrame([("hello",)], ["name"])
        result = uppercase_and_trim(df, "name").collect()[0]["name"]
        assert result == "HELLO"

    def test_whitespace_trimmed(self, spark):
        df = spark.createDataFrame([("  hello  ",)], ["name"])
        result = uppercase_and_trim(df, "name").collect()[0]["name"]
        assert result == "HELLO"

    def test_already_upper_unchanged(self, spark):
        df = spark.createDataFrame([("HELLO",)], ["name"])
        result = uppercase_and_trim(df, "name").collect()[0]["name"]
        assert result == "HELLO"

    def test_null_stays_null(self, spark):
        df = spark.createDataFrame([(None,)], schema="name STRING")
        result = uppercase_and_trim(df, "name").collect()[0]["name"]
        assert result is None


# ===================================================================
# canonicalize_dates
# ===================================================================
class TestCanonicalizeDates:
    def test_iso_format(self, spark):
        df = spark.createDataFrame([("2024-01-15",)], ["dt"])
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert str(result) == "2024-01-15"

    def test_compact_format(self, spark):
        df = spark.createDataFrame([("20240115",)], ["dt"])
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert str(result) == "2024-01-15"

    def test_slash_format(self, spark):
        df = spark.createDataFrame([("2024/01/15",)], ["dt"])
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert str(result) == "2024-01-15"

    def test_us_format(self, spark):
        df = spark.createDataFrame([("01/15/2024",)], ["dt"])
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert str(result) == "2024-01-15"

    def test_garbage_becomes_null(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["dt"])
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert result is None

    def test_null_stays_null(self, spark):
        df = spark.createDataFrame([(None,)], schema="dt STRING")
        result = canonicalize_dates(df, "dt").collect()[0]["dt"]
        assert result is None


# ===================================================================
# safe_yyyymmdd_to_date
# ===================================================================
class TestSafeYyyymmddToDate:
    def test_valid_8_digit_string(self, spark):
        df = spark.createDataFrame([("20240115",)], ["dt"])
        result = safe_yyyymmdd_to_date(df, "dt").collect()[0]["dt"]
        assert str(result) == "2024-01-15"

    def test_non_8_digit_becomes_null(self, spark):
        df = spark.createDataFrame([("2024-01-15",)], ["dt"])
        result = safe_yyyymmdd_to_date(df, "dt").collect()[0]["dt"]
        assert result is None  # Has dashes, not pure 8-digit

    def test_alpha_becomes_null(self, spark):
        df = spark.createDataFrame([("ABCDEFGH",)], ["dt"])
        result = safe_yyyymmdd_to_date(df, "dt").collect()[0]["dt"]
        assert result is None


# ===================================================================
# standardize_street_address
# ===================================================================
class TestStandardizeStreetAddress:
    def test_street_to_st(self, spark):
        df = spark.createDataFrame([("123 MAIN STREET",)], ["addr"])
        result = standardize_street_address(df, "addr").collect()[0]["addr"]
        assert result == "123 MAIN ST"

    def test_avenue_to_ave(self, spark):
        df = spark.createDataFrame([("456 PARK AVENUE",)], ["addr"])
        result = standardize_street_address(df, "addr").collect()[0]["addr"]
        assert result == "456 PARK AVE"

    def test_suite_to_ste(self, spark):
        df = spark.createDataFrame([("789 OAK BOULEVARD SUITE 100",)], ["addr"])
        result = standardize_street_address(df, "addr").collect()[0]["addr"]
        assert result == "789 OAK BLVD STE 100"

    def test_multiple_spaces_collapsed(self, spark):
        df = spark.createDataFrame([("123   MAIN   STREET",)], ["addr"])
        result = standardize_street_address(df, "addr").collect()[0]["addr"]
        assert result == "123 MAIN ST"

    def test_lowercase_uppercased(self, spark):
        df = spark.createDataFrame([("123 main street",)], ["addr"])
        result = standardize_street_address(df, "addr").collect()[0]["addr"]
        assert result == "123 MAIN ST"