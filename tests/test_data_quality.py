"""
tests/test_data_quality.py

Unit tests for dq/data_quality.py

Tests cover:
  - DQResult dataclass
  - Bronze checks (null %, duplicates, row count)
  - Silver checks (critical nulls, negative sales, temperature range,
                   duplicates, city coverage)
  - Gold checks   (null %, is_rainy values, category values, rolling non-negative)
  - Report print (smoke-test, no exception)

Run:
    pytest tests/test_data_quality.py -v
"""

import sys
import tempfile
import unittest
from pathlib import Path
from dataclasses import asdict

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# ── Re-use shared Spark fixture from test_pipeline ──────────────────────────
from tests.test_pipeline import _get_test_spark, SKIP_SPARK, spark_available

# ── Import DQ module under test ──────────────────────────────────────────────
from dq.data_quality import (
    DQResult,
    check_bronze_null_pct,
    check_bronze_duplicates,
    check_bronze_row_count,
    check_silver_null_critical,
    check_silver_negative_sales,
    check_silver_temperature_range,
    check_silver_duplicates,
    check_silver_cities,
    check_gold_null_pct,
    check_gold_is_rainy_values,
    check_gold_category_values,
    check_gold_rolling_non_negative,
    _print_report,
)

EXPECTED_CITIES = {"London", "Manchester", "Birmingham", "Leeds", "Glasgow"}


# ══════════════════════════════════════════════════════════════════════════════
# 1. DQResult dataclass
# ══════════════════════════════════════════════════════════════════════════════

class TestDQResultDataclass(unittest.TestCase):

    def test_required_fields_set_correctly(self):
        r = DQResult("DQ-B01", "bronze", "sales_bronze", "PASS", "All good", metric=0.0)
        self.assertEqual(r.check_id, "DQ-B01")
        self.assertEqual(r.layer,    "bronze")
        self.assertEqual(r.status,   "PASS")
        self.assertIsNotNone(r.checked_at)

    def test_serialisable_to_dict(self):
        r = DQResult("DQ-S01:city", "silver", "sales_silver", "FAIL", "nulls found",
                     metric=5.0, threshold=0.0, details={"col": "city"})
        d = asdict(r)
        self.assertIn("check_id", d)
        self.assertIn("details",  d)
        self.assertEqual(d["details"]["col"], "city")

    def test_default_status_values_are_valid(self):
        valid = {"PASS", "FAIL", "WARN", "SKIP"}
        for s in valid:
            r = DQResult("X", "bronze", "t", s, "msg")
            self.assertIn(r.status, valid)


# ══════════════════════════════════════════════════════════════════════════════
# Helper to write a small Parquet / Delta table in a temp dir
# ══════════════════════════════════════════════════════════════════════════════

def _write_parquet(spark, rows, schema, path: str):
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(path)
    return df


def _write_delta(spark, rows, schema, path: str):
    df = spark.createDataFrame(rows, schema)
    df.write.format("delta").mode("overwrite").save(path)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2. Bronze checks
# ══════════════════════════════════════════════════════════════════════════════

class TestBronzeChecks(unittest.TestCase):

    @SKIP_SPARK
    def test_null_pct_pass_when_no_nulls(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_parquet(spark,
                [("London", 5.0, 1.0), ("Manchester", 6.0, 2.0)],
                ["city", "temp_max", "temp_min"], tmp)
            results = check_bronze_null_pct(spark, tmp, "weather_bronze")
        pass_count = sum(1 for r in results if r.status == "PASS")
        fail_count = sum(1 for r in results if r.status == "FAIL")
        self.assertEqual(fail_count, 0)
        self.assertGreater(pass_count, 0)

    @SKIP_SPARK
    def test_null_pct_fail_when_too_many_nulls(self):
        spark = _get_test_spark()
        # 9 out of 10 rows have null in "city" → 90 % > threshold 5 %
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        rows = [(None, 5.0)] * 9 + [("London", 6.0)]
        with tempfile.TemporaryDirectory() as tmp:
            _write_parquet(spark, rows, ["city", "temp_max"], tmp)
            results = check_bronze_null_pct(spark, tmp, "weather_bronze")
        city_result = next((r for r in results if "city" in r.check_id), None)
        self.assertIsNotNone(city_result)
        self.assertEqual(city_result.status, "FAIL")

    @SKIP_SPARK
    def test_duplicate_check_pass_when_unique(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_parquet(spark,
                [("London", 5.0), ("Manchester", 6.0)],
                ["city", "temp"], tmp)
            results = check_bronze_duplicates(spark, tmp, "weather_bronze")
        self.assertEqual(results[0].status, "PASS")
        self.assertEqual(results[0].metric, 0)

    @SKIP_SPARK
    def test_duplicate_check_warn_when_dupes_exist(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_parquet(spark,
                [("London", 5.0), ("London", 5.0)],
                ["city", "temp"], tmp)
            results = check_bronze_duplicates(spark, tmp, "weather_bronze")
        self.assertEqual(results[0].status, "WARN")
        self.assertEqual(results[0].metric, 1)

    
    @SKIP_SPARK
    def test_row_count_pass_with_enough_rows(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            rows = [("London",)] * 200
            _write_parquet(spark, rows, ["city"], tmp)
            results = check_bronze_row_count(spark, tmp, "test")
        self.assertEqual(results[0].status, "PASS")

    @SKIP_SPARK
    def test_skip_result_on_missing_path(self):
        spark = _get_test_spark()
        results = check_bronze_null_pct(spark, "/nonexistent/path", "test")
        self.assertEqual(results[0].status, "SKIP")


# ══════════════════════════════════════════════════════════════════════════════
# 3. Silver checks
# ══════════════════════════════════════════════════════════════════════════════

class TestSilverChecks(unittest.TestCase):

    @SKIP_SPARK
    def test_null_critical_pass_no_nulls(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", 500.0)],
                ["date", "city", "total_sales"], tmp)
            results = check_silver_null_critical(
                spark, tmp, "sales_silver", ["date", "city", "total_sales"])
        fail_count = sum(1 for r in results if r.status == "FAIL")
        self.assertEqual(fail_count, 0)

    @SKIP_SPARK
    def test_null_critical_fail_when_null_present(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", None, 500.0),
                 ("2022-01-02", "London", 400.0)],
                ["date", "city", "total_sales"], tmp)
            results = check_silver_null_critical(
                spark, tmp, "sales_silver", ["city"])
        city_result = next(r for r in results if "city" in r.check_id)
        self.assertEqual(city_result.status, "FAIL")

    @SKIP_SPARK
    def test_negative_sales_fail(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", -50.0),
                 ("2022-01-02", "London",  200.0)],
                ["date", "city", "total_sales"], tmp)
            results = check_silver_negative_sales(spark, tmp, "sales_silver")
        self.assertEqual(results[0].status, "FAIL")
        self.assertEqual(results[0].metric, 1)

    @SKIP_SPARK
    def test_negative_sales_pass(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", 500.0)],
                ["date", "city", "total_sales"], tmp)
            results = check_silver_negative_sales(spark, tmp, "sales_silver")
        self.assertEqual(results[0].status, "PASS")

    @SKIP_SPARK
    def test_temperature_range_pass_normal_values(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", 5.0, 0.0)],
                ["date", "city", "avg_temp", "precipitation"], tmp)
            results = check_silver_temperature_range(spark, tmp, "weather_silver")
        self.assertEqual(results[0].status, "PASS")

    @SKIP_SPARK
    def test_temperature_range_fail_extreme_value(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", 99.0, 0.0)],  # 99°C is out of range
                ["date", "city", "avg_temp", "precipitation"], tmp)
            results = check_silver_temperature_range(spark, tmp, "weather_silver")
        self.assertEqual(results[0].status, "FAIL")

    @SKIP_SPARK
    def test_duplicates_warn_when_same_date_city(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London", 100.0),
                 ("2022-01-01", "London", 200.0)],   # same (date, city)
                ["date", "city", "total_sales"], tmp)
            results = check_silver_duplicates(
                spark, tmp, "sales_silver", ["date", "city"])
        self.assertEqual(results[0].status, "WARN")

    @SKIP_SPARK
    def test_cities_fail_when_city_missing(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark,
                [("2022-01-01", "London",     500.0),
                 ("2022-01-01", "Manchester",  400.0)],
                # Only 2 of 5 cities → should FAIL
                ["date", "city", "total_sales"], tmp)
            results = check_silver_cities(spark, tmp, "sales_silver")
        self.assertEqual(results[0].status, "FAIL")
        self.assertIn("Birmingham", results[0].details["missing"])

    @SKIP_SPARK
    def test_cities_pass_when_all_present(self):
        spark = _get_test_spark()
        cities = list(EXPECTED_CITIES)
        rows = [("2022-01-01", c, 500.0) for c in cities]
        with tempfile.TemporaryDirectory() as tmp:
            _write_delta(spark, rows, ["date", "city", "total_sales"], tmp)
            results = check_silver_cities(spark, tmp, "sales_silver")
        self.assertEqual(results[0].status, "PASS")


# ══════════════════════════════════════════════════════════════════════════════
# 4. Gold checks
# ══════════════════════════════════════════════════════════════════════════════

class TestGoldChecks(unittest.TestCase):

    def _write_gold(self, spark, tmp):
        rows = [
            ("2022-01-01", "London", 500.0, 5.0, 2.5, 1, 450.0, "Cold",   "Medium"),
            ("2022-01-02", "London", 800.0, 8.0, 0.0, 0, 650.0, "Cold",   "Medium"),
        ]
        schema = ["date", "city", "total_sales", "avg_temp", "precipitation",
                  "is_rainy", "rolling_7d_sales", "temp_category", "sales_category"]
        _write_delta(spark, rows, schema, tmp)

    @SKIP_SPARK
    def test_gold_null_pct_pass(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            self._write_gold(spark, tmp)
            results = check_gold_null_pct(
                spark, tmp, "sales_weather",
                ["total_sales", "avg_temp", "precipitation", "is_rainy"])
        fail_count = sum(1 for r in results if r.status == "FAIL")
        self.assertEqual(fail_count, 0)

    @SKIP_SPARK
    def test_gold_is_rainy_pass(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            self._write_gold(spark, tmp)
            results = check_gold_is_rainy_values(spark, tmp, "sales_weather")
        self.assertEqual(results[0].status, "PASS")

   
    @SKIP_SPARK
    def test_gold_category_values_pass(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            self._write_gold(spark, tmp)
            results = check_gold_category_values(spark, tmp, "sales_weather")
        fail_count = sum(1 for r in results if r.status == "FAIL")
        self.assertEqual(fail_count, 0)

    @SKIP_SPARK
    def test_gold_category_values_fail_bad_temp_category(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            rows = [("2022-01-01", "London", "Tepid", "Low")]   # "Tepid" is invalid
            _write_delta(spark, rows,
                         ["date", "city", "temp_category", "sales_category"], tmp)
            results = check_gold_category_values(spark, tmp, "sales_weather")
        temp_result = next(r for r in results if r.check_id == "DQ-G03")
        self.assertEqual(temp_result.status, "FAIL")
        self.assertIn("Tepid", temp_result.details["invalid"])

    @SKIP_SPARK
    def test_gold_rolling_non_negative_pass(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            rows = [("2022-01-01", "London", 400.0)]
            _write_delta(spark, rows, ["date", "city", "rolling_7d_sales"], tmp)
            results = check_gold_rolling_non_negative(spark, tmp, "sales_weather")
        self.assertEqual(results[0].status, "PASS")

    @SKIP_SPARK
    def test_gold_rolling_non_negative_fail(self):
        spark = _get_test_spark()
        with tempfile.TemporaryDirectory() as tmp:
            rows = [("2022-01-01", "London", -10.0)]
            _write_delta(spark, rows, ["date", "city", "rolling_7d_sales"], tmp)
            results = check_gold_rolling_non_negative(spark, tmp, "sales_weather")
        self.assertEqual(results[0].status, "FAIL")


# ══════════════════════════════════════════════════════════════════════════════
# 5. Report print (smoke test)
# ══════════════════════════════════════════════════════════════════════════════

class TestReportPrint(unittest.TestCase):

    def test_print_report_does_not_raise(self):
        results = [
            DQResult("DQ-B01", "bronze", "t1", "PASS",  "ok"),
            DQResult("DQ-S01", "silver", "t2", "FAIL",  "nulls found", metric=5.0),
            DQResult("DQ-G02", "gold",   "t3", "WARN",  "warning"),
            DQResult("DQ-G03", "gold",   "t4", "SKIP",  "skipped"),
        ]
        # Should complete without exception
        try:
            _print_report(results)
        except Exception as e:
            self.fail(f"_print_report raised unexpectedly: {e}")


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
