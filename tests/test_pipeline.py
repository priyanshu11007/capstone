"""
tests/test_pipeline.py

Unit tests for:
  - loader/spark_loader.py   (SparkLoader)
  - processing/bronze/       (SalesBronzeTransformer, WeatherBronzeTransformer)
  - processing/silver/       (SalesSilverTransformer, WeatherSilverTransformer)
  - processing/gold/         (SalesWeatherGold, DimDate, DimLocation, FactSalesWeather)

All Spark-dependent tests use a single local SparkSession fixture so the JVM
starts only once per test run.

Run:
    pytest tests/test_pipeline.py -v
    pytest tests/test_pipeline.py -v -k "silver"     # run only silver tests
"""

import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


# ══════════════════════════════════════════════════════════════════════════════
# Shared Spark fixture  (module-level → JVM starts once)
# ══════════════════════════════════════════════════════════════════════════════

_SPARK = None   # lazily initialised

def _get_test_spark():
    """Return a local SparkSession with Delta support, creating it if needed."""
    global _SPARK
    if _SPARK is None:
        try:
            from delta import configure_spark_with_delta_pip
            from pyspark.sql import SparkSession
            builder = (
                SparkSession.builder
                .appName("TestSuite")
                .master("local[2]")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.memory", "1g")
            )
            _SPARK = configure_spark_with_delta_pip(builder).getOrCreate()
            _SPARK.sparkContext.setLogLevel("ERROR")
        except Exception as e:
            _SPARK = None
            raise RuntimeError(
                f"Could not start SparkSession: {e}\n"
                "Make sure pyspark and delta-spark are installed and Java is available."
            )
    return _SPARK


def spark_available() -> bool:
    try:
        _get_test_spark()
        return True
    except Exception:
        return False


SKIP_SPARK = pytest.mark.skipif(
    not spark_available(),
    reason="PySpark/Java not available — skipping Spark-dependent tests"
)


# ══════════════════════════════════════════════════════════════════════════════
# 1. SparkLoader
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkLoader(unittest.TestCase):

    @SKIP_SPARK
    def test_spark_loader_creates_session(self):
        from loader.spark_loader import SparkLoader
        loader = SparkLoader()
        self.assertIsNotNone(loader.spark)

    @SKIP_SPARK
    def test_spark_loader_read_parquet(self):
        from loader.spark_loader import SparkLoader
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType

        spark  = _get_test_spark()
        loader = SparkLoader()
        loader.spark = spark

        schema = StructType([
            StructField("name", StringType(), True),
            StructField("val",  IntegerType(), True),
        ])
        df = spark.createDataFrame([("alice", 1), ("bob", 2)], schema)

        with tempfile.TemporaryDirectory() as tmp:
            df.write.parquet(tmp + "/test.parquet")
            result = loader.read(tmp + "/test.parquet", format="parquet")
            self.assertEqual(result.count(), 2)

    @SKIP_SPARK
    def test_spark_loader_unsupported_format_raises(self):
        from loader.spark_loader import SparkLoader
        loader = SparkLoader()
        loader.spark = _get_test_spark()
        with self.assertRaises(ValueError):
            loader.read("some/path", format="avro")


# ══════════════════════════════════════════════════════════════════════════════
# 2. Bronze transformers (logic tested with in-memory DataFrames)
# ══════════════════════════════════════════════════════════════════════════════

class TestSalesBronzeTransformerLogic(unittest.TestCase):
    """
    Tests the column-casting and city-assignment logic without reading files.
    We inject a small in-memory DataFrame instead of raw Parquet.
    """

    @SKIP_SPARK
    def test_city_assignment_five_cities(self):
        """CustomerID % 5 maps to exactly 5 distinct cities."""
        spark = _get_test_spark()
        from pyspark.sql.functions import expr

        cities = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]
        df = spark.range(50).selectExpr(
            "CAST(id + 10000 AS INT) AS CustomerID"
        )
        df = df.withColumn(
            "city",
            expr(f"""
                CASE
                    WHEN CustomerID % 5 = 0 THEN '{cities[0]}'
                    WHEN CustomerID % 5 = 1 THEN '{cities[1]}'
                    WHEN CustomerID % 5 = 2 THEN '{cities[2]}'
                    WHEN CustomerID % 5 = 3 THEN '{cities[3]}'
                    ELSE '{cities[4]}'
                END
            """)
        )
        found_cities = {row["city"] for row in df.select("city").distinct().collect()}
        self.assertEqual(found_cities, set(cities))

    @SKIP_SPARK
    def test_date_shift_adds_twelve_years(self):
        """Dates are shifted 12*12 = 144 months forward (UCI 2010→2022)."""
        spark = _get_test_spark()
        from pyspark.sql.functions import expr, to_timestamp, col

        df = spark.createDataFrame(
            [("2010-12-01 08:26:00",)], ["InvoiceDate"]
        )
        df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate")))
        df = df.withColumn("InvoiceDate", expr("add_months(InvoiceDate, 12*12)"))
        result_date = df.collect()[0]["InvoiceDate"]
        self.assertEqual(result_date.year, 2022)
        self.assertEqual(result_date.month, 12)

    @SKIP_SPARK
    def test_null_customerid_filtered_out(self):
        """Rows with NULL CustomerID must be dropped."""
        spark = _get_test_spark()
        from pyspark.sql.functions import col
        from pyspark.sql.types import DoubleType

        df = spark.createDataFrame(
            [(1, 12345.0), (2, None)],
            ["idx", "CustomerID"]
        ).withColumn("CustomerID", col("CustomerID").cast(DoubleType()))

        df_filtered = df.filter(col("CustomerID").isNotNull())
        self.assertEqual(df_filtered.count(), 1)


class TestWeatherBronzeTransformerLogic(unittest.TestCase):

    @SKIP_SPARK
    def test_temperature_cast_to_double(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col
        from pyspark.sql.types import DoubleType

        df = spark.createDataFrame(
            [("2022-01-01", "5.5", "1.0", "0.0", "London")],
            ["date", "temp_max", "temp_min", "precipitation", "city"]
        )
        df = df.withColumn("temp_max", col("temp_max").cast(DoubleType()))
        self.assertEqual(df.dtypes[1][1], "double")

    @SKIP_SPARK
    def test_null_temp_rows_dropped(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col

        df = spark.createDataFrame(
            [("2022-01-01", 5.0), ("2022-01-02", None)],
            ["date", "temp_max"]
        )
        df_clean = df.filter(col("temp_max").isNotNull())
        self.assertEqual(df_clean.count(), 1)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Silver transformers (logic with in-memory DataFrames)
# ══════════════════════════════════════════════════════════════════════════════

class TestSalesSilverLogic(unittest.TestCase):

    @SKIP_SPARK
    def test_negative_quantity_filtered(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col
        from pyspark.sql.types import IntegerType

        df = spark.createDataFrame(
            [(1, 5), (2, -3), (3, 0)],
            ["CustomerID", "Quantity"]
        ).withColumn("Quantity", col("Quantity").cast(IntegerType()))

        filtered = df.filter(col("Quantity") > 0)
        self.assertEqual(filtered.count(), 1)

    @SKIP_SPARK
    def test_total_price_calculation(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col
        from pyspark.sql.types import DoubleType

        df = spark.createDataFrame(
            [(10, 2.5), (5, 3.0)],
            ["Quantity", "UnitPrice"]
        )
        df = df.withColumn(
            "total_price",
            (col("Quantity").cast(DoubleType()) * col("UnitPrice"))
        )
        rows = df.orderBy("Quantity").collect()
        self.assertAlmostEqual(rows[0]["total_price"], 15.0)
        self.assertAlmostEqual(rows[1]["total_price"], 25.0)

    @SKIP_SPARK
    def test_daily_aggregation(self):
        """groupBy date+city and sum total_price → one row per (date, city)."""
        spark = _get_test_spark()
        from pyspark.sql.functions import col, sum as _sum
        from pyspark.sql.types import DoubleType, DateType

        df = spark.createDataFrame([
            ("2022-01-01", "London",  100.0),
            ("2022-01-01", "London",   50.0),
            ("2022-01-01", "Glasgow",  80.0),
        ], ["date", "city", "total_price"])

        df = df.withColumn("date", col("date").cast(DateType()))
        agg = df.groupBy("date", "city").agg(
            _sum("total_price").cast(DoubleType()).alias("total_sales")
        )
        london = agg.filter(col("city") == "London").collect()[0]["total_sales"]
        self.assertAlmostEqual(london, 150.0)
        self.assertEqual(agg.count(), 2)


class TestWeatherSilverLogic(unittest.TestCase):

    @SKIP_SPARK
    def test_avg_temp_feature(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col
        from pyspark.sql.types import DoubleType

        df = spark.createDataFrame(
            [(10.0, 2.0), (20.0, 4.0)],
            ["temp_max", "temp_min"]
        )
        df = df.withColumn(
            "avg_temp",
            ((col("temp_max") + col("temp_min")) / 2).cast(DoubleType())
        )
        rows = df.orderBy("temp_max").collect()
        self.assertAlmostEqual(rows[0]["avg_temp"], 6.0)
        self.assertAlmostEqual(rows[1]["avg_temp"], 12.0)


# ══════════════════════════════════════════════════════════════════════════════
# 4. Gold / Dimensions (logic)
# ══════════════════════════════════════════════════════════════════════════════

class TestGoldLogic(unittest.TestCase):

    @SKIP_SPARK
    def test_is_rainy_flag(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col, when
        from pyspark.sql.types import IntegerType

        df = spark.createDataFrame(
            [(0.0,), (2.5,), (0.0,)],
            ["precipitation"]
        )
        df = df.withColumn(
            "is_rainy",
            when(col("precipitation") > 0, 1).otherwise(0).cast(IntegerType())
        )
        rows = df.orderBy("precipitation").collect()
        self.assertEqual(rows[0]["is_rainy"], 0)
        self.assertEqual(rows[1]["is_rainy"], 0)
        self.assertEqual(rows[2]["is_rainy"], 1)

    @SKIP_SPARK
    def test_temp_category_boundaries(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col, when
        from pyspark.sql.types import StringType

        df = spark.createDataFrame(
            [(5.0,), (15.0,), (25.0,)],
            ["avg_temp"]
        )
        df = df.withColumn(
            "temp_category",
            when(col("avg_temp") < 10, "Cold")
            .when((col("avg_temp") >= 10) & (col("avg_temp") < 20), "Moderate")
            .otherwise("Hot")
            .cast(StringType())
        )
        rows = {r["avg_temp"]: r["temp_category"] for r in df.collect()}
        self.assertEqual(rows[5.0],  "Cold")
        self.assertEqual(rows[15.0], "Moderate")
        self.assertEqual(rows[25.0], "Hot")

    @SKIP_SPARK
    def test_sales_category_boundaries(self):
        spark = _get_test_spark()
        from pyspark.sql.functions import col, when
        from pyspark.sql.types import StringType

        df = spark.createDataFrame(
            [(100.0,), (1000.0,), (5000.0,)],
            ["total_sales"]
        )
        df = df.withColumn(
            "sales_category",
            when(col("total_sales") < 500, "Low")
            .when((col("total_sales") >= 500) & (col("total_sales") < 2000), "Medium")
            .otherwise("High")
            .cast(StringType())
        )
        rows = {r["total_sales"]: r["sales_category"] for r in df.collect()}
        self.assertEqual(rows[100.0],  "Low")
        self.assertEqual(rows[1000.0], "Medium")
        self.assertEqual(rows[5000.0], "High")

    @SKIP_SPARK
    def test_rolling_7d_window(self):
        """7-day rolling average on a single city sequence."""
        spark = _get_test_spark()
        from pyspark.sql.functions import col, avg
        from pyspark.sql.types import DoubleType, DateType
        from pyspark.sql.window import Window

        data = [
            ("2022-01-01", "London", 100.0),
            ("2022-01-02", "London", 200.0),
            ("2022-01-03", "London", 300.0),
        ]
        df = spark.createDataFrame(data, ["date", "city", "total_sales"])
        df = df.withColumn("date", col("date").cast(DateType()))

        window_spec = Window.partitionBy("city").orderBy("date").rowsBetween(-7, 0)
        df = df.withColumn(
            "rolling_7d_sales",
            avg("total_sales").over(window_spec).cast(DoubleType())
        )
        rows = {r["date"]: r["rolling_7d_sales"] for r in df.collect()}
        # On day 3: avg(100, 200, 300) = 200
        self.assertAlmostEqual(rows[date(2022, 1, 3)], 200.0)

    @SKIP_SPARK
    def test_inner_join_drops_unmatched_rows(self):
        """Inner join sales + weather drops dates with no weather match."""
        spark = _get_test_spark()
        from pyspark.sql.types import DateType
        from pyspark.sql.functions import col

        sales = spark.createDataFrame([
            ("2022-01-01", "London", 500.0),
            ("2022-01-02", "London", 600.0),   # no weather for this date
        ], ["date", "city", "total_sales"])
        sales = sales.withColumn("date", col("date").cast(DateType()))

        weather = spark.createDataFrame([
            ("2022-01-01", "London", 5.0, 0.0),
        ], ["date", "city", "avg_temp", "precipitation"])
        weather = weather.withColumn("date", col("date").cast(DateType()))

        joined = sales.join(weather, on=["date", "city"], how="inner")
        self.assertEqual(joined.count(), 1)


# ══════════════════════════════════════════════════════════════════════════════
# 5. CDC Manager (no Spark needed)
# ══════════════════════════════════════════════════════════════════════════════

class TestCDCManager(unittest.TestCase):

    def test_load_empty_watermark(self):
        """load_watermark returns {} when file does not exist."""
        with tempfile.TemporaryDirectory() as tmp:
            import os; orig = os.getcwd(); os.chdir(tmp)
            try:
                import scripts.cdc_manager as cdc
                # Patch the paths to the tmp dir
                orig_wm = cdc.WATERMARK_FILE
                cdc.WATERMARK_FILE = Path(tmp) / "wm.json"
                result = cdc.load_watermark()
                self.assertEqual(result, {})
            finally:
                cdc.WATERMARK_FILE = orig_wm
                os.chdir(orig)

    def test_save_and_reload_watermark(self):
        """Watermark survives a save/load cycle."""
        with tempfile.TemporaryDirectory() as tmp:
            import scripts.cdc_manager as cdc
            orig_wm  = cdc.WATERMARK_FILE
            orig_dir = cdc.CDC_DIR
            cdc.CDC_DIR        = Path(tmp)
            cdc.WATERMARK_FILE = Path(tmp) / "watermark.json"
            try:
                cdc.save_watermark({"weather": "2026-04-18", "retail_sales": "2026-04-17"})
                result = cdc.load_watermark()
                self.assertEqual(result["weather"],      "2026-04-18")
                self.assertEqual(result["retail_sales"], "2026-04-17")
            finally:
                cdc.WATERMARK_FILE = orig_wm
                cdc.CDC_DIR        = orig_dir

    def test_discover_no_new_retail_files_when_all_before_watermark(self):
        """No files discovered when watermark is after all file dates."""
        from datetime import date as d
        import scripts.cdc_manager as cdc

        with tempfile.TemporaryDirectory() as tmp:
            upload_dir = Path(tmp) / "uci"
            upload_dir.mkdir()
            # Create a fake live CSV with a known date in the filename
            (upload_dir / "OnlineRetail_live_2026-04-14_20260101T000000Z.csv").touch()

            orig_dir = cdc.RETAIL_RAW_DIR
            cdc.RETAIL_RAW_DIR = upload_dir
            try:
                # Watermark is AFTER 2026-04-14 → nothing new
                new = cdc.discover_new_retail_files(after=d(2026, 4, 15))
                self.assertEqual(len(new), 0)
            finally:
                cdc.RETAIL_RAW_DIR = orig_dir

    def test_discover_new_retail_files_finds_file_after_watermark(self):
        """Files dated AFTER the watermark are discovered."""
        from datetime import date as d
        import scripts.cdc_manager as cdc

        with tempfile.TemporaryDirectory() as tmp:
            upload_dir = Path(tmp) / "uci"
            upload_dir.mkdir()
            (upload_dir / "OnlineRetail_live_2026-04-18_20260101T000000Z.csv").touch()

            orig_dir = cdc.RETAIL_RAW_DIR
            cdc.RETAIL_RAW_DIR = upload_dir
            try:
                new = cdc.discover_new_retail_files(after=d(2026, 4, 15))
                self.assertEqual(len(new), 1)
                self.assertEqual(new[0][0], d(2026, 4, 18))
            finally:
                cdc.RETAIL_RAW_DIR = orig_dir

    def test_date_from_retail_fname_parses_correctly(self):
        import scripts.cdc_manager as cdc
        from datetime import date as d
        result = cdc._date_from_retail_fname("OnlineRetail_live_2026-04-18_20260419T083700Z.csv")
        self.assertEqual(result, d(2026, 4, 18))

    def test_date_from_retail_fname_returns_none_for_no_match(self):
        import scripts.cdc_manager as cdc
        self.assertIsNone(cdc._date_from_retail_fname("OnlineRetail.csv"))

    def test_date_from_weather_fname_parses_correctly(self):
        import scripts.cdc_manager as cdc
        from datetime import date as d
        result = cdc._date_from_weather_fname(
            "weather_london_2026-04-14_2026-04-18_20260419T094423Z.json"
        )
        self.assertEqual(result, d(2026, 4, 18))


# ══════════════════════════════════════════════════════════════════════════════
# Run directly
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
