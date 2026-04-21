# tests/conftest.py
"""
Pytest configuration for the capstone test suite.

Provides:
  - sys.path setup so all imports resolve from project root
  - A session-scoped SparkSession fixture shared across all tests
    (starts the JVM only once per pytest session)
"""

import sys
from pathlib import Path

import pytest

# ── Ensure project root is on sys.path ───────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ── Session-scoped SparkSession (JVM starts once per pytest run) ─────────────

@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for the entire test session.
    All Spark-dependent tests should request this fixture.

    Usage:
        def test_something(spark):
            df = spark.createDataFrame(...)
    """
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder
            .appName("CapstoneTestSuite")
            .master("local[2]")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "4")   # fast for small data
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()

    except Exception as exc:
        pytest.skip(f"SparkSession unavailable: {exc}")
