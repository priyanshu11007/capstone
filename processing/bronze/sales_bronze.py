"""
scripts/run_live_pipeline.py

One-stop runner for the complete live data lifecycle.
Integrates with your existing main.py pipeline.

Stages
------
1. generate  – Create today's live data files (weather JSON + retail CSV)
2. cdc       – Detect new files, update watermarks, trigger bronze
3. full      – generate + cdc (typical daily cron job)
4. backfill  – Seed the last 5 days of live data (run once at setup)
5. status    – Show pipeline state (watermarks, pending files)

Usage
-----
    # ONE-TIME SETUP: seed the last 5 days and run CDC
    python scripts/run_live_pipeline.py --stage backfill

    # DAILY CRON: generate tomorrow's data and run CDC
    python scripts/run_live_pipeline.py --stage full

    # Just generate files (no CDC)
    python scripts/run_live_pipeline.py --stage generate

    # Just run CDC (files already exist)
    python scripts/run_live_pipeline.py --stage cdc

    # See current state
    python scripts/run_live_pipeline.py --stage status

    # Generate + CDC for a specific date (testing)
    python scripts/run_live_pipeline.py --stage full --date 2026-04-21
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Import our live generators
from scripts.generate_live_weather import run_backfill as weather_backfill
from scripts.generate_live_weather import run_daily   as weather_daily
from scripts.generate_live_retail  import run_backfill as retail_backfill
from scripts.generate_live_retail  import run_daily   as retail_daily
from scripts.cdc_manager import (
    run_cdc_weather,
    run_cdc_retail,
    show_watermark_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [LivePipeline] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("live_pipeline")

BANNER = "=" * 65


def stage_backfill():
    """
    ONE-TIME SETUP: generate last 5 days of live data for both
    weather and retail, then run CDC to process them into bronze.
    """
    print(BANNER)
    print("  STAGE: BACKFILL (last 5 days)")
    print(BANNER)

    logger.info("Step 1/4 – Generating backfill weather data (5 days × 5 cities)")
    weather_backfill()

    logger.info("Step 2/4 – Generating backfill retail data (5 days × 500 records)")
    retail_backfill()

    logger.info("Step 3/4 – Running CDC for weather")
    run_cdc_weather(dry_run=False)

    logger.info("Step 4/4 – Running CDC for retail_sales")
    run_cdc_retail(dry_run=False)

    print(BANNER)
    print("  BACKFILL COMPLETE")
    print(f"  Weather : last 5 days generated & processed")
    print(f"  Retail  : last 5 days × 500 records generated & processed")
    print(f"  Watermarks updated for both entities.")
    print(BANNER)


def stage_generate(target_date: date = None):
    """Generate live data files for tomorrow (or target_date)."""
    print(BANNER)
    print("  STAGE: GENERATE")
    print(BANNER)

    logger.info("Generating live weather data...")
    weather_daily(target_date)

    logger.info("Generating live retail data...")
    retail_daily(target_date)

    print(BANNER)
    print("  GENERATE COMPLETE")
    print(BANNER)


def stage_cdc(dry_run: bool = False):
    """Run CDC for both entities."""
    print(BANNER)
    print("  STAGE: CDC (Incremental Ingestion)")
    print(BANNER)

    run_cdc_weather(dry_run=dry_run)
    run_cdc_retail(dry_run=dry_run)

    print(BANNER)
    print("  CDC COMPLETE")
    print(BANNER)


def stage_full(target_date: date = None, dry_run: bool = False):
    """Generate + CDC – typical daily scheduled run."""
    print(BANNER)
    print("  STAGE: FULL (generate + CDC)")
    print(BANNER)

    stage_generate(target_date)
    stage_cdc(dry_run=dry_run)

    print(BANNER)
    print("  FULL PIPELINE COMPLETE")
    print(BANNER)


def stage_status():
    show_watermark_status()


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Live Pipeline Runner – Generates + CDCs live weather & retail data"
    )
    parser.add_argument(
        "--stage",
        choices=["backfill", "generate", "cdc", "full", "status"],
        required=True,
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Target date (YYYY-MM-DD) for generate/full. Default = tomorrow.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run CDC in dry-run mode (no watermark updates, no bronze trigger).",
    )
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else None

    if args.stage == "backfill":
        stage_backfill()
    elif args.stage == "generate":
        stage_generate(target)
    elif args.stage == "cdc":
        stage_cdc(dry_run=args.dry_run)
    elif args.stage == "full":
        stage_full(target, dry_run=args.dry_run)
    elif args.stage == "status":
        stage_status()


if __name__ == "__main__":
    main()


"""
processing/bronze/sales_bronze.py

Reads historical retail Parquet files from raw layer and writes
cleaned, partitioned Parquet to bronze.

Schema strategy
---------------
After repair_raw_parquet.py is run, every raw file has:
  InvoiceNo    → str  (large_string in Parquet)
  StockCode    → str
  Description  → str
  Quantity     → int32   (Parquet INT32 — matches Spark IntegerType)
  InvoiceDate  → str     (will be parsed by to_timestamp)
  UnitPrice    → float64 (Parquet DOUBLE)
  CustomerID   → float64
  Country      → str

We read WITHOUT a declared schema (letting Spark infer from the now-clean
files) and then immediately cast every column we use to its canonical type.
This is robust against any residual variation and avoids the
INT64 vs INT32 conflict entirely at the read stage.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import StringType, IntegerType, DoubleType
from ingestion.base import BaseExtractor


class SalesBronzeTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path="", layer="bronze")
        self.spark = (
            SparkSession.builder
            .appName("Sales Bronze Transformation")
            .config("spark.sql.parquet.enableVectorizedReader", "false")  # allows INT64→int cast
            .getOrCreate()
        )

    def extract(self, **kwargs):

        raw_path = "data_lake/raw/retail_sales/uci_online_retail"

        self.logger.info("Reading raw parquet files using Spark...")

        # Read with vectorized reader OFF so Spark can tolerate INT64 in an
        # int32 column (belt-and-suspenders alongside the repair script).
        df = (
            self.spark.read
            .option("mergeSchema", "false")
            .option("enableVectorizedReader", "false")
            .parquet(raw_path)
        )

        # ── Cast every column to canonical type immediately after read ───
        df = df.withColumn("InvoiceNo",   col("InvoiceNo").cast(StringType()))
        df = df.withColumn("StockCode",   col("StockCode").cast(StringType()))
        df = df.withColumn("Description", col("Description").cast(StringType()))
        df = df.withColumn("Country",     col("Country").cast(StringType()))
        df = df.withColumn("Quantity",    col("Quantity").cast(IntegerType()))
        df = df.withColumn("UnitPrice",   col("UnitPrice").cast(DoubleType()))
        df = df.withColumn("CustomerID",  col("CustomerID").cast(DoubleType()))

        # ── Cleaning ─────────────────────────────────────────────────────
        df = df.filter(col("CustomerID").isNotNull())
        df = df.withColumn("CustomerID", col("CustomerID").cast(IntegerType()))

        # InvoiceDate may be string (after repair) or timestamp (old files).
        # Cast to string first to normalise, then parse.
        df = df.withColumn("InvoiceDate", col("InvoiceDate").cast(StringType()))
        df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate")))

        # ── City assignment ──────────────────────────────────────────────
        cities = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]
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

        # ── Date alignment (shift UCI 2010-2011 → 2022-2023) ────────────
        df = df.withColumn("InvoiceDate", expr("add_months(InvoiceDate, 12*12)"))
        df = df.withColumn("date", col("InvoiceDate").cast("date"))

        # ── Select output columns ────────────────────────────────────────
        country_col = "Country" if "Country" in df.columns else "country"
        df = df.select(
            col("InvoiceNo").cast(StringType()),
            col("StockCode").cast(StringType()),
            col("Description").cast(StringType()),
            col("Quantity").cast(IntegerType()),
            col("UnitPrice").cast(DoubleType()),
            col("CustomerID").cast(IntegerType()),
            col(country_col).cast(StringType()).alias("Country"),
            col("city").cast(StringType()),
            col("date"),
        )

        self.logger.info(f"Bronze row count: {df.count()}")

        output_path = "data_lake/bronze/sales/online_retail"
        (
            df.write
            .mode("overwrite")
            .partitionBy("city")
            .parquet(output_path)
        )

        self.logger.info(f"✔ Written Bronze data → {output_path}")
        return output_path

    def validate(self, data):
        return True
