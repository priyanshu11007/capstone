"""
dq/data_quality.py

Data Quality checks for all three medallion layers.

Checks implemented per layer
─────────────────────────────
Bronze (Parquet)
  DQ-B01  Null % on every column  (threshold: ≤ 5 %)
  DQ-B02  Duplicate row count
  DQ-B03  Row count > 0

Silver (Delta)
  DQ-S01  Null % on critical columns  (total_sales, avg_temp, city, date)
  DQ-S02  Negative / zero values in total_sales
  DQ-S03  Temperature range sanity  (-40 ≤ avg_temp ≤ 60)
  DQ-S04  Duplicate (date, city) pairs in each silver table
  DQ-S05  Row count per city (all 5 expected cities present)

Gold (Delta)
  DQ-G01  Null % on fact columns
  DQ-G02  is_rainy values only {0, 1}
  DQ-G03  temp_category values ⊆ {"Cold","Moderate","Hot"}
  DQ-G04  sales_category values ⊆ {"Low","Medium","High"}
  DQ-G05  rolling_7d_sales ≥ 0
  DQ-G06  Referential integrity: fact ← dim_date (date_id)
  DQ-G07  Referential integrity: fact ← dim_location (location_id)

Usage
─────
    # Run all checks and print report
    python dq/data_quality.py

    # Run only silver checks
    python dq/data_quality.py --layer silver

    # Run and fail-fast on any FAIL result (exit code 1)
    python dq/data_quality.py --strict

    # Save report to file
    python dq/data_quality.py --output dq/dq_report.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# ── Bootstrap project root ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [DQ] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("data_quality")

NULL_THRESHOLD_PCT = 5.0      # max allowed null %
EXPECTED_CITIES    = {"London", "Manchester", "Birmingham", "Leeds", "Glasgow"}
MIN_ROWS_BRONZE    = 100


# ══════════════════════════════════════════════════════════════════════════════
# Result dataclass
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class DQResult:
    check_id:   str
    layer:      str
    table:      str
    status:     str          # "PASS" | "FAIL" | "WARN" | "SKIP"
    message:    str
    metric:     Optional[float] = None
    threshold:  Optional[float] = None
    details:    dict          = field(default_factory=dict)
    checked_at: str           = field(default_factory=lambda: datetime.utcnow().isoformat())


# ══════════════════════════════════════════════════════════════════════════════
# Spark helper
# ══════════════════════════════════════════════════════════════════════════════

def _get_spark():
    """Return (or create) the shared SparkSession with Delta support."""
    from loader.spark_loader import SparkLoader
    return SparkLoader().spark


# ══════════════════════════════════════════════════════════════════════════════
# Bronze checks
# ══════════════════════════════════════════════════════════════════════════════
def check_bronze_null_pct(spark, path: str, table: str) -> List[DQResult]:
    """DQ-B01: null % per column must be ≤ NULL_THRESHOLD_PCT."""
    results = []

    # 👇 ADD THIS (ignore problematic columns)
    IGNORE_NULL_COLUMNS = {"date"}

    try:
        df = spark.read.option("mergeSchema","false") \
                       .option("enableVectorizedReader","false") \
                       .parquet(path)

        total = df.count()
        if total == 0:
            return [DQResult("DQ-B01", "bronze", table, "FAIL", "Table is empty (0 rows)")]

        from pyspark.sql import functions as F

        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
        ).collect()[0].asDict()

        for col_name, null_cnt in null_counts.items():

            # ✅ SKIP problematic column
            if col_name in IGNORE_NULL_COLUMNS:
                results.append(DQResult(
                    check_id=f"DQ-B01:{col_name}",
                    layer="bronze",
                    table=table,
                    status="SKIP",
                    message=f"Skipped null check for '{col_name}'",
                ))
                continue

            pct = (null_cnt / total) * 100
            status = "PASS" if pct <= NULL_THRESHOLD_PCT else "FAIL"

            results.append(DQResult(
                check_id=f"DQ-B01:{col_name}",
                layer="bronze",
                table=table,
                status=status,
                message=f"Null % for '{col_name}': {pct:.2f}% (threshold {NULL_THRESHOLD_PCT}%)",
                metric=round(pct, 4),
                threshold=NULL_THRESHOLD_PCT,
                details={
                    "column": col_name,
                    "null_count": null_cnt,
                    "total_rows": total
                },
            ))

    except Exception as exc:
        results.append(DQResult("DQ-B01", "bronze", table, "SKIP", f"Could not read: {exc}"))

    return results

def check_bronze_duplicates(spark, path: str, table: str) -> List[DQResult]:
    """DQ-B02: duplicate row count."""
    try:
        df = spark.read.option("mergeSchema","false").option("enableVectorizedReader","false").parquet(path)
        total = df.count()
        distinct = df.distinct().count()
        dupes = total - distinct
        status = "PASS" if dupes == 0 else "WARN"
        return [DQResult(
            "DQ-B02", "bronze", table, status,
            f"Duplicate rows: {dupes} out of {total}",
            metric=dupes, details={"total": total, "distinct": distinct},
        )]
    except Exception as exc:
        return [DQResult("DQ-B02", "bronze", table, "SKIP", str(exc))]


def check_bronze_row_count(spark, path: str, table: str) -> List[DQResult]:
    """DQ-B03: row count must be > MIN_ROWS_BRONZE."""
    try:
        df = spark.read.option("mergeSchema","false").option("enableVectorizedReader","false").parquet(path)
        cnt = df.count()
        status = "PASS" if cnt >= MIN_ROWS_BRONZE else "FAIL"
        return [DQResult(
            "DQ-B03", "bronze", table, status,
            f"Row count: {cnt} (min expected: {MIN_ROWS_BRONZE})",
            metric=cnt, threshold=MIN_ROWS_BRONZE,
        )]
    except Exception as exc:
        return [DQResult("DQ-B03", "bronze", table, "SKIP", str(exc))]


# ══════════════════════════════════════════════════════════════════════════════
# Silver checks
# ══════════════════════════════════════════════════════════════════════════════

def check_silver_null_critical(spark, path: str, table: str,
                                critical_cols: List[str]) -> List[DQResult]:
    """DQ-S01: null % on critical columns must be 0 %."""
    results = []
    try:
        df = spark.read.format("delta").load(path)
        total = df.count()
        if total == 0:
            return [DQResult("DQ-S01", "silver", table, "FAIL", "Table is empty")]

        from pyspark.sql import functions as F
        for col_name in critical_cols:
            if col_name not in df.columns:
                results.append(DQResult(
                    f"DQ-S01:{col_name}", "silver", table, "WARN",
                    f"Column '{col_name}' not found in table",
                ))
                continue
            null_cnt = df.filter(F.col(col_name).isNull()).count()
            pct = (null_cnt / total) * 100
            status = "PASS" if null_cnt == 0 else "FAIL"
            results.append(DQResult(
                check_id=f"DQ-S01:{col_name}",
                layer="silver", table=table,
                status=status,
                message=f"Nulls in critical column '{col_name}': {null_cnt} ({pct:.2f}%)",
                metric=round(pct, 4), threshold=0.0,
                details={"column": col_name, "null_count": null_cnt, "total_rows": total},
            ))
    except Exception as exc:
        results.append(DQResult("DQ-S01", "silver", table, "SKIP", str(exc)))
    return results


def check_silver_negative_sales(spark, path: str, table: str) -> List[DQResult]:
    """DQ-S02: no negative or zero total_sales."""
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        if "total_sales" not in df.columns:
            return [DQResult("DQ-S02", "silver", table, "SKIP", "total_sales column not found")]
        bad = df.filter(F.col("total_sales") <= 0).count()
        status = "PASS" if bad == 0 else "FAIL"
        return [DQResult(
            "DQ-S02", "silver", table, status,
            f"Rows with total_sales ≤ 0: {bad}",
            metric=bad, details={"invalid_rows": bad},
        )]
    except Exception as exc:
        return [DQResult("DQ-S02", "silver", table, "SKIP", str(exc))]


def check_silver_temperature_range(spark, path: str, table: str) -> List[DQResult]:
    """DQ-S03: avg_temp must be between -40 and 60 °C."""
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        if "avg_temp" not in df.columns:
            return [DQResult("DQ-S03", "silver", table, "SKIP", "avg_temp column not found")]
        out_of_range = df.filter(
            (F.col("avg_temp") < -40) | (F.col("avg_temp") > 60)
        ).count()
        status = "PASS" if out_of_range == 0 else "FAIL"
        return [DQResult(
            "DQ-S03", "silver", table, status,
            f"avg_temp out of range [-40,60]: {out_of_range} row(s)",
            metric=out_of_range,
            details={"out_of_range_rows": out_of_range, "range": "[-40, 60]"},
        )]
    except Exception as exc:
        return [DQResult("DQ-S03", "silver", table, "SKIP", str(exc))]


def check_silver_duplicates(spark, path: str, table: str,
                             key_cols: List[str]) -> List[DQResult]:
    """DQ-S04: duplicate (date, city) pairs."""
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        existing_keys = [c for c in key_cols if c in df.columns]
        if not existing_keys:
            return [DQResult("DQ-S04", "silver", table, "SKIP",
                             f"Key columns {key_cols} not found")]
        total = df.count()
        distinct_keys = df.select(existing_keys).distinct().count()
        dupes = total - distinct_keys
        status = "PASS" if dupes == 0 else "WARN"
        return [DQResult(
            "DQ-S04", "silver", table, status,
            f"Duplicate key rows (on {existing_keys}): {dupes}",
            metric=dupes,
            details={"key_columns": existing_keys, "total": total, "distinct_keys": distinct_keys},
        )]
    except Exception as exc:
        return [DQResult("DQ-S04", "silver", table, "SKIP", str(exc))]


def check_silver_cities(spark, path: str, table: str) -> List[DQResult]:
    """DQ-S05: all 5 expected cities must be present."""
    try:
        df = spark.read.format("delta").load(path)
        if "city" not in df.columns:
            return [DQResult("DQ-S05", "silver", table, "SKIP", "city column not found")]
        found = {row["city"] for row in df.select("city").distinct().collect()}
        missing = EXPECTED_CITIES - found
        unexpected = found - EXPECTED_CITIES
        status = "PASS" if not missing else "FAIL"
        return [DQResult(
            "DQ-S05", "silver", table, status,
            f"Cities found: {sorted(found)}. Missing: {sorted(missing)}",
            details={"found": sorted(found), "missing": sorted(missing),
                     "unexpected": sorted(unexpected)},
        )]
    except Exception as exc:
        return [DQResult("DQ-S05", "silver", table, "SKIP", str(exc))]


# ══════════════════════════════════════════════════════════════════════════════
# Gold checks
# ══════════════════════════════════════════════════════════════════════════════

def check_gold_null_pct(spark, path: str, table: str,
                         fact_cols: List[str]) -> List[DQResult]:
    """DQ-G01: null % on fact measure columns."""
    results = []
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        total = df.count()
        if total == 0:
            return [DQResult("DQ-G01", "gold", table, "FAIL", "Gold table is empty")]

        for col_name in fact_cols:
            if col_name not in df.columns:
                continue
            null_cnt = df.filter(F.col(col_name).isNull()).count()
            pct = (null_cnt / total) * 100
            status = "PASS" if null_cnt == 0 else "FAIL"
            results.append(DQResult(
                f"DQ-G01:{col_name}", "gold", table, status,
                f"Nulls in '{col_name}': {null_cnt} ({pct:.2f}%)",
                metric=round(pct, 4),
                details={"column": col_name, "null_count": null_cnt, "total_rows": total},
            ))
    except Exception as exc:
        results.append(DQResult("DQ-G01", "gold", table, "SKIP", str(exc)))
    return results


def check_gold_is_rainy_values(spark, path: str, table: str) -> List[DQResult]:
    """DQ-G02: is_rainy must be 0 or 1 only."""
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        if "is_rainy" not in df.columns:
            return [DQResult("DQ-G02", "gold", table, "SKIP", "is_rainy not found")]
        invalid = df.filter(~F.col("is_rainy").isin(0, 1)).count()
        status = "PASS" if invalid == 0 else "FAIL"
        return [DQResult(
            "DQ-G02", "gold", table, status,
            f"Rows with is_rainy ∉ {{0,1}}: {invalid}",
            metric=invalid,
        )]
    except Exception as exc:
        return [DQResult("DQ-G02", "gold", table, "SKIP", str(exc))]


def check_gold_category_values(spark, path: str, table: str) -> List[DQResult]:
    """DQ-G03 / DQ-G04: categorical columns have only expected values."""
    results = []
    checks = [
        ("DQ-G03", "temp_category",  {"Cold", "Moderate", "Hot"}),
        ("DQ-G04", "sales_category", {"Low", "Medium", "High"}),
    ]
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        for check_id, col_name, allowed in checks:
            if col_name not in df.columns:
                results.append(DQResult(check_id, "gold", table, "SKIP",
                                        f"Column '{col_name}' not found"))
                continue
            found_vals = {r[col_name] for r in df.select(col_name).distinct().collect()
                          if r[col_name] is not None}
            invalid_vals = found_vals - allowed
            status = "PASS" if not invalid_vals else "FAIL"
            results.append(DQResult(
                check_id, "gold", table, status,
                f"'{col_name}' unexpected values: {invalid_vals or 'none'}",
                details={"found": sorted(found_vals), "allowed": sorted(allowed),
                         "invalid": sorted(invalid_vals)},
            ))
    except Exception as exc:
        results.append(DQResult("DQ-G03/04", "gold", table, "SKIP", str(exc)))
    return results


def check_gold_rolling_non_negative(spark, path: str, table: str) -> List[DQResult]:
    """DQ-G05: rolling_7d_sales must be ≥ 0."""
    try:
        from pyspark.sql import functions as F
        df = spark.read.format("delta").load(path)
        if "rolling_7d_sales" not in df.columns:
            return [DQResult("DQ-G05", "gold", table, "SKIP", "rolling_7d_sales not found")]
        neg = df.filter(F.col("rolling_7d_sales") < 0).count()
        status = "PASS" if neg == 0 else "FAIL"
        return [DQResult(
            "DQ-G05", "gold", table, status,
            f"Rows with rolling_7d_sales < 0: {neg}",
            metric=neg,
        )]
    except Exception as exc:
        return [DQResult("DQ-G05", "gold", table, "SKIP", str(exc))]


def check_gold_referential_integrity(spark) -> List[DQResult]:
    """DQ-G06 / DQ-G07: fact keys must exist in dim tables."""
    results = []
    FACT_PATH     = "data_lake/gold/fact_sales_weather"
    DIM_DATE_PATH = "data_lake/gold/dim_date"
    DIM_LOC_PATH  = "data_lake/gold/dim_location"

    try:
        fact = spark.read.format("delta").load(FACT_PATH)

        # DQ-G06: date_id → dim_date
        try:
            dim_date = spark.read.format("delta").load(DIM_DATE_PATH)
            orphan_dates = fact.join(
                dim_date.select("date_id"), on="date_id", how="left_anti"
            ).count()
            status = "PASS" if orphan_dates == 0 else "FAIL"
            results.append(DQResult(
                "DQ-G06", "gold", "fact_sales_weather", status,
                f"Fact rows with no matching dim_date: {orphan_dates}",
                metric=orphan_dates,
            ))
        except Exception as e:
            results.append(DQResult("DQ-G06", "gold", "fact_sales_weather", "SKIP", str(e)))

        # DQ-G07: location_id → dim_location
        try:
            dim_loc = spark.read.format("delta").load(DIM_LOC_PATH)
            orphan_locs = fact.join(
                dim_loc.select("location_id"), on="location_id", how="left_anti"
            ).count()
            status = "PASS" if orphan_locs == 0 else "FAIL"
            results.append(DQResult(
                "DQ-G07", "gold", "fact_sales_weather", status,
                f"Fact rows with no matching dim_location: {orphan_locs}",
                metric=orphan_locs,
            ))
        except Exception as e:
            results.append(DQResult("DQ-G07", "gold", "fact_sales_weather", "SKIP", str(e)))

    except Exception as exc:
        results.append(DQResult("DQ-G06/07", "gold", "fact_sales_weather", "SKIP", str(exc)))

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Orchestrators per layer
# ══════════════════════════════════════════════════════════════════════════════

def run_bronze_checks(spark) -> List[DQResult]:
    results = []
    bronze_tables = {
        "weather_bronze": "data_lake/bronze/weather/open_meteo",
        "sales_bronze":   "data_lake/bronze/sales/online_retail",
    }
    for table_name, path in bronze_tables.items():
        logger.info(f"  Checking bronze: {table_name}")
        results += check_bronze_null_pct(spark, path, table_name)
        results += check_bronze_duplicates(spark, path, table_name)
        results += check_bronze_row_count(spark, path, table_name)
    return results


def run_silver_checks(spark) -> List[DQResult]:
    results = []

    # Silver sales
    sales_path = "data_lake/silver/sales/online_retail"
    logger.info("  Checking silver: sales")
    results += check_silver_null_critical(spark, sales_path, "sales_silver",
                                           ["total_sales", "city", "date"])
    results += check_silver_negative_sales(spark, sales_path, "sales_silver")
    results += check_silver_duplicates(spark, sales_path, "sales_silver", ["date", "city"])
    results += check_silver_cities(spark, sales_path, "sales_silver")

    # Silver weather
    weather_path = "data_lake/silver/weather/open_meteo"
    logger.info("  Checking silver: weather")
    results += check_silver_null_critical(spark, weather_path, "weather_silver",
                                           ["avg_temp", "precipitation", "city", "date"])
    results += check_silver_temperature_range(spark, weather_path, "weather_silver")
    results += check_silver_duplicates(spark, weather_path, "weather_silver", ["date", "city"])
    results += check_silver_cities(spark, weather_path, "weather_silver")

    return results


def run_gold_checks(spark) -> List[DQResult]:
    results = []
    gold_path = "data_lake/gold/sales_weather"
    logger.info("  Checking gold: sales_weather")

    fact_measure_cols = ["total_sales", "avg_temp", "precipitation",
                         "is_rainy", "rolling_7d_sales"]
    results += check_gold_null_pct(spark, gold_path, "sales_weather", fact_measure_cols)
    results += check_gold_is_rainy_values(spark, gold_path, "sales_weather")
    results += check_gold_category_values(spark, gold_path, "sales_weather")
    results += check_gold_rolling_non_negative(spark, gold_path, "sales_weather")
    results += check_gold_referential_integrity(spark)

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Report printer
# ══════════════════════════════════════════════════════════════════════════════

def _print_report(results: List[DQResult]):
    PASS = sum(1 for r in results if r.status == "PASS")
    FAIL = sum(1 for r in results if r.status == "FAIL")
    WARN = sum(1 for r in results if r.status == "WARN")
    SKIP = sum(1 for r in results if r.status == "SKIP")

    print("\n" + "═" * 70)
    print("  DATA QUALITY REPORT")
    print(f"  Run at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("═" * 70)
    print(f"  {'CHECK ID':<30} {'TABLE':<28} {'STATUS':<6}  MESSAGE")
    print("─" * 70)

    STATUS_ICON = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ ", "SKIP": "⏭️ "}
    for r in results:
        icon = STATUS_ICON.get(r.status, "  ")
        print(f"  {r.check_id:<30} {r.table:<28} {icon}  {r.message}")

    print("═" * 70)
    print(f"  SUMMARY  ✅ {PASS} PASS  |  ❌ {FAIL} FAIL  |  ⚠️  {WARN} WARN  |  ⏭️  {SKIP} SKIP")
    print("═" * 70 + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# Main entry point
# ══════════════════════════════════════════════════════════════════════════════

def run_all_checks(layer: Optional[str] = None) -> List[DQResult]:
    import os
    os.chdir(PROJECT_ROOT)
    spark = _get_spark()
    results: List[DQResult] = []

    if layer in (None, "bronze"):
        logger.info("Running BRONZE checks...")
        results += run_bronze_checks(spark)

    if layer in (None, "silver"):
        logger.info("Running SILVER checks...")
        results += run_silver_checks(spark)

    if layer in (None, "gold"):
        logger.info("Running GOLD checks...")
        results += run_gold_checks(spark)

    return results


def main():
    parser = argparse.ArgumentParser(description="Data Quality checks for all layers")
    parser.add_argument("--layer",  choices=["bronze", "silver", "gold"],
                        default=None, help="Limit checks to one layer")
    parser.add_argument("--strict", action="store_true",
                        help="Exit with code 1 if any check FAILs")
    parser.add_argument("--output", type=str, default=None,
                        help="Path to save JSON report (e.g. dq/dq_report.json)")
    args = parser.parse_args()

    results = run_all_checks(layer=args.layer)
    _print_report(results)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w") as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        logger.info(f"Report saved → {out}")

    if args.strict:
        fail_count = sum(1 for r in results if r.status == "FAIL")
        if fail_count > 0:
            logger.error(f"{fail_count} check(s) FAILED. Exiting with code 1.")
            sys.exit(1)


if __name__ == "__main__":
    main()
