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
