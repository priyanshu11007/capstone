"""
main.py
Unified entry point for the Global Retail & Weather Analytics Platform.

────────────────────────────────────────────────────────────────────
HISTORICAL PIPELINE  (2022-01-01 → 2024-12-31)
────────────────────────────────────────────────────────────────────
  python main.py --entity weather           # Ingest Open-Meteo JSON
  python main.py --entity retail_sales      # Ingest UCI CSV
  python main.py --entity weather_bronze    # Raw JSON → bronze Parquet
  python main.py --entity sales_bronze      # Raw Parquet → bronze Parquet
  python main.py --entity weather_silver    # Bronze → silver Delta
  python main.py --entity sales_silver      # Bronze → silver Delta
  python main.py --entity gold              # Silver → gold Delta (star schema)
  python main.py --entity dim_date          # Build dim_date
  python main.py --entity dim_location      # Build dim_location
  python main.py --entity fact              # Build fact_sales_weather
  python main.py --all                      # Run full historical pipeline

────────────────────────────────────────────────────────────────────
LIVE PIPELINE  (last 5 days + daily onwards)  – CDC-driven
────────────────────────────────────────────────────────────────────
  python main.py --live backfill            # ONE-TIME: seed 5 days + full stack
  python main.py --live daily               # DAILY CRON: generate tomorrow + CDC
  python main.py --live generate            # Generate files only (no CDC)
  python main.py --live cdc                 # CDC + rebuild live medallion stack
  python main.py --live status              # Show watermark state
  python main.py --live cdc --dry-run       # Dry-run CDC
  python main.py --live daily --date 2026-04-25

────────────────────────────────────────────────────────────────────
ANALYTICS CLI
────────────────────────────────────────────────────────────────────
  python analysis/cli_app.py                # Interactive menu (H / L selector)
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from ingestion.api_extractor import APIExtractor
from ingestion.file_extractor import FileExtractor
from processing.bronze.sales_bronze         import SalesBronzeTransformer
from processing.bronze.weather_bronze       import WeatherBronzeTransformer
from processing.bronze.sales_bronze_live    import SalesBronzeLiveTransformer
from processing.bronze.weather_bronze_live  import WeatherBronzeLiveTransformer
from processing.silver.sales_silver         import SalesSilverTransformer
from processing.silver.weather_silver       import WeatherSilverTransformer
from processing.silver.sales_silver_live    import SalesSilverLiveTransformer
from processing.silver.weather_silver_live  import WeatherSilverLiveTransformer
from processing.gold.sales_weather_gold      import SalesWeatherGold
from processing.gold.sales_weather_gold_live import SalesWeatherGoldLive
from processing.gold.dim_date               import DimDate
from processing.gold.dim_location           import DimLocation
from processing.gold.fact_sales_weather     import FactSalesWeather
from processing.gold.dim_product import DimProduct
from processing.gold.fact_sales_weather_product import FactSalesWeatherProduct


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/ingestion.log", mode="a"),
    ],
)
logger = logging.getLogger("main")
BANNER = "=" * 62


# ════════════════════════════════════════════════════════════════
# HISTORICAL PIPELINE
# ════════════════════════════════════════════════════════════════

def run_weather():
    logger.info(BANNER)
    logger.info("WEATHER ingestion  (Open-Meteo historical)")
    logger.info(BANNER)
    extractor = APIExtractor(config_path="configs/api_config.yaml")
    written   = extractor.extract(entity="weather")
    logger.info(f"Done. {len(written)} file(s) written.")
    return written


def run_retail(file_name=None):
    logger.info(BANNER)
    logger.info("RETAIL SALES ingestion  (manual upload)")
    logger.info(BANNER)
    extractor = FileExtractor(config_path="configs/api_config.yaml")
    written   = extractor.extract(
        entity="retail_sales",
        source_name="uci_online_retail",
        file_name=file_name,
        output_format="parquet",
    )
    logger.info(f"Done. {len(written)} file(s) written.")
    return written


def run_weather_bronze():
    logger.info(BANNER); logger.info("WEATHER BRONZE"); logger.info(BANNER)
    return WeatherBronzeTransformer().extract()

def run_sales_bronze():
    logger.info(BANNER); logger.info("SALES BRONZE"); logger.info(BANNER)
    return SalesBronzeTransformer().extract()

def run_sales_silver():
    logger.info(BANNER); logger.info("SALES SILVER"); logger.info(BANNER)
    return SalesSilverTransformer().extract()

def run_weather_silver():
    logger.info(BANNER); logger.info("WEATHER SILVER"); logger.info(BANNER)
    return WeatherSilverTransformer().extract()

def run_gold():
    logger.info(BANNER); logger.info("GOLD  (sales_weather)"); logger.info(BANNER)
    return SalesWeatherGold().extract()

def run_dim_date():
    logger.info(BANNER); logger.info("DIM DATE"); logger.info(BANNER)
    return DimDate().extract()

def run_dim_location():
    logger.info(BANNER); logger.info("DIM LOCATION"); logger.info(BANNER)
    return DimLocation().extract()

def run_fact():
    logger.info(BANNER); logger.info("FACT SALES WEATHER"); logger.info(BANNER)
    return FactSalesWeather().extract()

def run_dim_product():
    logger.info(BANNER); logger.info("DIM PRODUCT"); logger.info(BANNER)
    return DimProduct().extract()

def run_fact_product():
    logger.info(BANNER); logger.info("FACT PRODUCT"); logger.info(BANNER)
    return FactSalesWeatherProduct().extract()


# ════════════════════════════════════════════════════════════════
# LIVE PIPELINE
# ════════════════════════════════════════════════════════════════

def run_live_medallion_stack():
    """
    Rebuild the complete LIVE medallion stack after new files are staged:
      bronze_live → silver_live → gold_live
    """
    logger.info(BANNER)
    logger.info("LIVE STACK: weather_bronze_live")
    logger.info(BANNER)
    WeatherBronzeLiveTransformer().extract()

    logger.info(BANNER)
    logger.info("LIVE STACK: weather_silver_live")
    logger.info(BANNER)
    WeatherSilverLiveTransformer().extract()

    logger.info(BANNER)
    logger.info("LIVE STACK: sales_bronze_live")
    logger.info(BANNER)
    SalesBronzeLiveTransformer().extract()

    logger.info(BANNER)
    logger.info("LIVE STACK: sales_silver_live")
    logger.info(BANNER)
    SalesSilverLiveTransformer().extract()

    logger.info(BANNER)
    logger.info("LIVE STACK: gold_live  (sales_weather_live)")
    logger.info(BANNER)
    SalesWeatherGoldLive().extract()

    logger.info("✔ Live medallion stack rebuilt.")


def run_live(stage: str, date_str: str = None, dry_run: bool = False):
    """
    Dispatcher for live pipeline stages.

    backfill : One-time – generate last 5 days + CDC + rebuild live stack
    daily    : Daily cron – generate tomorrow + CDC + rebuild live stack
    generate : Create files only (no CDC, no pipeline)
    cdc      : CDC only → rebuild live stack if new data found
    status   : Show watermark state
    """
    from datetime import date, timedelta
    from scripts.generate_live_weather import run_backfill as w_backfill, run_daily as w_daily
    from scripts.generate_live_retail  import run_backfill as r_backfill, run_daily as r_daily
    from scripts.cdc_manager import (
        run_cdc_weather, run_cdc_retail, run_cdc_all, show_watermark_status
    )

    target = date.fromisoformat(date_str) if date_str else None

    if stage == "backfill":
        logger.info(BANNER)
        logger.info("LIVE PIPELINE — BACKFILL (last 5 days)")
        logger.info(BANNER)
        w_backfill()
        r_backfill()
        run_cdc_all(dry_run=False)      # stages files + rebuilds live stack

    elif stage == "daily":
        logger.info(BANNER)
        logger.info(f"LIVE PIPELINE — DAILY  (target: {target or 'tomorrow'})")
        logger.info(BANNER)
        w_daily(target)
        r_daily(target)
        run_cdc_all(dry_run=dry_run)    # stages files + rebuilds live stack

    elif stage == "generate":
        logger.info("Generating live files (weather + retail)...")
        dt = target or (date.today() + timedelta(days=1))
        w_daily(dt)
        r_daily(dt)
        logger.info("Generate complete. Run:  python main.py --live cdc")

    elif stage == "cdc":
        if dry_run:
            logger.info("*** DRY-RUN MODE ***")
        run_cdc_all(dry_run=dry_run)

    elif stage == "status":
        show_watermark_status()


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def main():
    Path("logs").mkdir(exist_ok=True)

    parser = argparse.ArgumentParser(
        description="Global Retail & Weather Analytics Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Historical pipeline:
  python main.py --entity weather
  python main.py --entity retail_sales
  python main.py --entity weather_bronze
  python main.py --entity sales_bronze
  python main.py --entity weather_silver
  python main.py --entity sales_silver
  python main.py --entity gold
  python main.py --entity dim_date
  python main.py --entity dim_location
  python main.py --entity fact
  python main.py --all

Live pipeline (CDC):
  python main.py --live backfill              # One-time setup
  python main.py --live daily                 # Daily cron
  python main.py --live daily --date 2026-04-25
  python main.py --live cdc                   # CDC + rebuild live stack
  python main.py --live cdc --dry-run
  python main.py --live generate              # Files only
  python main.py --live status                # Watermark status

Analytics CLI (Historical + Live):
  python analysis/cli_app.py
        """,
    )

    parser.add_argument(
        "--entity",
        choices=[
            "weather", "retail_sales",
            "weather_bronze", "sales_bronze",
            "weather_silver", "sales_silver",
            "gold", "dim_date", "dim_location", "fact","dim_product","fact_product"
        ],
        help="Historical pipeline stage to run",
    )
    parser.add_argument("--file",    type=str, default=None,
                        help="Specific file for --entity retail_sales")
    parser.add_argument("--all",     action="store_true",
                        help="Run full historical pipeline end-to-end")
    parser.add_argument(
        "--live",
        choices=["backfill", "daily", "generate", "cdc", "status"],
        metavar="STAGE",
        help="Live pipeline stage: backfill | daily | generate | cdc | status",
    )
    parser.add_argument("--date",    type=str, default=None,
                        help="Target date for --live daily/generate (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="CDC dry run – no watermark updates")

    args = parser.parse_args()

    # ── Live pipeline ────────────────────────────────────────────
    if args.live:
        run_live(stage=args.live, date_str=args.date, dry_run=args.dry_run)
        return

    # ── Historical pipeline ──────────────────────────────────────
    if args.all or args.entity == "weather":
        run_weather()
    if args.all or args.entity == "retail_sales":
        run_retail(file_name=args.file)
    if args.all or args.entity == "weather_bronze":
        run_weather_bronze()
    if args.all or args.entity == "sales_bronze":
        run_sales_bronze()
    if args.all or args.entity == "weather_silver":
        run_weather_silver()
    if args.all or args.entity == "sales_silver":
        run_sales_silver()
    if args.all or args.entity == "gold":
        run_gold()
    if args.all or args.entity == "dim_date":
        run_dim_date()
    if args.all or args.entity == "dim_location":
        run_dim_location()
    if args.all or args.entity == "fact":
        run_fact()
    if args.all or args.entity == "dim_product":
        run_dim_product()
    if args.all or args.entity == "fact_product":
        run_fact_product()
    

    if not args.all and not args.entity and not args.live:
        parser.print_help()


if __name__ == "__main__":
    main()
