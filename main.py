"""
main.py
Entry point — runs ingestion + bronze pipeline.

Usage:
    python main.py --entity weather
    python main.py --entity retail_sales --file "Online Retail.csv"
    python main.py --entity sales_bronze
    python main.py --entity weather_bronze
    python main.py --all
"""
import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is in path when run directly
sys.path.insert(0, str(Path(__file__).parent))

from ingestion.api_extractor import APIExtractor
from ingestion.file_extractor import FileExtractor
from processing.bronze.sales_bronze import SalesBronzeTransformer  # ✅ NEW
from processing.bronze.weather_bronze import WeatherBronzeTransformer
from processing.silver.sales_silver import SalesSilverTransformer
from processing.silver.weather_silver import WeatherSilverTransformer
from processing.gold.sales_weather_gold import SalesWeatherGold
from processing.gold.dim_date import DimDate
from processing.gold.dim_location import DimLocation
from processing.gold.fact_sales_weather import FactSalesWeather
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


# ---------------------------------------------------------
# WEATHER INGESTION
# ---------------------------------------------------------
def run_weather():
    logger.info("=" * 60)
    logger.info("Starting WEATHER ingestion (Open-Meteo historical)")
    logger.info("=" * 60)

    extractor = APIExtractor(config_path="configs/api_config.yaml")
    written = extractor.extract(entity="weather")

    logger.info(f"Weather ingestion complete. Files written: {len(written)}")
    for p in written:
        logger.info(f"  {p}")

    return written


# ---------------------------------------------------------
# RETAIL RAW INGESTION
# ---------------------------------------------------------
def run_retail(file_name: str = None):
    logger.info("=" * 60)
    logger.info("Starting RETAIL SALES ingestion (manual upload)")
    logger.info("=" * 60)

    extractor = FileExtractor(config_path="configs/api_config.yaml")

    written = extractor.extract(
        entity="retail_sales",
        source_name="uci_online_retail",
        file_name=file_name,
        output_format="parquet",
    )

    logger.info(f"Retail ingestion complete. Files written: {len(written)}")
    for p in written:
        logger.info(f"  {p}")

    return written


# ---------------------------------------------------------
# SALES BRONZE (PySpark)
# ---------------------------------------------------------
def run_sales_bronze():
    logger.info("=" * 60)
    logger.info("Starting SALES BRONZE transformation (PySpark)")
    logger.info("=" * 60)

    transformer = SalesBronzeTransformer()
    output_path = transformer.extract()

    logger.info(f"Sales Bronze complete → {output_path}")

    return output_path

def run_weather_bronze():
    logger.info("=" * 60)
    logger.info("Starting WEATHER BRONZE transformation (PySpark)")
    logger.info("=" * 60)

    transformer = WeatherBronzeTransformer()
    output_path = transformer.extract()

    logger.info(f"Weather Bronze complete → {output_path}")

    return output_path

def run_sales_silver():
    SalesSilverTransformer().extract()

def run_weather_silver():
    WeatherSilverTransformer().extract()

def run_gold():
    SalesWeatherGold().extract()

def run_dim_date():
    DimDate().extract()

def run_dim_location():
    DimLocation().extract()

def run_fact():
    FactSalesWeather().extract()

def run_dim_product():
    DimProduct().extract()

def run_fact_product():
    FactSalesWeatherProduct().extract()

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    Path("logs").mkdir(exist_ok=True)

    parser = argparse.ArgumentParser(
        description="Retail-Weather Platform – Ingestion + Bronze"
    )

    parser.add_argument(
        "--entity",
        choices=["weather", "retail_sales", "sales_bronze", "weather_bronze","sales_silver", "weather_silver","gold","dim_date", "dim_location", "fact","dim_product", "fact_product"],  
        help="Run pipeline for a specific entity",
    )

    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Specific file name inside manual-uploads (retail only)",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all pipelines",
    )

    args = parser.parse_args()

    if args.all:
        logger.info("Running FULL PIPELINE")

        run_weather()
        logger.info("DONE: weather")

        run_retail(file_name=args.file)
        logger.info("DONE: retail")

        run_sales_bronze()
        logger.info("DONE: sales_bronze")

        run_weather_bronze()
        logger.info("DONE: weather_bronze")

        run_sales_silver()
        logger.info("DONE: sales_silver")

        run_weather_silver()
        logger.info("DONE: weather_silver")

        run_gold()
        logger.info("DONE: gold")

        run_dim_date()
        run_dim_location()
        run_dim_product()
        logger.info("DONE: dimensions")

        run_fact()
        run_fact_product()
        logger.info("DONE: facts")
    elif args.entity == "weather":
        run_weather()

    elif args.entity == "retail_sales":
        run_retail(file_name=args.file)

    elif args.entity == "sales_bronze":
        run_sales_bronze()

    elif args.entity == "weather_bronze":
        run_weather_bronze()

    elif args.entity == "sales_silver":
        run_sales_silver()

    elif args.entity == "weather_silver":
        run_weather_silver()

    elif args.entity == "gold":
        run_gold()

    elif args.entity == "dim_date":
        run_dim_date()

    elif args.entity == "dim_location":
        run_dim_location()

    elif args.entity == "dim_product":
        run_dim_product()

    elif args.entity == "fact":
        run_fact()

    elif args.entity == "fact_product":
        run_fact_product()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()