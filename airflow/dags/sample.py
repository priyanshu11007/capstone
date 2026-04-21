# airflow/dags/sample.py
import time
import logging

logger = logging.getLogger(__name__)

def _run(task):
    logger.info(f"🚀 Running {task}")
    time.sleep(2)
    logger.info(f"✅ Finished {task}")


# -------- HISTORICAL --------
def ingest_weather(): _run("ingest_weather")
def ingest_retail(): _run("ingest_retail")
def weather_bronze(): _run("weather_bronze")
def sales_bronze(): _run("sales_bronze")
def weather_silver(): _run("weather_silver")
def sales_silver(): _run("sales_silver")
def gold(): _run("gold")
def dim_date(): _run("dim_date")
def dim_location(): _run("dim_location")
def fact(): _run("fact")


# -------- LIVE --------
def generate_weather(): _run("generate_weather")
def generate_retail(): _run("generate_retail")

def cdc_detect():
    _run("cdc_detect")
    return "weather_bronze_live"

def weather_bronze_live(): _run("weather_bronze_live")
def sales_bronze_live(): _run("sales_bronze_live")
def weather_silver_live(): _run("weather_silver_live")
def sales_silver_live(): _run("sales_silver_live")
def gold_live(): _run("gold_live")