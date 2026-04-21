from __future__ import annotations
import logging, os, sys, time
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

# 👇 Import all dummy functions once
from sample import (
    ingest_weather, ingest_retail,
    weather_bronze, sales_bronze,
    weather_silver, sales_silver,
    gold, dim_date, dim_location, fact
)

# 👇 Set project root to DAG folder
PROJECT_ROOT = Path(__file__).parent

sys.path.insert(0, str(PROJECT_ROOT))
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "capstone",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _setup():
    os.chdir(PROJECT_ROOT)
    sys.path.insert(0, str(PROJECT_ROOT))


# -----------------------------------------
# DELAY WRAPPER (controls execution time)
# -----------------------------------------
def run_with_delay(task_fn, task_name, delay=25):
    _setup()
    logger.info(f"🚀 Starting {task_name}")

    for i in range(delay):
        logger.info(f"{task_name} running... {i+1}/{delay} sec")
        time.sleep(1)

    task_fn()

    logger.info(f"✅ Finished {task_name}")


# -----------------------------------------
# TASK DEFINITIONS (same structure)
# -----------------------------------------
def task_ingest_weather(**ctx):
    run_with_delay(ingest_weather, "ingest_weather")

def task_ingest_retail(**ctx):
    run_with_delay(ingest_retail, "ingest_retail")

def task_weather_bronze(**ctx):
    run_with_delay(weather_bronze, "weather_bronze")

def task_sales_bronze(**ctx):
    run_with_delay(sales_bronze, "sales_bronze")

def task_weather_silver(**ctx):
    run_with_delay(weather_silver, "weather_silver")

def task_sales_silver(**ctx):
    run_with_delay(sales_silver, "sales_silver")

def task_gold(**ctx):
    run_with_delay(gold, "gold")

def task_dim_date(**ctx):
    run_with_delay(dim_date, "dim_date")

def task_dim_location(**ctx):
    run_with_delay(dim_location, "dim_location")

def task_fact(**ctx):
    run_with_delay(fact, "fact")


# -----------------------------------------
# DAG (UNCHANGED STRUCTURE)
# -----------------------------------------
with DAG(
    dag_id="dag_historical_pipeline",
    default_args=DEFAULT_ARGS,
    description="Historical medallion pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "historical", "etl"],
) as dag:

    t_iw  = PythonOperator(task_id="ingest_weather",  python_callable=task_ingest_weather)
    t_ir  = PythonOperator(task_id="ingest_retail",   python_callable=task_ingest_retail)
    t_wb  = PythonOperator(task_id="weather_bronze",  python_callable=task_weather_bronze)
    t_sb  = PythonOperator(task_id="sales_bronze",    python_callable=task_sales_bronze)
    t_ws  = PythonOperator(task_id="weather_silver",  python_callable=task_weather_silver)
    t_ss  = PythonOperator(task_id="sales_silver",    python_callable=task_sales_silver)
    t_g   = PythonOperator(task_id="gold",            python_callable=task_gold)
    t_dd  = PythonOperator(task_id="dim_date",        python_callable=task_dim_date)
    t_dl  = PythonOperator(task_id="dim_location",    python_callable=task_dim_location)
    t_f   = PythonOperator(task_id="fact",            python_callable=task_fact)

    # SAME FLOW
    t_iw >> t_wb >> t_ws
    t_ir >> t_sb >> t_ss
    [t_ws, t_ss] >> t_g >> [t_dd, t_dl] >> t_f