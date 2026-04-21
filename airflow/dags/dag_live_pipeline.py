from __future__ import annotations
import logging, os, sys, time
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 👇 Import dummy functions once
from sample import (
    generate_weather, generate_retail,
    cdc_detect,
    weather_bronze_live, sales_bronze_live,
    weather_silver_live, sales_silver_live,
    gold_live
)

# 👇 Project root = DAG folder
PROJECT_ROOT = Path(__file__).parent

sys.path.insert(0, str(PROJECT_ROOT))
logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "capstone",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def _setup():
    os.chdir(PROJECT_ROOT)
    sys.path.insert(0, str(PROJECT_ROOT))


# -----------------------------------------
# DELAY FUNCTION (makes DAG run ~5 mins)
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
# TASKS
# -----------------------------------------
def task_generate_weather(**ctx):
    run_with_delay(generate_weather, "generate_weather")

def task_generate_retail(**ctx):
    run_with_delay(generate_retail, "generate_retail")

def task_cdc_detect(**ctx):
    run_with_delay(cdc_detect, "cdc_detect")

def task_weather_bronze_live(**ctx):
    run_with_delay(weather_bronze_live, "weather_bronze_live")

def task_sales_bronze_live(**ctx):
    run_with_delay(sales_bronze_live, "sales_bronze_live")

def task_weather_silver_live(**ctx):
    run_with_delay(weather_silver_live, "weather_silver_live")

def task_sales_silver_live(**ctx):
    run_with_delay(sales_silver_live, "sales_silver_live")

def task_gold_live(**ctx):
    run_with_delay(gold_live, "gold_live")


# -----------------------------------------
# DAG
# -----------------------------------------
with DAG(
    dag_id="dag_live_pipeline",
    default_args=DEFAULT_ARGS,
    description="Live pipeline (all tasks always run)",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 4, 19),
    catchup=False,
    tags=["capstone", "live", "cdc", "daily"],
) as dag:

    gen_w = PythonOperator(
        task_id="generate_weather",
        python_callable=task_generate_weather
    )

    gen_r = PythonOperator(
        task_id="generate_retail",
        python_callable=task_generate_retail
    )

    cdc = PythonOperator(
        task_id="cdc_detect",
        python_callable=task_cdc_detect
    )

    t_wb = PythonOperator(
        task_id="weather_bronze_live",
        python_callable=task_weather_bronze_live
    )

    t_sb = PythonOperator(
        task_id="sales_bronze_live",
        python_callable=task_sales_bronze_live
    )

    t_ws = PythonOperator(
        task_id="weather_silver_live",
        python_callable=task_weather_silver_live
    )

    t_ss = PythonOperator(
        task_id="sales_silver_live",
        python_callable=task_sales_silver_live
    )

    t_gl = PythonOperator(
        task_id="gold_live",
        python_callable=task_gold_live
    )

    done = EmptyOperator(task_id="done")


    # -----------------------------------------
    # FLOW (NO SKIPPING)
    # -----------------------------------------
    [gen_w, gen_r] >> cdc

    cdc >> [t_wb, t_sb]

    t_wb >> t_ws
    t_sb >> t_ss

    [t_ws, t_ss] >> t_gl >> done