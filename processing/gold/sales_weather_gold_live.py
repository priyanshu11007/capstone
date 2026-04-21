"""
processing/gold/sales_weather_gold_live.py

Builds the LIVE gold table:  data_lake/gold/sales_weather_live

Reads from:
  - data_lake/silver/sales/online_retail_live    (live bronze → live silver)
  - data_lake/silver/weather/open_meteo_live     (live bronze → live silver)

Identical feature engineering and schema enforcement to sales_weather_gold.py.
"""
from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, when, avg
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window
from ingestion.base import BaseExtractor


class SalesWeatherGoldLive(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")
        self.loader = SparkLoader()
        self.spark  = self.loader.spark

    def extract(self, **kwargs):

        sales_path   = "data_lake/silver/sales/online_retail_live"
        weather_path = "data_lake/silver/weather/open_meteo_live"

        self.logger.info("Reading LIVE silver sales...")
        sales_df = self.loader.read(sales_path, format="delta")

        self.logger.info("Reading LIVE silver weather...")
        weather_df = self.loader.read(weather_path, format="delta")

        # ── Align join-key types ─────────────────────────────────────────
        sales_df   = sales_df.withColumn("city", col("city").cast(StringType()))
        weather_df = weather_df.withColumn("city", col("city").cast(StringType()))

        sales_df   = sales_df.withColumn("date", col("date").cast("date"))
        weather_df = weather_df.withColumn("date", col("date").cast("date"))

        # ── Align measure types ──────────────────────────────────────────
        sales_df   = sales_df.withColumn("total_sales",   col("total_sales").cast(DoubleType()))
        weather_df = weather_df.withColumn("avg_temp",      col("avg_temp").cast(DoubleType()))
        weather_df = weather_df.withColumn("precipitation", col("precipitation").cast(DoubleType()))

        # ── Join ─────────────────────────────────────────────────────────
        df = sales_df.join(weather_df, on=["date", "city"], how="inner")

        # ── Feature engineering ──────────────────────────────────────────
        df = df.withColumn(
            "is_rainy",
            when(col("precipitation") > 0, 1).otherwise(0).cast(IntegerType())
        )

        df = df.withColumn(
            "temp_category",
            when(col("avg_temp") < 10, "Cold")
            .when((col("avg_temp") >= 10) & (col("avg_temp") < 20), "Moderate")
            .otherwise("Hot")
            .cast(StringType())
        )

        df = df.withColumn(
            "sales_category",
            when(col("total_sales") < 500, "Low")
            .when((col("total_sales") >= 500) & (col("total_sales") < 2000), "Medium")
            .otherwise("High")
            .cast(StringType())
        )

        window_spec = Window.partitionBy("city").orderBy("date").rowsBetween(-7, 0)
        df = df.withColumn(
            "rolling_7d_sales",
            avg("total_sales").over(window_spec).cast(DoubleType())
        )

        output_path = "data_lake/gold/sales_weather_live"
        self.loader.write(
            df, output_path,
            format="delta",
            partition_cols=["city"],
            mode="overwrite",
        )

        self.logger.info(f"✔ Live Gold written (Delta) → {output_path}")
        return output_path

    def validate(self, data):
        return True
