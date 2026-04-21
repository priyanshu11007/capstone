"""
processing/silver/weather_silver_live.py

Reads LIVE bronze weather and writes cleaned daily records to:
    data_lake/silver/weather/open_meteo_live   (Delta)

Identical schema enforcement to weather_silver.py.
"""
from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType
from ingestion.base import BaseExtractor


class WeatherSilverLiveTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="silver")
        self.loader = SparkLoader()
        self.spark  = self.loader.spark

    def extract(self, **kwargs):

        df = self.loader.read(
            "data_lake/bronze/weather/open_meteo_live", format="parquet"
        )

        # ── Defensive casts ──────────────────────────────────────────────
        df = df.withColumn("temp_max",      col("temp_max").cast(DoubleType()))
        df = df.withColumn("temp_min",      col("temp_min").cast(DoubleType()))
        df = df.withColumn("precipitation", col("precipitation").cast(DoubleType()))
        df = df.withColumn("city",          col("city").cast(StringType()))

        df = df.filter(col("temp_max").isNotNull() & col("temp_min").isNotNull())

        df = df.withColumn(
            "avg_temp",
            ((col("temp_max") + col("temp_min")) / 2).cast(DoubleType())
        )

        df = df.select(
            col("date"),
            col("city").cast(StringType()),
            col("avg_temp").cast(DoubleType()),
            col("precipitation").cast(DoubleType()),
        )

        output_path = "data_lake/silver/weather/open_meteo_live"
        self.loader.write(
            df, output_path,
            format="delta",
            partition_cols=["city"],
            mode="overwrite",
        )

        self.logger.info(f"✔ Weather Silver Live written → {output_path}")
        return output_path

    def validate(self, data):
        return True
