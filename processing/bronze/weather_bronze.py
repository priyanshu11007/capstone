"""
processing/bronze/weather_bronze.py

Reads historical weather JSON from raw layer and writes
cleaned, exploded, partitioned Parquet to bronze.

Schema fixes:
  - date          → StringType (kept as string to avoid locale date parsing)
  - temp_max/min  → DoubleType
  - precipitation → DoubleType
  - city          → StringType
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, input_file_name, regexp_extract
from pyspark.sql.types import DoubleType, StringType
from ingestion.base import BaseExtractor


class WeatherBronzeTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="bronze")
        self.spark = (
            SparkSession.builder
            .appName("Weather Bronze Transformation")
            .getOrCreate()
        )

    def extract(self, **kwargs):

        raw_path = "data_lake/raw/weather/open_meteo"

        self.logger.info("Reading raw weather JSON files...")

        df = self.spark.read.option("multiline", "true").json(raw_path)
        df = df.withColumn("file_path", input_file_name())
        df = df.withColumn(
            "city",
            regexp_extract(col("file_path"), r"city=([^/]+)", 1)
        )

        df = df.withColumn(
            "daily_data",
            arrays_zip(
                col("daily.time"),
                col("daily.temperature_2m_max"),
                col("daily.temperature_2m_min"),
                col("daily.precipitation_sum"),
            )
        )
        df = df.withColumn("daily_data", explode(col("daily_data")))

        df = df.select(
            col("city").cast(StringType()),
            col("daily_data.time").cast(StringType()).alias("date"),
            col("daily_data.temperature_2m_max").cast(DoubleType()).alias("temp_max"),
            col("daily_data.temperature_2m_min").cast(DoubleType()).alias("temp_min"),
            col("daily_data.precipitation_sum").cast(DoubleType()).alias("precipitation"),
        )

        # Drop rows where date or temp is null (malformed JSON edge case)
        df = df.filter(col("date").isNotNull() & col("temp_max").isNotNull())

        self.logger.info(f"Weather Bronze rows: {df.count()}")

        output_path = "data_lake/bronze/weather/open_meteo"
        (
            df.write
            .mode("overwrite")
            .partitionBy("city")
            .parquet(output_path)
        )

        self.logger.info(f"✔ Written Weather Bronze → {output_path}")
        return output_path

    def validate(self, data):
        return True
