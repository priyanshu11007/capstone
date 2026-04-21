"""
processing/gold/fact_sales_weather.py

Builds the fact table (star schema) by joining gold sales_weather
with dim_date and dim_location.

Schema fixes:
  - location_id cast to LongType (monotonically_increasing_id returns LongType)
  - All measure columns cast explicitly
"""
from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType
from ingestion.base import BaseExtractor


class FactSalesWeather(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")
        self.loader = SparkLoader()
        self.spark  = self.loader.spark

    def extract(self, **kwargs):

        gold_path     = "data_lake/gold/sales_weather"
        dim_date_path = "data_lake/gold/dim_date"
        dim_loc_path  = "data_lake/gold/dim_location"

        gold_df    = self.loader.read(gold_path,     format="delta")
        dim_date   = self.loader.read(dim_date_path, format="delta")
        dim_loc    = self.loader.read(dim_loc_path,  format="delta")

        # ── Align join keys ──────────────────────────────────────────────
        gold_df  = gold_df.withColumn("date", col("date").cast("date"))
        gold_df  = gold_df.withColumn("city", col("city").cast(StringType()))
        dim_date = dim_date.withColumn("date", col("date").cast("date"))
        dim_loc  = dim_loc.withColumn("city", col("city").cast(StringType()))
        dim_loc  = dim_loc.withColumn("location_id", col("location_id").cast(LongType()))

        # ── Join ─────────────────────────────────────────────────────────
        df = (
            gold_df
            .join(dim_date, on="date", how="left")
            .join(dim_loc,  on="city", how="left")
        )

        # ── Select fact columns with explicit types ───────────────────────
        df = df.select(
            col("date_id").cast(StringType()),
            col("location_id").cast(LongType()),
            col("total_sales").cast(DoubleType()),
            col("avg_temp").cast(DoubleType()),
            col("precipitation").cast(DoubleType()),
            col("is_rainy").cast(IntegerType()),
            col("rolling_7d_sales").cast(DoubleType()),
        )

        output_path = "data_lake/gold/fact_sales_weather"
        self.loader.write(
            df, output_path,
            format="delta",
            partition_cols=["location_id"],
            mode="overwrite",
        )

        self.logger.info(f"✔ fact_sales_weather written → {output_path}")
        return output_path

    def validate(self, data):
        return True
