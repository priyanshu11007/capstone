"""
processing/silver/sales_silver_live.py

Reads LIVE bronze sales and writes aggregated daily totals to:
    data_lake/silver/sales/online_retail_live   (Delta)

Identical schema enforcement to sales_silver.py.
"""
from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import DoubleType, IntegerType, StringType
from ingestion.base import BaseExtractor


class SalesSilverLiveTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="silver")
        self.loader = SparkLoader()
        self.spark  = self.loader.spark

    def extract(self, **kwargs):

        df = self.loader.read(
            "data_lake/bronze/sales/online_retail_live", format="parquet"
        )

        # ── Defensive casts ──────────────────────────────────────────────
        df = df.withColumn("Quantity",  col("Quantity").cast(IntegerType()))
        df = df.withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
        df = df.withColumn("city",      col("city").cast(StringType()))

        # ── Clean ────────────────────────────────────────────────────────
        df = df.filter(col("Quantity")  > 0)
        df = df.filter(col("UnitPrice") > 0)

        df = df.withColumn(
            "total_price",
            col("Quantity").cast(DoubleType()) * col("UnitPrice")
        )

        df = df.groupBy("date", "city").agg(
            _sum("total_price").cast(DoubleType()).alias("total_sales")
        )

        df = df.withColumn("total_sales", col("total_sales").cast(DoubleType()))
        df = df.withColumn("city",        col("city").cast(StringType()))

        output_path = "data_lake/silver/sales/online_retail_live"
        self.loader.write(
            df, output_path,
            format="delta",
            partition_cols=["city"],
            mode="overwrite",
        )

        self.logger.info(f"✔ Sales Silver Live written → {output_path}")
        return output_path

    def validate(self, data):
        return True
