"""
processing/silver/sales_silver.py

Reads bronze sales Parquet and writes aggregated daily totals to
a Delta table in the silver layer.

Schema fixes:
  - Quantity  filter: cast to int before comparison (handles nullable int)
  - UnitPrice filter: cast to double before comparison
  - total_sales output: DoubleType (guaranteed)
  - date output:       DateType
  - city output:       StringType
"""
from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, sum as _sum
from pyspark.sql.types import DoubleType, IntegerType, StringType
from ingestion.base import BaseExtractor


class SalesSilverTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="silver")
        self.loader = SparkLoader()
        self.spark  = self.loader.spark

    def extract(self, **kwargs):

        path = "data_lake/bronze/sales/online_retail"
        df   = self.loader.read(path, format="parquet")

        # ── Defensive casts before filtering ────────────────────────────
        df = df.withColumn("Quantity",  col("Quantity").cast(IntegerType()))
        df = df.withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))
        df = df.withColumn("city",      col("city").cast(StringType()))

        # ── Clean ────────────────────────────────────────────────────────
        df = df.filter(col("Quantity")  > 0)
        df = df.filter(col("UnitPrice") > 0)

        # ── Feature engineering ──────────────────────────────────────────
        df = df.withColumn(
            "total_price",
            (col("Quantity").cast(DoubleType())) * col("UnitPrice")
        )

        df_product = df.select(
            "date","city","StockCode","Description",
            "Quantity","UnitPrice","total_price"
        )

        # ── Aggregate ────────────────────────────────────────────────────
        df = df.groupBy("date", "city").agg(
            _sum("total_price").cast(DoubleType()).alias("total_sales")
        )

        # ── Final type guarantee ─────────────────────────────────────────
        df = df.withColumn("total_sales", col("total_sales").cast(DoubleType()))
        df = df.withColumn("city",        col("city").cast(StringType()))

        output_path = "data_lake/silver/sales/online_retail"
        self.loader.write(
            df, output_path,
            format="delta",
            partition_cols=["city"],
            mode="overwrite",
        )

        self.loader.write(
            df_product, "data_lake/silver/sales/product_grain",
            format="delta",
            partition_cols=["city"],
            mode="overwrite",
        )

        self.logger.info(f"✔ Sales Silver written (Delta) → {output_path}")
        return output_path

    def validate(self, data):
        return True
