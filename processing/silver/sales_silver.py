from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, sum as _sum
from ingestion.base import BaseExtractor


class SalesSilverTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="silver")

        # Use Generic Loader (handles Spark + Delta)
        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        path = "data_lake/bronze/sales/online_retail"

        # -------------------------------------------------------
        # Read using generic loader
        # -------------------------------------------------------
        df = self.loader.read(path, format="parquet")

        # -------------------------------------------------------
        # Clean
        # -------------------------------------------------------
        df = df.filter(col("Quantity") > 0)
        df = df.filter(col("UnitPrice") > 0)

        # -------------------------------------------------------
        # Feature engineering
        # -------------------------------------------------------
        df = df.withColumn("total_price", col("Quantity") * col("UnitPrice"))

        df_product = df.select(
            "date", "city", "StockCode", "Description",
            "Quantity", "UnitPrice", "total_price"
        )
        # -------------------------------------------------------
        # Aggregate (daily sales)
        # -------------------------------------------------------
        df = df.groupBy("date", "city").agg(
            _sum("total_price").alias("total_sales")
        )

        output_path = "data_lake/silver/sales/online_retail"

        # -------------------------------------------------------
        # Write using generic loader (DELTA)
        # -------------------------------------------------------
        self.loader.write(
            df,
            output_path,
            format="delta",             
            partition_cols=["city"],     
            mode="overwrite"
        )

        self.loader.write(df_product, "data_lake/silver/sales/product_grain", format="delta", partition_cols=["city"], mode="overwrite")
        self.logger.info(f"✔ Sales Silver written (Delta) → {output_path}")

        return output_path

    def validate(self, data):
        return True