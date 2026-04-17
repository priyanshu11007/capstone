from loader.spark_loader import SparkLoader
from pyspark.sql.functions import monotonically_increasing_id, col, trim, upper
from ingestion.base import BaseExtractor

class DimProduct(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")
        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):
        df = self.loader.read("data_lake/silver/sales/product_grain", format="delta")

        # Distinct products with cleaned description
        df = df.select(
            col("StockCode").alias("stock_code"),
            trim(col("Description")).alias("product_description")
        ).distinct()

        # Drop nulls/blanks
        df = df.filter(
            col("stock_code").isNotNull() &
            col("product_description").isNotNull() &
            (col("product_description") != "")
        )

        # Surrogate key
        df = df.withColumn("product_id", monotonically_increasing_id())

        output_path = "data_lake/gold/dim_product"
        self.loader.write(df, output_path, format="delta", mode="overwrite")
        self.logger.info(f"✔ dim_product written → {output_path}")
        return output_path

    def validate(self, data):
        return True