from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, when
from ingestion.base import BaseExtractor

class FactSalesWeatherProduct(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")
        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):
        product_grain = self.loader.read("data_lake/silver/sales/product_grain", format="delta")
        weather_df    = self.loader.read("data_lake/silver/weather/open_meteo", format="delta")
        dim_date      = self.loader.read("data_lake/gold/dim_date", format="delta")
        dim_loc       = self.loader.read("data_lake/gold/dim_location", format="delta")
        dim_product   = self.loader.read("data_lake/gold/dim_product", format="delta")

        # Join weather onto product-grain sales
        df = product_grain.join(weather_df, on=["date", "city"], how="inner")

        # Enrich with dimension surrogate keys
        df = df.join(dim_date.select("date", "date_id"), on="date") \
               .join(dim_loc.select("city", "location_id"), on="city") \
               .join(dim_product.select("stock_code", "product_id"),
                     df["StockCode"] == dim_product["stock_code"], how="left") \
               .drop("stock_code")

        # Derived flags (same as before)
        df = df.withColumn("is_rainy", when(col("precipitation") > 0, 1).otherwise(0)) \
               .withColumn("temp_category",
                    when(col("avg_temp") < 10, "Cold")
                    .when((col("avg_temp") >= 10) & (col("avg_temp") < 20), "Moderate")
                    .otherwise("Hot"))

        df = df.select(
            "date_id", "location_id", "product_id",
            col("StockCode").alias("stock_code"),
            col("Description").alias("product_description"),
            "Quantity", "UnitPrice", "total_price",
            "avg_temp", "precipitation", "is_rainy", "temp_category"
        )

        output_path = "data_lake/gold/fact_sales_weather_product"
        self.loader.write(df, output_path, format="delta", partition_cols=["location_id"], mode="overwrite")
        self.logger.info(f"✔ fact_sales_weather_product written → {output_path}")
        return output_path

    def validate(self, data):
        return True