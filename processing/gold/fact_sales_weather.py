from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col
from ingestion.base import BaseExtractor


class FactSalesWeather(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")

        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        gold_path = "data_lake/gold/sales_weather"
        dim_date_path = "data_lake/gold/dim_date"
        dim_location_path = "data_lake/gold/dim_location"

        # Read data
        gold_df = self.loader.read(gold_path, format="delta")
        dim_date_df = self.loader.read(dim_date_path, format="delta")
        dim_loc_df = self.loader.read(dim_location_path, format="delta")

        # Join with dimensions
        df = gold_df.join(dim_date_df, on="date") \
                    .join(dim_loc_df, on="city")

        # Select fact columns
        df = df.select(
            col("date_id"),
            col("location_id"),
            col("total_sales"),
            col("avg_temp"),
            col("precipitation"),
            col("is_rainy"),
            col("rolling_7d_sales")
        )

        output_path = "data_lake/gold/fact_sales_weather"

        # Write as Delta
        self.loader.write(
            df,
            output_path,
            format="delta",
            partition_cols=["location_id"],  # optional
            mode="overwrite"
        )

        self.logger.info(f"✔ fact_sales_weather written → {output_path}")

        return output_path

    def validate(self, data):
        return True