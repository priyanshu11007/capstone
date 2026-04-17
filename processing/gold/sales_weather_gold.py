from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, when, avg
from pyspark.sql.window import Window
from ingestion.base import BaseExtractor


class SalesWeatherGold(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")

        # ✅ Use Generic Loader
        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        sales_path = "data_lake/silver/sales/online_retail"
        weather_path = "data_lake/silver/weather/open_meteo"

        # -------------------------------------------------------
        # Read using generic loader (DELTA 🔥)
        # -------------------------------------------------------
        sales_df = self.loader.read(sales_path, format="delta")
        weather_df = self.loader.read(weather_path, format="delta")

        # -------------------------------------------------------
        # Join
        # -------------------------------------------------------
        df = sales_df.join(
            weather_df,
            on=["date", "city"],
            how="inner"
        )

        # -------------------------------------------------------
        # Feature Engineering
        # -------------------------------------------------------

        # Rain flag
        df = df.withColumn(
            "is_rainy",
            when(col("precipitation") > 0, 1).otherwise(0)
        )

        # Temperature category
        df = df.withColumn(
            "temp_category",
            when(col("avg_temp") < 10, "Cold")
            .when((col("avg_temp") >= 10) & (col("avg_temp") < 20), "Moderate")
            .otherwise("Hot")
        )

        # Sales category
        df = df.withColumn(
            "sales_category",
            when(col("total_sales") < 500, "Low")
            .when((col("total_sales") >= 500) & (col("total_sales") < 2000), "Medium")
            .otherwise("High")
        )

        # Rolling 7-day average
        window_spec = Window.partitionBy("city").orderBy("date").rowsBetween(-7, 0)

        df = df.withColumn(
            "rolling_7d_sales",
            avg("total_sales").over(window_spec)
        )

        # -------------------------------------------------------
        # Write using generic loader (DELTA 🔥)
        # -------------------------------------------------------
        output_path = "data_lake/gold/sales_weather"

        self.loader.write(
            df,
            output_path,
            format="delta",              # ✅ Delta table
            partition_cols=["city"],     # ✅ Partition
            mode="overwrite"
        )

        self.logger.info(f"✔ Gold dataset written (Delta) → {output_path}")

        return output_path

    def validate(self, data):
        return True