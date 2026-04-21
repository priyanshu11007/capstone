from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col, year, month, dayofmonth, dayofweek, weekofyear
from ingestion.base import BaseExtractor


class DimDate(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")

        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        path = "data_lake/gold/sales_weather"

        # Read GOLD (Delta)
        df = self.loader.read(path, format="delta")

        # Get distinct dates
        df = df.select("date").distinct()

        # Add date attributes
        df = df.withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date"))) \
               .withColumn("day", dayofmonth(col("date"))) \
               .withColumn("day_of_week", dayofweek(col("date"))) \
               .withColumn("week_of_year", weekofyear(col("date")))

        # Surrogate key
        df = df.withColumn("date_id", col("date").cast("string"))

        output_path = "data_lake/gold/dim_date"

        # Write as Delta
        self.loader.write(
            df,
            output_path,
            format="delta",
            mode="overwrite"
        )

        self.logger.info(f"✔ dim_date written → {output_path}")

        return output_path

    def validate(self, data):
        return True