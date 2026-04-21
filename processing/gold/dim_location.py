from loader.spark_loader import SparkLoader
from pyspark.sql.functions import monotonically_increasing_id
from ingestion.base import BaseExtractor


class DimLocation(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="gold")

        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        path = "data_lake/gold/sales_weather"

        df = self.loader.read(path, format="delta")

        # Get distinct cities
        df = df.select("city").distinct()

        # Surrogate key
        df = df.withColumn("location_id", monotonically_increasing_id())

        output_path = "data_lake/gold/dim_location"

        # Write as Delta
        self.loader.write(
            df,
            output_path,
            format="delta",
            mode="overwrite"
        )

        self.logger.info(f"✔ dim_location written → {output_path}")

        return output_path

    def validate(self, data):
        return True