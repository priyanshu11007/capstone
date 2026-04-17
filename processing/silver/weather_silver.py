from loader.spark_loader import SparkLoader
from pyspark.sql.functions import col
from ingestion.base import BaseExtractor


class WeatherSilverTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path=None, layer="silver")

        # ✅ Use Generic Loader
        self.loader = SparkLoader()
        self.spark = self.loader.spark

    def extract(self, **kwargs):

        path = "data_lake/bronze/weather/open_meteo"

        # -------------------------------------------------------
        # Read using generic loader
        # -------------------------------------------------------
        df = self.loader.read(path, format="parquet")

        # -------------------------------------------------------
        # Feature engineering
        # -------------------------------------------------------
        df = df.withColumn(
            "avg_temp",
            (col("temp_max") + col("temp_min")) / 2
        )

        # -------------------------------------------------------
        # Select only needed columns
        # -------------------------------------------------------
        df = df.select("date", "city", "avg_temp", "precipitation")

        output_path = "data_lake/silver/weather/open_meteo"

        # -------------------------------------------------------
        # Write using generic loader (DELTA 🔥)
        # -------------------------------------------------------
        self.loader.write(
            df,
            output_path,
            format="delta",              # ✅ Delta table
            partition_cols=["city"],     # ✅ Partitioning
            mode="overwrite"
        )

        self.logger.info(f"✔ Weather Silver written (Delta) → {output_path}")

        return output_path

    def validate(self, data):
        return True