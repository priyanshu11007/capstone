from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class SparkLoader:

    def __init__(self):

        builder = SparkSession.builder \
            .appName("Generic Loader") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Enable schema evolution
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    def read(self, path: str, format: str = "delta"):
        if format == "delta":
            return self.spark.read.format("delta").load(path)
        elif format == "parquet":
            return self.spark.read.parquet(path)
        elif format == "json":
            return self.spark.read.option("multiline", "true").json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write(self, df, path: str, format="delta", partition_cols=None, mode="overwrite"):

        writer = df.write.format(format).mode(mode)

        if format == "delta":
            writer = writer.option("mergeSchema", "true")

        if partition_cols:
            if isinstance(partition_cols, list):
                writer = writer.partitionBy(*partition_cols)
            else:
                writer = writer.partitionBy(partition_cols)

        writer.save(path)