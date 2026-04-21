"""
loader/spark_loader.py

Generic Spark loader with Delta Lake support.

Schema safety configs:
  - parquet.mergeSchema = false     : no silent schema drift
  - enableVectorizedReader = false  : allows INT64 → IntegerType tolerance
                                      (Spark 3.4+ rejects this with vectorized on)
"""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class SparkLoader:

    def __init__(self):
        builder = (
            SparkSession.builder
            .appName("Generic Loader")
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
        )
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def read(self, path: str, format: str = "delta"):
        if format == "delta":
            return self.spark.read.format("delta").load(path)
        elif format == "parquet":
            return (
                self.spark.read
                .option("mergeSchema", "false")
                .option("enableVectorizedReader", "false")
                .parquet(path)
            )
        elif format == "json":
            return self.spark.read.option("multiline", "true").json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

    def write(self, df, path: str, format="delta", partition_cols=None, mode="overwrite"):
        writer = df.write.format(format).mode(mode)
        if partition_cols:
            writer = writer.partitionBy(partition_cols)
        writer.save(path)
