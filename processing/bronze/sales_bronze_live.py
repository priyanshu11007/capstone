"""
processing/bronze/sales_bronze_live.py

Reads LIVE retail Parquet files and writes cleaned Parquet to bronze.
"""
from pyspark.sql.functions import col, to_timestamp, expr
from pyspark.sql.types import StringType, IntegerType, DoubleType
from ingestion.base import BaseExtractor
from loader.spark_loader import SparkLoader


class SalesBronzeLiveTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path="", layer="bronze")
        self._loader = SparkLoader()
        self.spark   = self._loader.spark

    def extract(self, **kwargs):

        raw_path = "data_lake/raw/retail_sales/uci_online_retail_live"

        self.logger.info("Reading LIVE raw retail Parquet files...")

        df = (
            self.spark.read
            .option("mergeSchema", "false")
            .option("enableVectorizedReader", "false")
            .parquet(raw_path)
        )

        # Cast every column immediately
        df = df.withColumn("InvoiceNo",   col("InvoiceNo").cast(StringType()))
        df = df.withColumn("StockCode",   col("StockCode").cast(StringType()))
        df = df.withColumn("Description", col("Description").cast(StringType()))
        df = df.withColumn("Country",     col("Country").cast(StringType()))
        df = df.withColumn("Quantity",    col("Quantity").cast(IntegerType()))
        df = df.withColumn("UnitPrice",   col("UnitPrice").cast(DoubleType()))
        df = df.withColumn("CustomerID",  col("CustomerID").cast(DoubleType()))

        # Cleaning
        df = df.filter(col("CustomerID").isNotNull())
        df = df.withColumn("CustomerID", col("CustomerID").cast(IntegerType()))
        df = df.withColumn("InvoiceDate", col("InvoiceDate").cast(StringType()))
        df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate")))

        # City assignment
        cities = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"]
        df = df.withColumn(
            "city",
            expr(f"""
                CASE
                    WHEN CustomerID % 5 = 0 THEN '{cities[0]}'
                    WHEN CustomerID % 5 = 1 THEN '{cities[1]}'
                    WHEN CustomerID % 5 = 2 THEN '{cities[2]}'
                    WHEN CustomerID % 5 = 3 THEN '{cities[3]}'
                    ELSE '{cities[4]}'
                END
            """)
        )

        df = df.withColumn("date", col("InvoiceDate").cast("date"))

        country_col = "Country" if "Country" in df.columns else "country"
        df = df.select(
            col("InvoiceNo").cast(StringType()),
            col("StockCode").cast(StringType()),
            col("Description").cast(StringType()),
            col("Quantity").cast(IntegerType()),
            col("UnitPrice").cast(DoubleType()),
            col("CustomerID").cast(IntegerType()),
            col(country_col).cast(StringType()).alias("Country"),
            col("city").cast(StringType()),
            col("date"),
        )

        self.logger.info(f"Sales Bronze Live row count: {df.count()}")

        output_path = "data_lake/bronze/sales/online_retail_live"
        (
            df.write
            .mode("overwrite")
            .partitionBy("city")
            .parquet(output_path)
        )

        self.logger.info(f"✔ Written Sales Bronze Live → {output_path}")
        return output_path

    def validate(self, data):
        return True
