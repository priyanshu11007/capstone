from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, expr
from ingestion.base import BaseExtractor


class SalesBronzeTransformer(BaseExtractor):

    def __init__(self):
        super().__init__(config_path="", layer="bronze")

        self.spark = (
            SparkSession.builder
            .appName("Sales Bronze Transformation")
            .getOrCreate()
        )

    def extract(self, **kwargs):

        raw_path = "data_lake/raw/retail_sales/uci_online_retail"

        self.logger.info("Reading raw parquet files using Spark...")

        df = self.spark.read.parquet(raw_path)

        # -------------------------------------------------------
        # Minimal cleaning
        # -------------------------------------------------------
        df = df.filter(col("CustomerID").isNotNull())

        df = df.withColumn("CustomerID", col("CustomerID").cast("int"))

        df = df.withColumn(
            "InvoiceDate",
            to_timestamp(col("InvoiceDate"))
        )

        # -------------------------------------------------------
        # Add CITY (simulation)
        # -------------------------------------------------------
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

        # -------------------------------------------------------
        # Align DATE (shift +12 years)
        # -------------------------------------------------------
        df = df.withColumn(
            "InvoiceDate",
            expr("add_months(InvoiceDate, 12*12)")
        )

        df = df.withColumn("date", col("InvoiceDate").cast("date"))

        # -------------------------------------------------------
        # Select required columns
        # -------------------------------------------------------
        country_col = "Country" if "Country" in df.columns else "country"

        df = df.select(
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "UnitPrice",
            "CustomerID",
            country_col,
            "city",
            "date"
        )

        self.logger.info(f"Bronze row count: {df.count()}")

        # -------------------------------------------------------
        # Write partitioned output
        # -------------------------------------------------------
        output_path = "data_lake/bronze/sales/online_retail"

        (
            df.write
            .mode("overwrite")
            .partitionBy("city")
            .parquet(output_path)
        )

        self.logger.info(f"✔ Written Bronze data → {output_path}")

        return output_path

    def validate(self, data):
        return True