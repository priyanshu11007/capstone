from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, col

# --------------------------------------------------
# Start Spark
# --------------------------------------------------
spark = SparkSession.builder.appName("Test Gold Layer").getOrCreate()

# --------------------------------------------------
# Read GOLD data
# --------------------------------------------------
path = "data_lake/gold/sales_weather"

df = spark.read.parquet(path)

# --------------------------------------------------
# 1. Schema
# --------------------------------------------------
print("\n========== SCHEMA ==========")
df.printSchema()

# --------------------------------------------------
# 2. Sample Data
# --------------------------------------------------
print("\n========== SAMPLE DATA ==========")
df.show(10, truncate=False)

# --------------------------------------------------
# 3. Row Count
# --------------------------------------------------
print("\n========== ROW COUNT ==========")
print(df.count())

# --------------------------------------------------
# 4. Date Range
# --------------------------------------------------
print("\n========== DATE RANGE ==========")
df.select(
    min("date").alias("first_date"),
    max("date").alias("last_date")
).show()

# --------------------------------------------------
# 5. Cities
# --------------------------------------------------
print("\n========== CITIES ==========")
df.select("city").distinct().show()

# --------------------------------------------------
# 6. Avg Sales per City
# --------------------------------------------------
print("\n========== AVG SALES PER CITY ==========")
df.groupBy("city").agg(
    avg("total_sales").alias("avg_sales")
).orderBy(col("avg_sales").desc()).show()

# --------------------------------------------------
# 7. Rain vs Sales
# --------------------------------------------------
print("\n========== RAIN VS SALES ==========")
df.groupBy("is_rainy").agg(
    avg("total_sales").alias("avg_sales")
).show()

# --------------------------------------------------
# 8. Temp Category vs Sales
# --------------------------------------------------
print("\n========== TEMP CATEGORY VS SALES ==========")
df.groupBy("temp_category").agg(
    avg("total_sales").alias("avg_sales")
).show()

# --------------------------------------------------
# 9. Top Sales Days
# --------------------------------------------------
print("\n========== TOP SALES DAYS ==========")
df.orderBy(col("total_sales").desc()).select(
    "date", "city", "total_sales"
).show(10)

# --------------------------------------------------
# 10. Check Nulls
# --------------------------------------------------
print("\n========== NULL CHECK ==========")
df.select([
    col(c).isNull().cast("int").alias(c) for c in df.columns
]).groupBy().sum().show()

# --------------------------------------------------
# Stop Spark
# --------------------------------------------------
spark.stop()