import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from loader.spark_loader import SparkLoader

# Use SAME loader (Delta enabled)
loader = SparkLoader()
spark = loader.spark

# Load GOLD
df = loader.read("data_lake/gold/sales_weather", format="delta")

df.createOrReplaceTempView("sales_weather")

# ---------------------------------------------------
# Run SQL queries
# ---------------------------------------------------

print("\n--- 1. Temperature vs Sales ---")
spark.sql("""
SELECT temp_category, ROUND(AVG(total_sales),2) AS avg_sales
FROM sales_weather
GROUP BY temp_category
ORDER BY avg_sales DESC
""").show()

print("\n--- 2. Monthly Trends ---")
spark.sql("""
SELECT MONTH(date) AS month, ROUND(SUM(total_sales),2) AS revenue
FROM sales_weather
GROUP BY month
ORDER BY month
""").show()

print("\n--- 3. City Performance ---")
spark.sql("""
SELECT city, ROUND(AVG(total_sales),2) AS avg_sales
FROM sales_weather
GROUP BY city
ORDER BY avg_sales DESC
""").show()

print("\n--- 4. Rain Impact ---")
spark.sql("""
SELECT is_rainy, ROUND(AVG(total_sales),2) AS avg_sales
FROM sales_weather
GROUP BY is_rainy
""").show()

print("\n--- 5. Top Sales Day ---")
spark.sql("""
SELECT 
    date,
    city,
    total_sales
FROM sales_weather
ORDER BY total_sales DESC
LIMIT 10;
""").show()

print("\n--- 6. Weekly Trend ---")
spark.sql("""
SELECT 
    WEEKOFYEAR(date) AS week,
    ROUND(AVG(total_sales), 2) AS avg_sales
FROM sales_weather
GROUP BY week
ORDER BY week;
""").show()

print("\n--- 7. Best Performing Day of Week ---")
spark.sql("""
SELECT 
    CASE DAYOFWEEK(date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_name,
    ROUND(AVG(total_sales), 2) AS avg_sales
FROM sales_weather
GROUP BY day_name
ORDER BY avg_sales DESC;
""").show()

print("\n--- 8. Peak Sales Conditions ---")
spark.sql("""
SELECT 
    city,
    temp_category,
    is_rainy,
    ROUND(AVG(total_sales), 2) AS avg_sales
FROM sales_weather
GROUP BY city, temp_category, is_rainy
ORDER BY avg_sales DESC
LIMIT 10;
""").show()

print("\n--- 9. Sales Growth over time ---")
spark.sql("""
SELECT 
    YEAR(date) AS year,
    MONTH(date) AS month,
    ROUND(SUM(total_sales), 2) AS revenue
FROM sales_weather
GROUP BY year, month
ORDER BY year, month;
""").show()