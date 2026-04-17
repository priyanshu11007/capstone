import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from loader.spark_loader import SparkLoader
import matplotlib.pyplot as plt

# ---------------------------------------
# Setup Spark
# ---------------------------------------
loader = SparkLoader()
spark = loader.spark

df = loader.read("data_lake/gold/sales_weather", format="delta")
df.createOrReplaceTempView("sales_weather")
# ---------------------------------------
# FUNCTIONS (based on your queries)
# ---------------------------------------

def query1():
    print("\n--- 1. Temperature vs Sales ---")
    result = spark.sql("""
    SELECT temp_category, ROUND(AVG(total_sales),2) AS avg_sales
    FROM sales_weather
    GROUP BY temp_category
    ORDER BY avg_sales DESC
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.bar(pdf["temp_category"], pdf["avg_sales"])
    plt.title("Temperature vs Sales")
    plt.xlabel("Temperature Category")
    plt.ylabel("Avg Sales")
    plt.show()


def query2():
    print("\n--- 2. Monthly Trends ---")
    result = spark.sql("""
    SELECT MONTH(date) AS month, ROUND(SUM(total_sales),2) AS revenue
    FROM sales_weather
    GROUP BY month
    ORDER BY month
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.plot(pdf["month"], pdf["revenue"])
    plt.title("Monthly Sales Trends")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.show()


def query3():
    print("\n--- 3. City Performance ---")
    result = spark.sql("""
    SELECT city, ROUND(AVG(total_sales),2) AS avg_sales
    FROM sales_weather
    GROUP BY city
    ORDER BY avg_sales DESC
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.bar(pdf["city"], pdf["avg_sales"])
    plt.xticks(rotation=45)
    plt.title("City Performance")
    plt.ylabel("Avg Sales")
    plt.show()


def query4():
    print("\n--- 4. Rain Impact ---")
    result = spark.sql("""
    SELECT is_rainy, ROUND(AVG(total_sales),2) AS avg_sales
    FROM sales_weather
    GROUP BY is_rainy
    """)
    result.show()

    pdf = result.toPandas()
    labels = ["No Rain", "Rain"]
    plt.figure()
    plt.bar(labels, pdf["avg_sales"])
    plt.title("Rain Impact on Sales")
    plt.ylabel("Avg Sales")
    plt.show()


def query5():
    print("\n--- Peak Sales Day (Chart) ---")

    result = spark.sql("""
    SELECT 
        date,
        city,
        total_sales
    FROM sales_weather
    ORDER BY total_sales DESC
    LIMIT 10
    """)

    result.show()

    pdf = result.toPandas()

    # Create label: date + city
    pdf["label"] = pdf["date"].astype(str) + " (" + pdf["city"] + ")"

    # Plot
    plt.figure()
    plt.bar(pdf["label"], pdf["total_sales"])

    plt.title("Top 10 Peak Sales Days")
    plt.xlabel("Date (City)")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=45)

    plt.tight_layout()
    plt.show()


def query6():
    print("\n--- 6. Weekly Trend ---")
    result = spark.sql("""
    SELECT 
        WEEKOFYEAR(date) AS week,
        ROUND(AVG(total_sales), 2) AS avg_sales
    FROM sales_weather
    GROUP BY week
    ORDER BY week
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.plot(pdf["week"], pdf["avg_sales"])
    plt.title("Weekly Trend")
    plt.xlabel("Week")
    plt.ylabel("Avg Sales")
    plt.show()


def query7():
    print("\n--- 7. Best Performing Day of Week ---")
    result = spark.sql("""
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
    ORDER BY avg_sales DESC
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.bar(pdf["day_name"], pdf["avg_sales"])
    plt.xticks(rotation=45)
    plt.title("Best Day of Week")
    plt.ylabel("Avg Sales")
    plt.show()


def query8():
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
    LIMIT 10
    """).show()


def query9():
    print("\n--- 9. Sales Growth over time ---")
    result = spark.sql("""
    SELECT 
        YEAR(date) AS year,
        MONTH(date) AS month,
        ROUND(SUM(total_sales), 2) AS revenue
    FROM sales_weather
    GROUP BY year, month
    ORDER BY year, month
    """)
    result.show()

    pdf = result.toPandas()
    plt.figure()
    plt.plot(pdf["month"], pdf["revenue"])
    plt.title("Sales Growth Over Time")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.show()

import matplotlib.pyplot as plt

def query10():
    print("\n--- Top Products on Rainy Days ---")

    df = loader.read("data_lake/gold/fact_sales_weather_product", format="delta")

    df.createOrReplaceTempView("fact_sales_weather_product")

    result = spark.sql("""
        SELECT 
            product_description,
            SUM(total_price)   AS total_revenue,
            SUM(Quantity)      AS total_units
        FROM fact_sales_weather_product
        WHERE is_rainy = 1
        GROUP BY product_description
        ORDER BY total_revenue DESC
        LIMIT 15
    """)

    # Show table
    result.show(truncate=False)

    # Convert to pandas
    pdf = result.toPandas()

    # ---------------------------------------------------
    # Plot graph
    # ---------------------------------------------------
    plt.figure()

    plt.barh(pdf["product_description"], pdf["total_revenue"])

    plt.xlabel("Total Revenue")
    plt.ylabel("Product")
    plt.title("Top 15 Products on Rainy Days")

    plt.gca().invert_yaxis()  # highest on top

    plt.tight_layout()
    plt.show()

# ---------------------------------------
# CLI MENU
# ---------------------------------------

def main():
    while True:
        print("\nSelect an option:")
        print("1. Temperature vs Sales")
        print("2. Monthly Trends")
        print("3. City Performance")
        print("4. Rain Impact")
        print("5. Top Sales Day")
        print("6. Weekly Trend")
        print("7. Best Day of Week")
        print("8. Peak Conditions")
        print("9. Sales Growth")
        print("10.Top Products on Rainy Days")
        print("11. Exit")

        choice = input("Enter choice: ")

        if choice == "1":
            query1()
        elif choice == "2":
            query2()
        elif choice == "3":
            query3()
        elif choice == "4":
            query4()
        elif choice == "5":
            query5()
        elif choice == "6":
            query6()
        elif choice == "7":
            query7()
        elif choice == "8":
            query8()
        elif choice == "9":
            query9()
        elif choice == "10":
            query10()
            break
        else:
            print("Invalid choice!")


if __name__ == "__main__":
    main()