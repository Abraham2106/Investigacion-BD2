import time
from pyspark.sql import SparkSession

KUDU_MASTER = "127.0.0.1:7051"
TABLE_NAME  = "orders"

def run_query(spark, name: str, sql_query: str) -> None:
    print(f"\n[{name}]")
    start_time = time.perf_counter()
    result_df = spark.sql(sql_query)
    rows = result_df.collect()
    elapsed_ms = (time.perf_counter() - start_time) * 1000
    result_df.show(truncate=False)
    print(f"Tiempo: {elapsed_ms:.2f} ms | Registros: {len(rows)}")

def main():
    print("Ejecutando consultas analíticas sobre Kudu...")

    spark = SparkSession.builder \
        .appName("KuduAnalytics") \
        .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read \
        .format("org.apache.kudu.spark.kudu") \
        .option("kudu.master", KUDU_MASTER) \
        .option("kudu.table", TABLE_NAME) \
        .load()
    df.createOrReplaceTempView("orders")

    # query 1: ingresos totales por pais 
    q1 = """
    SELECT country_code, ROUND(SUM(amount), 2) AS total_revenue
    FROM orders
    GROUP BY country_code
    ORDER BY total_revenue DESC
    """
    run_query(spark, "SUM(amount) GROUP BY country_code", q1)

    # query 2: cantidad total de ordenes agrupadas por estado
    q2 = """
    SELECT status, COUNT(*) AS total_orders
    FROM orders
    GROUP BY status
    """
    run_query(spark, "COUNT(*) GROUP BY status", q2)

    # query 3: ingresos totales agrupados por tipo de producto
    q3 = """
    SELECT product, ROUND(SUM(amount), 2) AS total_revenue
    FROM orders
    GROUP BY product
    ORDER BY total_revenue DESC
    """
    run_query(spark, "SUM(amount) GROUP BY product", q3)

    spark.stop()
    print("Listo.")

if __name__ == "__main__":
    main()
