import json
import time
from pyignite import Client
from pyspark.sql import SparkSession

KUDU_MASTER      = "127.0.0.1:7051"
TABLE_NAME       = "orders"
REFRESH_INTERVAL = 5   # segundos — minimo de Grafana

def build_spark() -> SparkSession:
    spark = SparkSession.builder \
        .appName("AnalyticsRefresh") \
        .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def compute(spark: SparkSession) -> tuple:
    df = spark.read \
        .format("org.apache.kudu.spark.kudu") \
        .option("kudu.master", KUDU_MASTER) \
        .option("kudu.table", TABLE_NAME) \
        .load()
    df.createOrReplaceTempView("orders")

    by_country = [json.loads(r) for r in spark.sql("""
        SELECT country_code, ROUND(SUM(amount), 2) AS total_revenue
        FROM orders
        WHERE status = 'APPROVED'
        GROUP BY country_code ORDER BY total_revenue DESC
    """).toJSON().collect()]

    by_status = [json.loads(r) for r in spark.sql("""
        SELECT status, COUNT(*) AS total_orders
        FROM orders GROUP BY status
    """).toJSON().collect()]

    by_product = [json.loads(r) for r in spark.sql("""
        SELECT product, ROUND(SUM(amount), 2) AS total_revenue
        FROM orders
        WHERE status = 'APPROVED'
        GROUP BY product ORDER BY total_revenue DESC
    """).toJSON().collect()]

    recent_orders = [json.loads(r) for r in spark.sql("""
        SELECT order_id, user_id, country_code, product, amount, status, timestamp
        FROM orders ORDER BY timestamp DESC LIMIT 10
    """).toJSON().collect()]

    return by_country, by_status, by_product, recent_orders

def main():
    print("Iniciando motor de refresco de metricas...")

    # Spark se levanta una sola vez; la sesion se reutiliza en cada ciclo
    spark = build_spark()

    ignite = Client()
    ignite.connect("localhost", 10800)
    cache_country = ignite.get_or_create_cache("analytics_by_country")
    cache_status  = ignite.get_or_create_cache("analytics_by_status")
    cache_product = ignite.get_or_create_cache("analytics_by_product")
    cache_recent  = ignite.get_or_create_cache("recent_orders")

    print(f"Refrescando cada {REFRESH_INTERVAL}s. Ctrl+C para detener.")

    while True:
        try:
            t0 = time.perf_counter()
            by_country, by_status, by_product, recent_orders = compute(spark)

            cache_country.put("data", json.dumps(by_country))
            cache_status.put("data",  json.dumps(by_status))
            cache_product.put("data", json.dumps(by_product))
            cache_recent.put("data",  json.dumps(recent_orders))

            elapsed = (time.perf_counter() - t0) * 1000
            print(f"Actualizado en {elapsed:.0f}ms | paises={len(by_country)} productos={len(by_product)} ordenes={sum(r['total_orders'] for r in by_status)}")

        except Exception as e:
            print(f"Error en ciclo: {e}")

        time.sleep(REFRESH_INTERVAL)

if __name__ == "__main__":
    main()
