import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC  = "orders-processed"
KUDU_MASTER  = "127.0.0.1:7051"
TABLE_NAME   = "orders"

SCHEMA = StructType([
    StructField("order_id",     StringType(), False),
    StructField("user_id",      StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("product",      StringType(), True),
    StructField("amount",       DoubleType(), True),
    StructField("status",       StringType(), True),
    StructField("timestamp",    StringType(), True),
])

def main():
    print("Iniciando Kudu Structured Streaming Drainer...")
    
    pyspark_version = pyspark.__version__
    print(f"Detectada version local de PySpark: {pyspark_version}")

    spark = SparkSession.builder \
        .appName("KuduDrainer") \
        .config("spark.jars.packages", f"org.apache.kudu:kudu-spark3_2.12:1.17.0,org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Leer flujo desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parsear JSON y filtrar solo APPROVED
    processed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), SCHEMA).alias("data")) \
        .select("data.*") \
        .filter(col("status") == "APPROVED")   # RF-11

    print(f"Escribiendo flujo de streaming directamente en la tabla Kudu '{TABLE_NAME}'...")

    query = processed_df.writeStream \
        .format("org.apache.kudu.spark.kudu") \
        .option("kudu.master", KUDU_MASTER) \
        .option("kudu.table", TABLE_NAME) \
        .option("kudu.operation", "upsert") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/kudu_drain_checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()