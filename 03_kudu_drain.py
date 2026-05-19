import json
import time
import sys
import datetime
from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER  = "localhost:9092"
INPUT_TOPIC   = "orders-processed"
KUDU_MASTER   = "127.0.0.1:7051"
TABLE_NAME    = "orders"


# Estos datos para la reparticion del batch es trivial ya que 
# 
BATCH_SIZE    = 50
BATCH_TIMEOUT = 3.0

SCHEMA = StructType([
    StructField("order_id",     StringType(), False),
    StructField("user_id",      StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("product",      StringType(), True),
    StructField("amount",       DoubleType(), True),
    StructField("status",       StringType(), True),
    StructField("timestamp",    StringType(), True),
])

def flush(spark, buffer: list) -> None:
    try:
        df = spark.createDataFrame(buffer, SCHEMA)
        df.write \
            .format("org.apache.kudu.spark.kudu") \
            .option("kudu.master",    KUDU_MASTER) \
            .option("kudu.table",     TABLE_NAME) \
            .option("kudu.operation", "upsert") \
            .mode("append") \
            .save()
        print(f"Lote de {len(buffer)} órdenes persistido en Kudu.")
    except Exception as e:
        print(f"Error al escribir en Kudu: {e}")
    finally:
        buffer.clear()

def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id":          "kudu_drainer_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

def main():
    print("Iniciando Kudu Drainer...")

    spark = SparkSession.builder \
        .appName("KuduDrainer") \
        .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    consumer = build_consumer()
    consumer.subscribe([INPUT_TOPIC])
    print(f"Consumer suscrito a {INPUT_TOPIC}")
    buffer: list = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is not None and not msg.error():
                try:
                    order = json.loads(msg.value().decode("utf-8"))

                    status_val = str(order.get("status", "UNKNOWN"))

                    buffer.append({
                        "order_id":     str(order["order_id"]),
                        "user_id":      str(order.get("user_id", "")),
                        "country_code": str(order.get("country_code", "")),
                        "product":      str(order.get("product", "")),
                        "amount":       float(order.get("amount", 0.0)),
                        "status":       status_val,
                        "timestamp":    str(order.get(
                            "timestamp",
                            datetime.datetime.now(datetime.UTC).isoformat()
                        )),
                    })

                except Exception as parse_err:
                    print(f"Error parseando mensaje: {parse_err}")

            elif msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {msg.error()}")

            elapsed = time.time() - last_flush
            if buffer and (len(buffer) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT):
                flush(spark, buffer)
                last_flush = time.time()

    except KeyboardInterrupt:
        print("Deteniendo drainer...")
        if buffer:
            flush(spark, buffer)
    finally:
        consumer.close()
        spark.stop()
        print("Recursos cerrados.")

if __name__ == "__main__":
    main()
