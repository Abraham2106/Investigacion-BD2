import json
from confluent_kafka import Producer
TOPIC= "orders"
producer= Producer({"bootstrap.servers": "localhost:9092"})
def delivery_report(err, msg):
    if err is not None:
        print(f"Error enviando mensaje: {err}")
    else:
        print(
            f"Mensaje enviado a "
            f"{msg.topic()} [{msg.partition()}]"
        )
def publish_order(order: dict):
    producer.produce(
        topic=TOPIC,
        key=order["country_code"],
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_report
    )
    producer.flush()