import sys
import os

import json
import time
from datetime import datetime
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from ignite_client import risk_countries_cache, user_velocity_cache

"""
Topics:
    orders:
        Topic donde se reciben las ordenes
    orders-processed:
        Topic donde se envian las ordenes procesadas
"""

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "orders"
OUTPUT_TOPIC = "orders-processed"

# Se levanta productor para las ordenes procesadas
producer = Producer({"bootstrap.servers": KAFKA_BROKER})

# Crear el tópico de forma pro-activa si no existe
_admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
_existing = _admin.list_topics(timeout=5).topics
if INPUT_TOPIC not in _existing:
    _admin.create_topics([NewTopic(INPUT_TOPIC, num_partitions=3, replication_factor=1)])[INPUT_TOPIC].result()
if OUTPUT_TOPIC not in _existing:
    _admin.create_topics([NewTopic(OUTPUT_TOPIC, num_partitions=3, replication_factor=1)])[OUTPUT_TOPIC].result()

# Consumidor con datos de orders ( el auto offset reset es para que lea desde el inicio )
# Obtenido de : https://stackoverflow.com/questions/48320672/what-is-the-difference-between-kafka-earliest-and-latest-offset-values

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'fraud_detector_group',
    'auto.offset.reset': 'earliest'
})

# El consumidor comienza a escuchar el topic orders 
consumer.subscribe([INPUT_TOPIC])

def check_fraud(order):
    """ Se validan por medio de reglas"""

    # R1 - PAis de riesgo 
    country = order.get("country_code")
    is_risk = risk_countries_cache.get(country)
    if is_risk:
        return "BLOCKED", "RISK_COUNTRY"
    
    # R2 - Velocidad de la orden 
    user_id = order.get("user_id")
    current_time = time.time()
    
    # R2.1 - Se revisa el historial del usuario y si hace mas de 3 ordenes en menos de 60 segundos se bloquea la orden
    history_str = user_velocity_cache.get(user_id)
    if not history_str:
        history = [1, current_time]
    else:
        history = json.loads(history_str)
        count, first_timestamp = history
        if current_time - first_timestamp < 60:
            count += 1
            if count > 3:
                return "BLOCKED", "HIGH_VELOCITY"
            history = [count, first_timestamp]
        else:
            # Si ya transcurrio un minuto, reinicia el contador ( no es tan seguro pero elimina a muchos bots)
            history = [1, current_time]
            
    # Guardar estado actualizado en Ignite en formato JSON
    user_velocity_cache.put(user_id, json.dumps(history))
    
    return "APPROVED", "OK"

print(f"Worker escuchando en topic '{INPUT_TOPIC}' y detectando fraude")

try:
    while True:

        # Se usa un poll de 1 segundo para que el consumer no se quede "trabado" 
        # Obtenido de : https://docs.rs/rdkafka/0.36.2/rdkafka/consumer/base_consumer/struct.BaseConsumer.html#method.poll

        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Error en consumidor: {msg.error()}")
            continue

        order_json = msg.value().decode('utf-8')
        order = json.loads(order_json)
        
        status, reason = check_fraud(order)
        
        # EL objetivo de ignite es este; "enriquecer" la orden para que pueda ser procesada y declarada como fraudelante o no 
        order["status"] = status
        order["reason"] = reason
        order["timestamp"] = datetime.utcnow().isoformat() + "Z"  
        
        # Se envia al topic de ordenes procesadas 
        producer.produce(
            topic=OUTPUT_TOPIC,
            key=order["country_code"],
            value=json.dumps(order).encode("utf-8")
        )
        producer.flush() # Para confirmar el envio del mensaje - Obtenido de : https://stackoverflow.com/questions/52589772/kafka-producer-difference-between-flush-and-poll
        
        # Pretty print de la orden
        print(f"Orden {order['order_id']} | País: {order['country_code']} | Usuario: {order['user_id']} -> {status} ({reason})")

except KeyboardInterrupt:
    print("GG...")
finally:
    consumer.close()