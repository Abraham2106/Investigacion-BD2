import sys
import json
import time
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import test_colors as tc

KAFKA_SERVER = 'localhost:9092'
TOPIC_NAME = 'test-connection-topic'

def run_kafka_test():
    tc.print_blue("Prueba de conexion Apache Kafka")
    
    try:
        # Intentar conexion al puerto 9092
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER, client_id='KafkaTest')
        tc.print_green("Estad: Conectado al puerto 9092.")
    except Exception as e:
        tc.print_red(f"Error durante la prueba de Kafka: {e}")
        sys.exit(1)
        
    # Limpiar si el topico ya existia
    if TOPIC_NAME in admin_client.list_topics():
        admin_client.delete_topics([TOPIC_NAME])
        
    # Crear topico temp para prueba
    try:
        new_topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        tc.print_green("Se creo topico de prueba.")
    except Exception as e:
        tc.print_red(f"Error al crear el topico: {e}")
        admin_client.close()
        sys.exit(1)
        
    # Enviar mensajes de prueba
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        for i in range(10):
            producer.send(TOPIC_NAME, key=f"key_{i}", value={"message_id": i})
            time.sleep(0.05)  # Breve pausa para apreciar la barra de progreso
            tc.show_progress(i + 1, 10, prefix="Produciendo:")
            
        producer.flush()
        producer.close()
        tc.print_green("Mensajes enviados.")
    except Exception as e:
        tc.print_red(f"Error al enviar: {e}")
        admin_client.delete_topics([TOPIC_NAME])
        admin_client.close()
        sys.exit(1)
        
    # Leer mensajes de prueba
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='test-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            consumer_timeout_ms=2000
        )
        
        messages_received = []
        for message in consumer:
            messages_received.append(message.value)
            time.sleep(0.05)  # Breve pausa para apreciar la barra de progreso
            tc.show_progress(len(messages_received), 10, prefix="Consumiendo:")
            
        consumer.close()
        
        if len(messages_received) == 10:
            tc.print_green("Envio y recepcion exitoso.")
        else:
            tc.print_red("Error al recuperar el valor.")
            sys.exit(1)
            
    except Exception as e:
        tc.print_red(f"Error al consumir: {e}")
        sys.exit(1)
    finally:
        # Eliminar topico y cerrar conexion
        admin_client.delete_topics([TOPIC_NAME])
        admin_client.close()
        tc.print_green("Topico destruido y conexion cerrada.")
        tc.print_blue("Fin del test.")

if __name__ == "__main__":
    run_kafka_test()
