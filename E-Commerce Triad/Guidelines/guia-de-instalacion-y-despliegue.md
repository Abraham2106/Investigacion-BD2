# Guía de Instalación y Despliegue

## 1. Levantamiento del Entorno (Docker Compose)

```bash
# 1. Configurar la IP de red del host local
export KUDU_QUICKSTART_IP=$(hostname -I | awk '{print $1}')

# 2. Levantar todos los servicios en segundo plano
docker compose up -d
```

---

## 2. Configuración del Entorno de Desarrollo (Python venv)

Para poder ejecutar los simuladores e interactuar con los puertos de red del clúster se debe de trabajar con un entorno virtual en Python.
```bash
python3 -m venv .venv

source .venv/bin/activate
# Dependencias para interactuar con el cluster
pip install pyignite kafka-python-ng pyspark==3.5.1
```

### Detalle tecnico de las dependencias:
*   **pyignite**: Cliente thin para interactuar con Apache Ignite en memoria.
*   **kafka-python-ng**: Cliente para producir y consumir eventos en Apache Kafka.
*   **pyspark (version 3.5.1)**: Puente JVM compatible con Scala 2.12 para conectarse a Apache Kudu.

---

## 3. Verificaciones Tecnicas y Diagnostico de Estado

Comandos para verificar el correcto funcionamiento y comunicación de cada servicio del ecosistema.

### 3.1. Apache Kudu (Puertos 7051 y 8051)

*   **Verificar contenedores activos:**
    ```bash
    docker ps --filter "name=kudu"
    ```
*   **Entrar a la terminal interactiva del Master:**
    ```bash
    docker exec -it kudu-master-1 /bin/bash
    ```
*   **Ejecutar diagnostico de salud del cluster (Kudu CLI ksck):**
    ```bash
    docker exec -it kudu-master-1 kudu cluster ksck kudu-master-1:7051
    ```
*   **Acceder a la Consola Web del Master:**
    Abrir `http://localhost:8051` en el navegador para ver tablas, tablets y configuraciones.

### 3.2. Apache Kafka (Puerto 9092)

*   **Listar topicos activos:**
    ```bash
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    ```
*   **Crear topico 'orders' con 3 particiones:**
    ```bash
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
    ```

### 3.3. Apache Ignite (Puerto 10800)

*   **Correr prueba rapida de conexion y cache:**
    ```bash
    python test/test_ignite.py      
    ``` 
    Nota: se requiere crear un archivo test/test_ignite.py con el siguiente contenido:
    ```python
    import ignite.contig_api as ig

    def test_ignite_connection():
        nodes = [("localhost", 10800)]
        ignite = ig.SparkDataFrame(nodes)
    
        # Intentar crear una cache de prueba
        try:
            ignite.cache("test_cache")
            print("Exito")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            ignite.close()

    test_ignite_connection()
    ```

### 3.4. Grafana (Puerto 3000)

*   **Acceso a la interfaz web:**
    Abrir `http://localhost:3000` en el navegador (Usuario: `admin` / Contrasena: `admin`).