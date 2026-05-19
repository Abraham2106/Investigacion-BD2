# Especificación de Requerimientos — E-Commerce Triad Pipeline

Sistema de E-Commerce basado en **FastAPI + Apache Kafka + Apache Ignite + PySpark + Apache Kudu**, diseñado para ingerir, procesar y almacenar pedidos en tiempo real, detectando fraudes y filtrando zonas de riesgo.

---

## 1. Requerimientos Funcionales (RF)

> [!TIP]
> Si desea generar tablas como estas puede hacerlo en [Tables Generator](https://www.tablesgenerator.com/markdown_tables)

### Módulo de Ingesta (FastAPI + Kafka)

| ID    | Descripción |
|-------|-------------|
| RF-01 | El sistema debe exponer un endpoint `POST /orders` para la recepción de pedidos en formato JSON. |
| RF-02 | Cada pedido recibido debe publicarse en el topic `orders` de Kafka utilizando `country_code` como clave de partición. |
| RF-03 | Si un pedido llega con campos faltantes o con tipos de datos incorrectos (ej. precio en texto en lugar de número), el endpoint debe rechazarlo con un error `422` antes de enviarlo a Kafka. |
| RF-04 | Al recibir un pedido válido, el endpoint debe responder con el `order_id` del pedido y el estado `queued`, confirmando que ya está en la cola de procesamiento. |
| RF-05 | Debe existir un script de Python que genere y envíe pedidos de prueba de forma automática, con valores aleatorios de `user_id`, `country_code`, `product` y `amount`. |

### Módulo de Procesamiento en Caliente (Ignite)

| ID    | Descripción |
|-------|-------------|
| RF-06 | El sistema debe leer cada pedido del topic `orders` y asignarle una clasificación: `APPROVED` si pasa los filtros, o `BLOCKED` si es detectado como sospechoso. |
| RF-07 | Si el `country_code` de un pedido está en la caché `risk_countries` de Ignite, el pedido se clasifica como `BLOCKED` automáticamente. |
| RF-08 | Si un `user_id` realiza más compras de las permitidas en una ventana de 1 minuto (detectado con la caché `user_velocity`), sus pedidos siguientes se clasifican como `BLOCKED`. |
| RF-09 | Después de clasificar cada pedido, el resultado completo debe publicarse en el topic `orders-processed` para que el módulo de almacenamiento pueda consumirlo. |
| RF-10 | Debe ser posible agregar o quitar `country_code` de la caché `risk_countries` en Ignite en tiempo real, sin reiniciar ningún script ni contenedor. |

### Módulo de Almacenamiento en Frío (Kudu)

| ID    | Descripción |
|-------|-------------|
| RF-11 | El sistema debe leer solo los pedidos con estado `APPROVED` del topic `orders-processed` y guardarlos permanentemente en una tabla de Kudu. |
| RF-12 | Al escribir en Kudu se deben usar operaciones `upsert` (actualizar si ya existe, insertar si no) para evitar registros duplicados cuando Kafka reenvía el mismo mensaje. |
| RF-13 | Los registros en Kudu deben distribuirse entre particiones usando `order_id` como hash, para que ningún tablet concentre toda la carga de escritura. |
| RF-14 | El sistema debe soportar consultas analíticas sobre la tabla de Kudu: total de ventas por `country_code`, revenue por `product` y conteo de pedidos `APPROVED` vs `BLOCKED`. |

### Módulo de Visualización (Grafana)

| ID    | Descripción |
|-------|-------------|
| RF-15 | El sistema debe contar con un panel (dashboard) en Grafana que se conecte a la base de datos Kudu para mostrar gráficos interactivos. |
| RF-16 | El panel debe mostrar visualmente el total de ingresos por país y la cantidad de pedidos aprobados contra los bloqueados. |

### Infraestructura

| ID    | Descripción |
|-------|-------------|
| RF-17 | Todos los servicios (Kafka, Ignite, Kudu, Grafana) deben poder levantarse con el comando `docker-compose up -d` en la máquina local, sin configuración adicional. |
| RF-18 | FastAPI debe exponer automáticamente la UI de Swagger en `http://localhost:8000/docs` para poder probar el endpoint `POST /orders` desde el navegador.


---

## 2. Historias de Usuario (HU)

### Ingesta de Pedidos

**HU-01 — Registrar un pedido**
> Como **usuario**, quiero poder enviar un pedido de prueba vía API para comprobar que mi sistema de ingesta lo recibe, valida y responde correctamente.

*Criterios de aceptación:*
- El endpoint `POST /orders` acepta un JSON con `order_id`, `user_id`, `country_code`, `product` y `amount`.
- El sistema responde con `{"status": "queued", "order_id": "..."}` de manera rápida en mi máquina local.
- Si falta algún campo obligatorio, el sistema responde con un error `422` detallando qué falló.

---

**HU-02 — Simular actividad de E-Commerce**
> Como **usuario**, quiero contar con un script simulador para automatizar el envío de múltiples pedidos y así evaluar el comportamiento del pipeline local sin ingresar datos manualmente.

*Criterios de aceptación:*
- El script genera pedidos con `country_code`, `user_id`, `product` y `amount` aleatorios.
- Puede configurarse para enviar N pedidos por segundo desde mi terminal local.
- Los pedidos simulados cubren múltiples regiones para probar el sharding.

---

### Detección de Fraude

**HU-03 — Filtrar pedidos por lista negra de países**
> Como **usuario**, quiero implementar reglas en memoria (lista de riesgo) para bloquear pedidos de países sospechosos y así aprender a filtrar transacciones en tiempo real antes de guardarlas.

*Criterios de aceptación:*
- Los pedidos con `country_code` en la lista `risk_countries` se marcan como `BLOCKED`.
- Puedo actualizar esta lista en memoria sin necesidad de reiniciar mis contenedores o scripts.
- El bloqueo se decide en la capa de procesamiento caliente (Ignite) antes de llegar a la persistencia.

---

**HU-04 — Detectar compras sospechosas por velocidad**
> Como **usuario**, quiero medir la velocidad de compra por usuario en ventanas de 1 minuto para aprender a identificar comportamientos automatizados (bots) en caliente.

*Criterios de aceptación:*
- Si un `user_id` supera el límite de compras en 1 minuto, su siguiente pedido se bloquea automáticamente (`BLOCKED`).
- El contador en memoria se reinicia limpiamente al iniciar cada minuto.
- El límite es fácilmente ajustable para poder probarlo localmente.

---

### Almacenamiento y Analítica

**HU-05 — Guardar de forma duradera los pedidos aprobados**
> Como **usuario**, quiero guardar de forma persistente solo los pedidos aprobados en Kudu para comprender los mecanismos de almacenamiento e idempotencia en motores HTAP.

*Criterios de aceptación:*
- Únicamente los pedidos clasificados como `APPROVED` se registran en la tabla de Kudu.
- Si el mismo pedido es procesado dos veces por Kafka, Kudu lo resuelve con un `upsert` sin duplicar el registro.
- Puedo detener y reiniciar mi script de persistencia sin perder los mensajes acumulados en Kafka.

---

**HU-06 — Consultar ingresos por país**
> Como **usuario**, quiero poder realizar consultas analíticas rápidas sobre los datos de Kudu para aprender a calcular reportes acumulados (como ventas por región) usando almacenamiento columnar.

*Criterios de aceptación:*
- La consulta SQL o API me permite obtener la suma de ventas (`amount`) agrupadas por `country_code`.
- La consulta responde en pocos segundos con mis datos de prueba locales.

---

**HU-07 — Consultar tasa de bloqueos y fraudes**
> Como **usuario**, quiero consultar la proporción de pedidos aprobados versus bloqueados para comprobar visualmente la efectividad de mis reglas de filtrado implementadas.

*Criterios de aceptación:*
- La consulta me devuelve la cantidad exacta de pedidos clasificados como `APPROVED` y `BLOCKED`.
- Los resultados reflejan de inmediato el estado del procesamiento caliente local.

---

### Visualización de Datos

**HU-08 — Visualizar Dashboards en Grafana**
> Como **usuario**, quiero visualizar el resumen de ventas y fraudes en un panel interactivo de Grafana para analizar la información del proyecto de una forma mucho más gráfica, amigable y fácil de entender.

*Criterios de aceptación:*
- Cuento con un dashboard en Grafana conectado a Kudu.
- Puedo ver gráficos (como barras o pasteles) que me muestran los pedidos `APPROVED` vs `BLOCKED`.
- Los gráficos me ayudan a ver los resultados sin necesidad de ejecutar consultas SQL de forma manual.

---

### Infraestructura

**HU-09 — Levantar el ecosistema local con un comando**
> Como **usuario**, quiero poder levantar toda mi infraestructura local (Kafka, Ignite, Kudu) con un solo comando para ahorrar tiempo y concentrarme puramente en la programación.

*Criterios de aceptación:*
- `docker-compose up -d` arranca todos los servicios necesarios en contenedores.
- El stack queda listo para recibir pedidos en pocos segundos.
- Cuento con un listado claro de las librerías Python necesarias para instalar de forma sencilla.

---

## 3. Especificación de Formatos JSON (Estructuras de Datos)

A continuación se detallan las estructuras exactas de datos que fluyen a través de la API y el bus de eventos de Kafka.

### 3.1. Carga de Entrada (`POST /orders`)
Representa el JSON que el simulador o cliente envía al endpoint expuesto por FastAPI.

**Ejemplo de Petición (Request Body):**
```json
{
  "order_id": "",
  "user_id": "",
  "country_code": "",
  "product": "",
  "amount": 0.0
}
```

---

### 3.2. Respuestas del Servidor (FastAPI)

**Respuesta Exitosa (`202 Accepted` o `200 OK`):**
Se retorna cuando el payload pasa las validaciones de tipo en FastAPI y se publica exitosamente en Kafka (`orders`).
```json
{
  "status": "",
  "order_id": ""
}
```

**Respuesta por Error de Validación (`422 Unprocessable Entity`):**
Se genera automáticamente si falta algún campo requerido o los tipos de datos son inválidos.
```json
{
  "detail": [
    {
      "loc": ["", ""],
      "msg": "",
      "type": ""
    }
  ]
}
```

---

### 3.3. Evento Enriquecido (`orders-processed`)
Representa el JSON final generado tras el procesamiento en caliente con Apache Ignite, el cual incluye la evaluación de fraude (`status`) y la estampa de tiempo (`timestamp`), listo para ser consumido y guardado en Apache Kudu.

```json
{
  "order_id": "",
  "user_id": "",
  "country_code": "",
  "product": "",
  "amount": 0.0,
  "status": "",
  "timestamp": ""
}
```

---

### 3.4. Esquema de Cachés en Apache Ignite (Capa en Memoria)

Las cachés en Apache Ignite se definen bajo un modelo Clave-Valor (`Key-Value`) en memoria RAM para un acceso ultra rápido.

#### Caché: `risk_countries`
Lista de países bloqueados por políticas de riesgo.
* **Clave (`Key`):** `country_code` (Tipo: `String`)
* **Valor (`Value`):** `is_active` (Tipo: `Boolean` o vacío/presencia en la caché)

#### Caché: `user_velocity`
Caché temporal utilizada para contar transacciones por usuario en un intervalo de tiempo.
* **Clave (`Key`):** `user_id` (Tipo: `String`)
* **Valor (`Value`):** `transaction_count` (Tipo: `Integer`)
* **Política de Expiración (TTL):** Expiración automática tras 1 minuto.

---

### 3.5. Esquema de Tabla Columnar en Apache Kudu (Capa Fría)

La persistencia definitiva y analítica en Apache Kudu requiere la definición rigurosa de columnas, tipos y estrategias de sharding (particionado).

#### Definición de Tabla: `orders`

| Columna | Tipo de Datos Kudu | Clave Primaria | ¿Permite Nulos? |
| :--- | :--- | :---: | :---: |
| `order_id` | `STRING` | Sí | No |
| `user_id` | `STRING` | No | No |
| `country_code` | `STRING` | No | No |
| `product` | `STRING` | No | No |
| `amount` | `DOUBLE` | No | No |
| `status` | `STRING` | No | No |
| `timestamp` | `UNIX_TIME_MICROS` | No | No |

#### Estrategia de Particionado (Sharding)

| Propiedad | Configuración |
| :--- | :--- |
| **Algoritmo** | Particionado por Hash (`Hash Partitioning`) |
| **Columna de Sharding** | `order_id` |
| **Número de Tablets** | `3` |

---

### 3.6. Consideraciones Técnicas: PySpark como Puente Python ↔ Kudu

El script `03_kudu_drain.py` utiliza **PySpark** para interactuar con Kudu. A continuación se detallan las restricciones de versión, el patrón de DDL y el patrón de escritura que deben respetarse estrictamente en la implementación.

#### 3.6.1. Versión Requerida

| Componente | Versión | Motivo |
| :--- | :--- | :--- |
| `pyspark` | `3.5.1` | Única versión de PySpark compilada sobre Scala 2.12, requerida por el conector Kudu |
| `kudu-spark3_2.12` | `1.17.0` | Conector oficial de Kudu para Spark 3.x / Scala 2.12, descargado automáticamente desde Maven |
| Java / JVM | 11+ | Requerida en el sistema operativo; PySpark la inicia automáticamente como proceso de fondo |

> [!CAUTION]
> No instalar `pyspark==4.x`. Usa Scala 2.13, que es **binariamente incompatible** con `kudu-spark3_2.12` y produce un `java.lang.NoClassDefFoundError: scala/Serializable` en tiempo de ejecución.

#### 3.6.2. Instalación del Entorno

Dado que Ubuntu 22.04+ protege el entorno Python del sistema (PEP 668), se debe usar un entorno virtual:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.1
```

#### 3.6.3. Patrón de Inicialización de Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KuduDrainer") \
    .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
    .getOrCreate()
```

El parámetro `spark.jars.packages` le indica a Spark que descargue el JAR del conector desde Maven Central en el primer arranque. Las ejecuciones subsecuentes lo toman del caché local (`~/.ivy2/jars`), por lo que son instantáneas.

#### 3.6.4. Patrón de Creación de Tabla (KuduContext y JVM) — RF-13

Dado que el conector oficial de Kudu no implementa un plugin de catálogo estándar para Spark SQL (lo cual arrojaría un error `ClassNotFoundException: org.apache.kudu.spark.kudu.KuduCatalog`), y que `CREATE TABLE ... USING kudu` en Spark SQL puro requiere que la tabla ya exista físicamente en Kudu, **el patrón industrial y robusto para crear tablas columnar en Kudu desde Python es utilizar la API nativa de `KuduContext` a través de la JVM de Spark (`spark.sparkContext._jvm`)**.

Este enfoque permite definir llaves primarias, tipos de Spark y particionamiento por Hash nativos de Kudu de forma programática y 100% segura, traduciendo tipos de datos de PySpark directamente a Scala:

```python
from pyspark.sql.types import *

sc = spark.sparkContext
jvm = sc._jvm

# Instanciar el KuduContext apuntando al Master
kudu_context = jvm.org.apache.kudu.spark.kudu.KuduContext("127.0.0.1:7051", sc._jsc.sc())

table_name = "orders"

if not kudu_context.tableExists(table_name):
    # 1. Definir el esquema regular en PySpark
    schema = StructType([
        StructField("order_id", StringType(), False),  # PK no nula
        StructField("user_id", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # 2. Traducir el esquema StructType de PySpark al StructType de Scala vía JSON
    scala_schema = jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
    
    # 3. Definir opciones de particionado y factor de replicación
    options = jvm.org.apache.kudu.client.CreateTableOptions()
    options.setNumReplicas(1)  # Soporte para clústeres ligeros de desarrollo (2 tservers)
    
    # Agregar particionado por Hash (3 buckets) usando una lista de Java
    cols_list = jvm.java.util.ArrayList()
    cols_list.add("order_id")
    options.addHashPartitions(cols_list, 3)
    
    # Traducir la clave primaria a un objeto Seq de Scala
    pk_seq = jvm.scala.collection.JavaConverters.asScalaBufferConverter(cols_list).asScala().toSeq()
    
    # 4. Crear físicamente la tabla a través del KuduContext
    kudu_context.createTable(table_name, scala_schema, pk_seq, options)
    print("Tabla creada en Kudu con éxito.")
```

> [!TIP]
> **Consideración de Replicación:** Establecer `options.setNumReplicas(1)` es crítico en clústeres de desarrollo que se han limitado a menos de 3 Tablet Servers. De lo contrario, Kudu lanzará una excepción al no poder cumplir con el factor de replicación por defecto (3).

#### 3.6.5. Patrón de Escritura Idempotente (Upsert) — RF-12

```python
df.write \
    .format("org.apache.kudu.spark.kudu") \
    .option("kudu.master", "localhost:7051,localhost:7151,localhost:7251") \
    .option("kudu.table", "orders") \
    .option("kudu.operation", "upsert") \
    .mode("append") \
    .save()
```

La opción `kudu.operation = upsert` garantiza que si Kafka reenvía el mismo `order_id`, Kudu actualizará el registro existente en lugar de duplicarlo (RF-12).

#### 3.6.6. Consideraciones de Ciclo de Vida del Cluster

- Al iniciar el script por primera vez tras un `docker compose restart`, el cluster Kudu tarda **~10-15 segundos** en elegir un líder Raft. El driver de Spark emite advertencias `Unable to find the leader master...` durante este período, lo cual es **normal y esperado**.
- Si el script de Python es interrumpido abruptamente (`Ctrl+C`), puede dejar sockets TCP huérfanos en el cluster de Kudu. Si el siguiente intento falla con errores de red Netty (`SslHandler.unwrap`), se debe reiniciar el cluster: `KUDU_QUICKSTART_IP=$(hostname -I | awk '{print $1}') docker compose restart`.





