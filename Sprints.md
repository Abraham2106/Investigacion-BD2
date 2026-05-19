# Plan de Sprints — E-Commerce Triad Pipeline

Este documento describe el plan detallado de sprints para el desarrollo e integración de la tubería de datos de comercio electrónico (FastAPI, Kafka, Ignite, Kudu y Grafana).

---

## Fase 1 — Infraestructura y Conectividad

### S-01: Entorno base con Docker Compose
*   **Requerimientos:** `RF-17`
*   **Objetivo:** Levantar todos los servicios del ecosistema con un solo comando y verificar que se comunican entre sí.
*   **Tareas:**
    *   [x] Crear `docker-compose.yml` con servicios: Zookeeper, Kafka Broker, Apache Ignite, Apache Kudu (master + tablet server) y Grafana.
    *   [x] Configurar redes internas (bridge) y volúmenes persistentes para cada servicio.
    *   [x] Definir variables de entorno básicas: puertos, credenciales, nombres de servicios.
    *   [x] Verificar health-checks: Kafka responde en 9092, Ignite en 10800, Kudu master en 7051.
    *   [x] Documentar los comandos de inicio, pausa y limpieza del entorno.

### S-02: Validación del entorno y conectividad
*   **Requerimientos:** `RF-17`, `RF-18`
*   **Objetivo:** Confirmar que todos los contenedores se comunican correctamente antes de escribir código de negocio.
*   **Tareas:**
    *   [x] Ejecutar un producer/consumer de prueba en Kafka y verificar que los mensajes fluyen.
    *   [x] Conectarse a Ignite via thin client (Python) y leer/escribir una clave de prueba.
    *   [x] Conectarse a Kudu, crear una tabla temporal y confirmar lectura/escritura.
    *   [x] Levantar FastAPI con un endpoint `/health` y verificar desde Swagger en `localhost:8000/docs`.
    *   [x] Crear `requirements.txt` con todas las dependencias Python del proyecto.

---

## Fase 2 — Ingesta de Datos

### S-03: API de ingesta con FastAPI
*   **Requerimientos:** `RF-01`, `RF-03`, `RF-04`, `RF-18`
*   **Objetivo:** Exponer el endpoint `POST /orders` con validación estricta de esquema y respuesta inmediata al cliente.
*   **Tareas:**
    *   [x] Definir el modelo Pydantic de entrada: `order_id`, `user_id`, `country_code`, `product`, `amount` (float).
    *   [x] Implementar `POST /orders` que valide el payload y devuelva 422 automático si faltan campos.
    *   [x] Responder con `{"status": "queued", "order_id": "..."}` tras publicación exitosa.
    *   [x] Agregar manejo de excepciones para errores de conexión con Kafka.
    *   [x] Verificar la UI de Swagger en `/docs` con ejemplos de request válido e inválido.

### S-04: Productor Kafka y particionamiento
*   **Requerimientos:** `RF-02`, `RF-05`
*   **Objetivo:** Publicar pedidos en el topic `orders` usando `country_code` como clave de partición.
*   **Tareas:**
    *   [x] Crear el topic `orders` con 3 particiones y replication factor 1 (entorno local).
    *   [x] Implementar el KafkaProducer en FastAPI usando `confluent-kafka-python`.
    *   [x] Configurar `country_code` como partition key al momento de producir el mensaje.
    *   [x] Serializar el payload como JSON (UTF-8) antes de publicar.
    *   [x] Escribir el script simulador (`05_simulator.py`) que genere N pedidos/seg con datos aleatorios.

---

## Fase 3 — Procesamiento en Caliente

### S-05: Caché de países de riesgo en Ignite
*   **Requerimientos:** `RF-07`, `RF-10`
*   **Objetivo:** Crear y poblar la caché `risk_countries` en memoria, permitiendo actualizaciones en caliente.
*   **Tareas:**
    *   [x] Conectar al cluster Ignite con el thin client de Python (`pyignite`).
    *   [x] Crear la caché `risk_countries` de tipo String → Boolean.
    *   [x] Poblar la caché con una lista inicial de países de prueba (ej. `XX`, `YY`, `ZZ`).
    *   [x] Exponer un endpoint auxiliar en FastAPI: `POST /risk-countries` para agregar/quitar países.
    *   [x] Verificar que los cambios en la caché son visibles sin reiniciar ningún script.

### S-06: Detección de fraude por velocidad de compra
*   **Requerimientos:** `RF-06`, `RF-08`, `RF-09`
*   **Objetivo:** Consumir el topic `orders`, aplicar ambas reglas de fraude y publicar en `orders-processed`.
*   **Tareas:**
    *   [x] Crear el topic `orders-processed` con las mismas particiones que `orders`.
    *   [x] Implementar el consumidor Kafka en `02_ignite_fraud.py` con group_id propio.
    *   [x] Crear la caché `user_velocity` con TTL de 60 segundos e incremento atómico por `user_id`.
    *   [x] Aplicar las dos reglas en orden: primero `country_code`, luego velocidad de usuario.
    *   [x] Enriquecer el evento con `status` (`APPROVED`/`BLOCKED`) y `timestamp` antes de publicar en `orders-processed`.

---

## Fase 4 — Almacenamiento en Frío

### S-07: Creación del esquema en Kudu
*   **Requerimientos:** `RF-11`, `RF-12`, `RF-13`
*   **Objetivo:** Definir la tabla `orders` en Kudu con las columnas, tipos y estrategia de particionado correctos.
*   **Tareas:**
    *   [ ] Conectar a Kudu Master usando `kudu-python client`.
    *   [ ] Crear la tabla `orders` con columnas: `order_id` (PK), `user_id`, `country_code`, `product`, `amount`, `status`, `timestamp`.
    *   [ ] Configurar hash partitioning sobre `order_id` con 3 tablets.
    *   [ ] Verificar que la tabla existe y está accesible desde el cliente Python.
    *   [ ] Documentar el esquema final en el README del proyecto.

### S-08: Consumidor de almacenamiento en Kudu
*   **Requerimientos:** `RF-11`, `RF-12`, `RF-14`
*   **Objetivo:** Leer solo pedidos `APPROVED` de `orders-processed` y persistirlos con `upsert` en Kudu.
*   **Tareas:**
    *   [ ] Implementar `03_kudu_drain.py` como consumidor Kafka del topic `orders-processed`.
    *   [ ] Filtrar mensajes: procesar únicamente los que tengan `status == APPROVED`.
    *   [ ] Ejecutar operación `upsert` en Kudu por cada pedido aprobado.
    *   [ ] Manejar errores de escritura: log del error y continuar sin detener el consumidor.
    *   [ ] Probar idempotencia: reenviar el mismo `order_id` dos veces y verificar que no se duplica.

### S-09: Consultas analíticas sobre Kudu
*   **Requerimientos:** `RF-14`
*   **Objetivo:** Validar que los datos en Kudu soportan las consultas analíticas requeridas por el proyecto.
*   **Tareas:**
    *   [ ] Escribir consulta: `SUM(amount) GROUP BY country_code`.
    *   [ ] Escribir consulta: `COUNT(*) GROUP BY status` (`APPROVED` vs `BLOCKED`).
    *   [ ] Escribir consulta: `SUM(amount) GROUP BY product`.
    *   [ ] Medir tiempo de respuesta de cada consulta con el dataset de prueba generado.
    *   [ ] Guardar los queries en un archivo `analytics.sql` para referencia futura.

---

## Fase 5 — Visualización e Integración

### S-10: Dashboard en Grafana
*   **Requerimientos:** `RF-15`, `RF-16`
*   **Objetivo:** Conectar Grafana a Kudu y construir un panel con los KPIs principales del pipeline.
*   **Tareas:**
    *   [ ] Configurar el datasource de Kudu en Grafana (plugin o datasource compatible).
    *   [ ] Crear panel de barras: ingresos totales por país (`country_code` vs `SUM amount`).
    *   [ ] Crear panel de pie chart o barras apiladas: pedidos `APPROVED` vs `BLOCKED`.
    *   [ ] Crear panel de tabla: top 5 productos por revenue.
    *   [ ] Exportar el dashboard como JSON y versionarlo en el repositorio.

### S-11: Pruebas de integración end-to-end
*   **Requerimientos:** `RF-01`, `RF-06`, `RF-11`, `RF-15`
*   **Objetivo:** Ejecutar el flujo completo desde el simulador hasta Grafana y verificar consistencia de datos.
*   **Tareas:**
    *   [ ] Lanzar el simulador con 500 pedidos, incluyendo países de riesgo y usuarios con alta velocidad.
    *   [ ] Verificar en Ignite que los pedidos bloqueados tienen el motivo correcto (país o velocidad).
    *   [ ] Verificar en Kudu que solo los `APPROVED` están almacenados y sin duplicados.
    *   [ ] Confirmar que los gráficos de Grafana reflejan los datos correctamente.
    *   [ ] Documentar los resultados con capturas de pantalla en el README.

### S-12: Documentación y entrega final
*   **Requerimientos:** `RF-17`, `RF-18`
*   **Objetivo:** Dejar el proyecto completamente documentado y listo para correr desde cero en cualquier máquina.
*   **Tareas:**
    *   [ ] Completar el README con arquitectura, instrucciones de instalación y ejecución paso a paso.
    *   [ ] Revisar y limpiar el `requirements.txt` con versiones fijas.
    *   [ ] Agregar archivo `.env.example` con todas las variables de entorno necesarias.
    *   [ ] Crear un script de seed inicial que pueble `risk_countries` en Ignite al arrancar.
    *   [ ] Hacer demo final del flujo completo: `docker-compose up` → simulador → Grafana con datos en vivo.
