# Requerimientos — E-Commerce Triad Pipeline

Sistema de E-Commerce basado en **FastAPI + Apache Kafka + Apache Ignite + Apache Kudu**, diseñado para ingerir, procesar y almacenar pedidos en tiempo real, detectando fraudes y filtrando zonas de riesgo.

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




