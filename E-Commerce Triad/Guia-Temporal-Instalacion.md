# Guia Temporal de Instalacion

## 1. Apache Kudu (Quickstart Docker)

```bash
# 1. Configurar la IP local
export KUDU_QUICKSTART_IP=$(hostname -I | awk '{print $1}')

# 2. Levantar los contenedores
docker compose up -d
```

---

### 1.1. Entrar al contenedor
Para ingresar al shell del contenedor de Kudu
```bash
docker exec -it $(docker ps -aqf "name=kudu-master-1") /bin/bash
```

### 1.2. Verificar estado

* **1. Verificar que los contenedores estén levantados y activos:**
  ```bash
  docker ps
  ```
  *(Debería ver 1 contenedor de master (`kudu-master-1`) y 2 contenedores de tablet servers (`kudu-tserver-1` y `kudu-tserver-2`) con el estatus `Up`)*.

* **2. Interfaz Web (Consola Gráfica de Kudu Master):**
  Diríjase a la dirección del Master:
  * **Master 1**: `http://localhost:8051`

  En esta interfaz web podrás ver:
  - Las tablas y esquemas de base de datos creados.
  - La lista de Tablet Servers conectados y sus IPs.
  - Las variables de configuración y el estado de salud general.

* **3. Verificación de salud interna (Kudu CLI `ksck`):**
  ```bash
  docker exec -it $(docker ps -aqf "name=kudu-master-1") kudu cluster ksck kudu-master-1:7051
  ```
  Si el cluster está totalmente operativo, al final del reporte verás un mensaje de aprobación que dice **`OK`**.

---

## 2. Apache Ignite (Caché In-Memory)

El contenedor de Apache Ignite se levanta automáticamente con el comando `docker compose up -d` y expone el puerto `10800`.

### 2.1. Instalar el cliente de Python (Thin Client)

Para interactuar con Ignite desde Python se debe de usar/instalar el cliente de Python, pyignite, es un cliente thin que hace que se haga la interaccion por medio de protocolos binarios.
```bash
# Asegúrate de tener tu entorno virtual activo
pip install pyignite
```
