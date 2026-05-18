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
  *(Debería ver 3 contenedores de master (`kudu-master-1`, `2`, `3`) y 5 contenedores de tablet servers (`kudu-tserver-1` al `5`) con el estatus `Up`)*.

* **2. Interfaz Web (Consola Gráfica de Kudu Master):**
  Dirijase a estas direcciones 
  * **Master 1**: `http://localhost:8051`
  * **Master 2**: `http://localhost:8151`
  * **Master 3**: `http://localhost:8251`

  En esta interfaz web podrás ver:
  - Las tablas y esquemas de base de datos creados.
  - La lista de Tablet Servers conectados y sus IPs.
  - Las variables de configuración y el estado de salud general.

* **3. Verificación de salud interna (Kudu CLI `ksck`):**
  ```bash
  docker exec -it $(docker ps -aqf "name=kudu-master-1") kudu cluster ksck kudu-master-1:7051,kudu-master-2:7151,kudu-master-3:7251
  ```
  Si el cluster está totalmente operativo, al final del reporte verás un mensaje de aprobación que dice **`OK`**.

---
