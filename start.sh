#!/bin/bash

# 1. Definir IP para Kudu y desactivar buffering de Python
export KUDU_QUICKSTART_IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}')
export KUDU_QUICKSTART_IP=${KUDU_QUICKSTART_IP:-127.0.0.1}
export PYTHONUNBUFFERED=1

echo "IP Kudu detectada: $KUDU_QUICKSTART_IP"

# 2. Levantar infraestructura en Docker
docker compose up -d

# 3. Inicializar esquema de Kudu
.venv/bin/python setup_kudu_schema.py

# 4. Levantar todos los servicios en segundo plano redirigiendo a /dev/null
echo "Levantando API FastAPI..."
nohup .venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 > /dev/null 2>&1 &

echo "Levantando Motor de Fraude..."
nohup .venv/bin/python 02_ignite_fraud.py > /dev/null 2>&1 &

echo "Levantando Consumidor/Drenador Kudu..."
nohup .venv/bin/python 03_kudu_drain.py > /dev/null 2>&1 &

echo "Levantando Recalculador de Analíticas..."
nohup .venv/bin/python 06_analytics_refresh.py > /dev/null 2>&1 &

echo "Esperando 20 segundos para estabilizar el backend antes de iniciar la simulación..."
sleep 20

echo "Levantando Simulador de Transacciones..."
nohup .venv/bin/python -u 05_simulator.py --total 0 --rate 3 > /dev/null 2>&1 &

echo "Pipeline iniciado correctamente en segundo plano (simulador activo y sin logs)."
