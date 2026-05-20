#!/bin/bash
# Matar procesos previos
pkill -f uvicorn 2>/dev/null
pkill -f "02_ignite_fraud" 2>/dev/null
pkill -f "03_kudu_drain" 2>/dev/null
pkill -f "06_analytics_refresh" 2>/dev/null
pkill -f "05_simulator" 2>/dev/null

# Limpiar checkpoints corruptos de Spark
rm -rf /tmp/kudu_drain_checkpoint

export PYTHONUNBUFFERED=1

# 1. Reiniciar Docker Compose limpiando volumenes antiguos
echo "Reiniciando servicios Docker y limpiando volumenes de Kudu..."
docker compose down -v
docker compose up -d

# 2. Esperar de forma determinista que el Master y los 2 TServers esten listos
echo "Esperando a que Kudu Master y los 2 Tablet Servers esten listos..."
while ! curl -s http://127.0.0.1:8051/tablet-servers | grep -q "There are 2 registered tablet servers."; do
    sleep 2
done
echo "¡Kudu listo con 2 Tablet Servers registrados!"

# 3. Inicializar esquema
.venv/bin/python setup_kudu_schema.py

# 4. Levantar servicios con logs individuales
echo "Levantando API FastAPI..."
nohup .venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 > fastapi.log 2>&1 &
echo "Levantando Motor de Fraude..."
nohup .venv/bin/python 02_ignite_fraud.py > fraud.log 2>&1 &
echo "Levantando Consumidor/Drenador Kudu..."
nohup .venv/bin/python 03_kudu_drain.py > drain.log 2>&1 &
echo "Levantando Recalculador de Analiticas..."
nohup .venv/bin/python 06_analytics_refresh.py > analytics.log 2>&1 &
echo "Esperando 20 segundos para estabilizar el backend..."
sleep 20
echo "Levantando Simulador de Transacciones..."
nohup .venv/bin/python -u 05_simulator.py --total 0 --rate 3 > simulator.log 2>&1 &
echo "Pipeline iniciado correctamente. Los logs se estan guardando en archivos .log"