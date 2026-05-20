#!/bin/bash
# Matar procesos previos
pkill -f uvicorn 2>/dev/null
pkill -f "02_ignite_fraud" 2>/dev/null
pkill -f "03_kudu_drain" 2>/dev/null
pkill -f "06_analytics_refresh" 2>/dev/null
pkill -f "05_simulator" 2>/dev/null

# Setup WSL hosts (solo si no existe)
if ! grep -q "kudu-tserver-1" /etc/hosts; then
    echo "127.0.0.1 kudu-master-1 kudu-tserver-1 kudu-tserver-2" | sudo tee -a /etc/hosts
fi

# Limpiar checkpoints corruptos de Spark
rm -rf /tmp/kudu_drain_checkpoint

export PYTHONUNBUFFERED=1

# 1. IP temporal para levantar Docker
export KUDU_QUICKSTART_IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}')
export KUDU_QUICKSTART_IP=${KUDU_QUICKSTART_IP:-127.0.0.1}
docker compose up -d

# 2. Esperar y leer IP real del contenedor master
echo "Esperando 40 segundos para que Kudu arranque..."
sleep 40
export KUDU_QUICKSTART_IP=$(docker inspect investigacion-bd2-kudu-master-1-1 | grep '"IPAddress"' | tail -1 | awk -F'"' '{print $4}')
echo "IP Kudu Master: $KUDU_QUICKSTART_IP"

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