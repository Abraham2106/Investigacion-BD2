#!/bin/bash

echo "Deteniendo procesos del pipeline en segundo plano..."
pkill -f "uvicorn main:app" 2>/dev/null
pkill -f "02_ignite_fraud.py" 2>/dev/null
pkill -f "03_kudu_drain.py" 2>/dev/null
pkill -f "06_analytics_refresh.py" 2>/dev/null
pkill -f "05_simulator.py" 2>/dev/null

# Liberar el puerto 8000 a la fuerza por si queda colgado
if command -v fuser &> /dev/null; then
    fuser -k 8000/tcp &>/dev/null
elif command -v lsof &> /dev/null; then
    kill -9 $(lsof -t -i:8000) &>/dev/null
fi

# Definir IP para evitar errores de interpolación al bajar contenedores
export KUDU_QUICKSTART_IP=$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7}')
export KUDU_QUICKSTART_IP=${KUDU_QUICKSTART_IP:-127.0.0.1}

echo "Bajando contenedores de Docker..."
docker compose down

echo "Todo el pipeline ha sido detenido."
