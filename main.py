import sys
import os
import json

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from schemas import Order, RiskCountry
from kafka_producer import publish_order
from ignite_client import risk_countries_cache, ignite_client

app = FastAPI(title="Orders API")

# CORS necesario para que el plugin Infinity de Grafana pueda leer desde el navegador
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=status.HTTP_202_ACCEPTED)
def create_order(order: Order):
    try:
        order_dict = order.model_dump()
        publish_order(order_dict)
        return {
            "status": "queued",
            "order_id": order.order_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")


@app.post("/risk-countries", status_code=status.HTTP_200_OK)
def update_risk_country(risk: RiskCountry):
    """Actualiza el estado de un pais en la cache."""
    try:
        risk_countries_cache.put(risk.country_code, risk.is_risk)
        return {
            "status": "updated",
            "country_code": risk.country_code,
            "is_risk": risk.is_risk
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ignite error: {str(e)}")


# ──────────────────────────────────────────────
# Endpoints de analítica — leen de la caché de Ignite
# Alimentados por 06_analytics_refresh.py cada 5s
# ──────────────────────────────────────────────

from pyignite import Client

def _read_cache(cache_name: str) -> list:
    try:
        client = Client()
        client.connect('localhost', 10800)
        cache = client.get_or_create_cache(cache_name)
        raw = cache.get("data")
        client.close()
        return json.loads(raw) if raw else []
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Ignite read error: {str(e)}")


@app.get("/analytics/by-country")
def analytics_by_country():
    return _read_cache("analytics_by_country")


@app.get("/analytics/by-status")
def analytics_by_status():
    return _read_cache("analytics_by_status")


@app.get("/analytics/by-product")
def analytics_by_product():
    return _read_cache("analytics_by_product")


@app.get("/analytics/recent-orders")
def analytics_recent_orders():
    return _read_cache("recent_orders")