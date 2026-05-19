import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))) # Para que reconozca los otros archivos

from fastapi import FastAPI, HTTPException, status
from schemas import Order, RiskCountry
from kafka_producer import publish_order
from ignite_client import risk_countries_cache


app = FastAPI(
    title="Orders API"
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

        raise HTTPException(
            status_code=500,
            detail=f"Kafka error: {str(e)}"
        )

@app.post("/risk-countries", status_code=status.HTTP_200_OK)
def update_risk_country(risk: RiskCountry):
    """ 
    Actualiza el estado de un pais en la cache

    Args:
        risk (RiskCountry): Objeto RiskCountry con el codigo del pais y si es de riesgo

    Returns:
        dict: Diccionario con el estado de la operacion
    """
    try:
        risk_countries_cache.put(risk.country_code, risk.is_risk)
        return {
            "status": "updated",
            "country_code": risk.country_code,
            "is_risk": risk.is_risk
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ignite error: {str(e)}"
        )