from fastapi import FastAPI, HTTPException, status

from schemas import Order
from kafka_producer import publish_order


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