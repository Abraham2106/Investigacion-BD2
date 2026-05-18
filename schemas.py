from pydantic import BaseModel, Field

class Order(BaseModel):
    order_id: str = Field(..., example="ORD-1001")
    user_id: str = Field(..., example="USER-01")
    country_code: str = Field(..., min_length=2, max_length=2, example="CR")
    product: str = Field(..., example="Laptop")
    amount: float = Field(..., gt=0, example=1200.50)