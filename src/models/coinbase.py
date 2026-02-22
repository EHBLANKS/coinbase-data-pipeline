from decimal import Decimal
from pydantic import BaseModel


class Ticker(BaseModel):
    product_id: str
    price: Decimal
    best_bid: Decimal
    best_ask: Decimal
    best_bid_quantity: Decimal
    best_ask_quantity: Decimal

    model_config = {"extra": "allow"}
