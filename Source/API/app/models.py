from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Product(BaseModel):
    product_id: UUID
    quantity: int
    price_per_unit: float
    weight_per_unit: float


class Destination(BaseModel):
    country: str
    city: str
    postal_code: str


class PriceRequest(BaseModel):
    user_id: UUID
    products: list[Product]
    destination: Destination
    promo_code: str


class PriceResponseDetails(BaseModel):
    products_price: float
    transportation_fee: float
    tax: float
    discount: float


class PriceResponse(BaseModel):
    final_price: float
    details: PriceResponseDetails
    message: str


class KafkaPriceRequest(BaseModel):
    request_id: UUID
    user_id: UUID
    products: list[Product]
    destination: Destination
    promo_code: str


class KafkaPriceResult(BaseModel):
    request_id: UUID
    final_price: float
    details: PriceResponseDetails
    message: str


class ApiPerformanceEvent(BaseModel):
    timestamp: datetime
    user_id: UUID
    request_id: UUID
    response_time_ms: float
    status: str
    final_price: float
    request_size: int