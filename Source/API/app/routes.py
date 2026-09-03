import asyncio
import logging
import time
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter

from app.config import settings
from app.kafka_client import KafkaClient
from app.models import KafkaPriceRequest, PriceRequest, PriceResponse, ApiPerformanceEvent
from app.request_manager import RequestManager
from app.monitoring import MonitoringProducer
from app.exception import KafkaRequestError, PriceTimeoutError


logger = logging.getLogger(__name__)


def create_router(request_manager: RequestManager,kafka_client: KafkaClient, monitoring: MonitoringProducer):

    router = APIRouter()

    @router.post(
        "/calculate-price",
        response_model=PriceResponse
    )
    async def calculate_price(request: PriceRequest):
        start_time = time.perf_counter()
        
        request_size = len(request.model_dump_json().encode("utf-8"))
        request_id = uuid4()
        logger.info("Request received request_id=%s", request_id)

        future = request_manager.create_request(request_id)

        kafka_request = KafkaPriceRequest(
            request_id=request_id,
            user_id=request.user_id,
            products=request.products,
            destination=request.destination,
            promo_code=request.promo_code
        )

        try:
            await kafka_client.send_request(kafka_request.model_dump(mode="json"))
            logger.info("Kafka request sent request_id=%s", request_id)

        except KafkaRequestError:
            request_manager.remove_request(request_id)

            response_time_ms = (time.perf_counter()- start_time) * 1000

            await send_monitoring_event(
                monitoring=monitoring,
                request=request,
                request_id=request_id,
                response_time_ms=response_time_ms,
                request_size=request_size,
                status="ERROR",
                final_price=0.0,
            )

            logger.exception("Kafka request failed request_id=%s",request_id)
            raise

        try:
            result = await asyncio.wait_for(future,timeout=settings.REQUEST_TIMEOUT_SECONDS)
            logger.info("Result received request_id=%s", request_id)

        except asyncio.TimeoutError:
            request_manager.remove_request(request_id)

            response_time_ms = (time.perf_counter()- start_time) * 1000

            await send_monitoring_event(
                monitoring=monitoring,
                request=request,
                request_id=request_id,
                response_time_ms=response_time_ms,
                request_size=request_size,
                status="TIMEOUT",
                final_price=0.0,
            )            
            logger.warning("Request timeout request_id=%s",request_id)

            raise PriceTimeoutError()


        response = PriceResponse(
            final_price=result.final_price,
            details=result.details,
            message=result.message,
        )

        response_time_ms = (time.perf_counter()- start_time) * 1000

        await send_monitoring_event(
            monitoring=monitoring,
            request=request,
            request_id=request_id,
            response_time_ms=response_time_ms,
            request_size=request_size,
            status="SUCCESS",
            final_price=result.final_price,
        )

        logger.info("Request completed request_id=%s",request_id,)

        return response


    @router.get("/health")
    async def health():
        return {
            "status": "ok"
        }

    return router


async def send_monitoring_event(monitoring: MonitoringProducer,request: PriceRequest,request_id,response_time_ms,request_size: int,status: str,final_price: float):

    event = ApiPerformanceEvent(
        timestamp=datetime.utcnow(),
        user_id=request.user_id,
        request_id=request_id,
        response_time_ms=response_time_ms,
        status=status,
        final_price=final_price,
        request_size=request_size,
    )

    await monitoring.send(event)