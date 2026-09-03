import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.exception import (KafkaRequestError,PriceTimeoutError)
from app.kafka_client import KafkaClient
from app.monitoring import MonitoringProducer
from app.request_manager import RequestManager
from app.routes import create_router


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s "
        "%(message)s"
    ),
)


logger = logging.getLogger(__name__)


app = FastAPI(title="Price Calculation API",version="1.0.0")

request_manager = RequestManager()

kafka_client = KafkaClient(request_manager=request_manager)

monitoring = MonitoringProducer()



@app.exception_handler(PriceTimeoutError)
async def price_timeout_handler(request,exc):
    return JSONResponse(
        status_code=504,
        content={
            "message": "Price calculation timeout"
        },
    )


@app.exception_handler(KafkaRequestError)
async def kafka_request_error_handler(request,exc):
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal Kafka error"
        },
    )


@app.on_event("startup")
async def startup_event():
    logger.info(
        "API startup started"
    )

    await kafka_client.start()
    logger.info("Kafka client started")

    await monitoring.start()
    logger.info("Monitoring producer started")

    logger.info("API startup completed")


@app.on_event("shutdown")
async def shutdown_event():

    logger.info("API shutdown started")
    
    await kafka_client.stop()
    logger.info("Kafka client stopped")

    await monitoring.stop()
    logger.info("Monitoring producer stopped")

    logger.info("API shutdown completed")


app.include_router(
    create_router(
        request_manager=request_manager,
        kafka_client=kafka_client,
        monitoring=monitoring,
    )
)