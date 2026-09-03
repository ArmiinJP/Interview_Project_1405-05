import logging

from aiokafka import AIOKafkaProducer

from app.config import settings
from app.models import ApiPerformanceEvent


logger = logging.getLogger(__name__)


class MonitoringProducer:
    def __init__(self):
        self.producer = None


    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=settings.KAFKA_MONITORING_CLIENT_ID,
        )
        await self.producer.start()


    async def send(self,event: ApiPerformanceEvent):
        if self.producer is None:
            logger.warning("Monitoring producer is not initialized")
            return

        try:
            await self.producer.send_and_wait(
                settings.KAFKA_MONITORING_TOPIC,
                value=event.model_dump_json().encode("utf-8")
            )

        except Exception:
            logger.exception("Failed to send monitoring event")


    async def stop(self):
        if self.producer:
            await self.producer.stop()