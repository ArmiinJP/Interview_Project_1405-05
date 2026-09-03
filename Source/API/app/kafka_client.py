import asyncio
import json
import logging

from aiokafka import (AIOKafkaProducer,AIOKafkaConsumer)

from app.config import settings
from app.models import KafkaPriceResult
from app.request_manager import RequestManager
from app.exception import KafkaRequestError


logger = logging.getLogger(__name__)


class KafkaClient:
    def __init__(self,request_manager: RequestManager):
        self.request_manager = request_manager
        self.producer = None
        self.consumer = None
        self.consumer_task = None


    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            client_id=settings.KAFKA_CLIENT_ID)
        await self.producer.start()

        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_RESULT_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            client_id=settings.KAFKA_CLIENT_ID
        )
        await self.consumer.start()

        self.consumer_task = asyncio.create_task(self.consume_results())


    async def send_request(self, message: dict):

        if self.producer is None:
            raise KafkaRequestError("Kafka producer is not initialized")

        try:
            await self.producer.send_and_wait(
                settings.KAFKA_REQUEST_TOPIC,
                value=json.dumps(message).encode("utf-8"),
                key=str(message["request_id"]).encode("utf-8"),
            )

        except Exception as exc:
            logger.exception("Kafka request send failed request_id=%s",message.get("request_id"))
            raise KafkaRequestError() from exc


    async def consume_results(self):
        try:
            async for msg in self.consumer:
                await self.handle_result(msg)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Kafka result consumer failed")


    async def handle_result(self,msg):

        try:
            data = json.loads(msg.value.decode("utf-8"))
            result = KafkaPriceResult(**data)
            resolved = (self.request_manager.resolve_request(result.request_id,result))

            if resolved:
                logger.info("Result resolved request_id=%s",result.request_id)
            else:
                logger.warning("Late result received request_id=%s",result.request_id)

        except Exception:
            logger.exception("Invalid kafka result message")


    async def stop(self):
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            await self.consumer.stop()

        if self.producer:
            await self.producer.stop()