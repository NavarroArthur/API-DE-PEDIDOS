import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import settings

logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def connect(self) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, settings.connect_retries + 1):
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap,
                value_serializer=lambda v: json.dumps(v).encode(),
                enable_idempotence=True,
            )
            try:
                await producer.start()
                self._producer = producer
                logger.info("Kafka connected (attempt %d)", attempt)
                return
            except KafkaConnectionError as exc:
                last_exc = exc
                logger.warning(
                    "Kafka connect failed (%d/%d): %s",
                    attempt, settings.connect_retries, exc,
                )
                await producer.stop()
                await asyncio.sleep(settings.connect_retry_delay)
        raise RuntimeError(f"Kafka unreachable: {last_exc}")

    async def publish(self, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka not connected")
        await self._producer.send_and_wait(settings.kafka_topic, payload)
        logger.info("Kafka published id=%s", payload.get("id"))

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
