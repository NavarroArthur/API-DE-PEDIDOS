import asyncio
import json
import logging
from typing import Any

import aio_pika

from app.config import settings

logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None

    async def connect(self) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, settings.connect_retries + 1):
            try:
                self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
                self._channel = await self._connection.channel()
                await self._channel.declare_queue(settings.rabbitmq_queue, durable=True)
                logger.info("RabbitMQ connected (attempt %d)", attempt)
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "RabbitMQ connect failed (%d/%d): %s",
                    attempt, settings.connect_retries, exc,
                )
                await asyncio.sleep(settings.connect_retry_delay)
        raise RuntimeError(f"RabbitMQ unreachable: {last_exc}")

    async def publish(self, payload: dict[str, Any]) -> None:
        if self._channel is None:
            raise RuntimeError("RabbitMQ not connected")
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self._channel.default_exchange.publish(
            message, routing_key=settings.rabbitmq_queue
        )
        logger.info("RabbitMQ published id=%s", payload.get("id"))

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
