import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import aio_pika

from app.config import settings

logger = logging.getLogger(__name__)


class RabbitMQClient:
    """Publica comandos de processamento e, no worker, consome a fila.

    Topologia: fila durável `pedidos.processar` com dead-letter para
    `pedidos.processar.dlq`. Mensagem rejeitada sem requeue cai na DLQ.
    """

    def __init__(self) -> None:
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._queue: aio_pika.abc.AbstractQueue | None = None

    @property
    def connected(self) -> bool:
        return self._connection is not None and not self._connection.is_closed

    async def connect(self) -> None:
        last_exc: Exception | None = None
        for attempt in range(1, settings.connect_retries + 1):
            try:
                self._connection = await aio_pika.connect_robust(settings.rabbitmq_url)
                self._channel = await self._connection.channel()
                await self._declare_topology()
                logger.info("RabbitMQ connected (attempt %d)", attempt)
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "RabbitMQ connect failed (%d/%d): %s",
                    attempt,
                    settings.connect_retries,
                    exc,
                )
                await asyncio.sleep(settings.connect_retry_delay)
        raise RuntimeError(f"RabbitMQ unreachable: {last_exc}")

    async def _declare_topology(self) -> None:
        assert self._channel is not None
        dlx = await self._channel.declare_exchange(
            settings.rabbitmq_dlx, aio_pika.ExchangeType.DIRECT, durable=True
        )
        dlq = await self._channel.declare_queue(settings.rabbitmq_dlq, durable=True)
        await dlq.bind(dlx, routing_key=settings.rabbitmq_dlq)
        self._queue = await self._channel.declare_queue(
            settings.rabbitmq_queue,
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.rabbitmq_dlx,
                "x-dead-letter-routing-key": settings.rabbitmq_dlq,
            },
        )

    async def publish(self, payload: dict[str, Any]) -> None:
        if self._channel is None:
            raise RuntimeError("RabbitMQ not connected")
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=payload.get("event_id"),
        )
        await self._channel.default_exchange.publish(message, routing_key=settings.rabbitmq_queue)
        logger.info("RabbitMQ published pedido=%s", payload.get("pedido_id"))

    async def consume(
        self,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
        prefetch: int = 10,
    ) -> None:
        if self._channel is None or self._queue is None:
            raise RuntimeError("RabbitMQ not connected")
        await self._channel.set_qos(prefetch_count=prefetch)
        async with self._queue.iterator() as messages:
            async for message in messages:
                try:
                    await handler(json.loads(message.body))
                except Exception:
                    logger.exception("Falha ao processar; enviando para DLQ")
                    await message.reject(requeue=False)
                else:
                    await message.ack()

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None
            self._queue = None
