import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import settings

logger = logging.getLogger(__name__)

C = TypeVar("C", AIOKafkaProducer, AIOKafkaConsumer)


async def _start_with_retry(factory: Callable[[], C], nome: str) -> C:
    # Um cliente aiokafka parado não pode ser reiniciado: cada tentativa cria um novo.
    last_exc: Exception | None = None
    for attempt in range(1, settings.connect_retries + 1):
        client = factory()
        try:
            await client.start()
            logger.info("Kafka %s connected (attempt %d)", nome, attempt)
            return client
        except KafkaConnectionError as exc:
            last_exc = exc
            logger.warning(
                "Kafka %s connect failed (%d/%d): %s",
                nome,
                attempt,
                settings.connect_retries,
                exc,
            )
            await client.stop()
            await asyncio.sleep(settings.connect_retry_delay)
    raise RuntimeError(f"Kafka unreachable: {last_exc}")


class KafkaPublisher:
    """Publica eventos de domínio. A chave é o id do pedido, então todos os
    eventos de um mesmo pedido caem na mesma partição e mantêm a ordem."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    @property
    def connected(self) -> bool:
        return self._producer is not None

    async def connect(self) -> None:
        self._producer = await _start_with_retry(
            lambda: AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap,
                key_serializer=lambda k: k.encode(),
                value_serializer=lambda v: json.dumps(v).encode(),
                enable_idempotence=True,
            ),
            "producer",
        )

    async def publish(self, payload: dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka not connected")
        await self._producer.send_and_wait(settings.kafka_topic, payload, key=payload["pedido_id"])
        logger.info("Kafka published %s pedido=%s", payload.get("tipo"), payload["pedido_id"])

    async def close(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None


async def consumir(
    group_id: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
) -> None:
    """Loop de consumo com commit manual depois do handler (at-least-once)."""
    consumer = await _start_with_retry(
        lambda: AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v),
        ),
        f"consumer[{group_id}]",
    )
    try:
        async for msg in consumer:
            await handler(msg.value)
            await consumer.commit()
    finally:
        await consumer.stop()
