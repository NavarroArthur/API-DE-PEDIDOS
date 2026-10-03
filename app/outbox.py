"""Relay do transactional outbox.

Lê eventos pendentes do array `outbox` dos pedidos, publica e só então
remove do array. Se o processo cair entre publicar e remover, o evento é
publicado de novo no próximo ciclo: entrega at-least-once. Os consumidores
são idempotentes pelo `event_id`.
"""

import asyncio
import logging
from typing import Any, Protocol

from app.config import settings
from app.db import pedidos_collection
from app.models import TipoEvento

logger = logging.getLogger(__name__)


class Publisher(Protocol):
    async def publish(self, payload: dict[str, Any]) -> None: ...


async def publicar_pendentes(rabbit: Publisher, kafka: Publisher, lote: int = 100) -> int:
    collection = pedidos_collection()
    publicados = 0
    cursor = collection.find(
        {"outbox.0": {"$exists": True}}, {"_id": 0, "id": 1, "outbox": 1}
    ).limit(lote)
    async for doc in cursor:
        for evento in doc["outbox"]:
            await kafka.publish(evento)
            if evento["tipo"] == TipoEvento.PEDIDO_CRIADO.value:
                await rabbit.publish(
                    {"event_id": evento["event_id"], "pedido_id": evento["pedido_id"]}
                )
            await collection.update_one(
                {"id": doc["id"]},
                {"$pull": {"outbox": {"event_id": evento["event_id"]}}},
            )
            publicados += 1
    return publicados


async def rodar_relay(rabbit: Publisher, kafka: Publisher) -> None:
    while True:
        try:
            await publicar_pendentes(rabbit, kafka)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Relay do outbox falhou; tentando de novo")
        await asyncio.sleep(settings.outbox_poll_interval)
