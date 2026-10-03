"""Consumer de auditoria: lê `pedidos.eventos` no Kafka e grava cada evento
na coleção `eventos`, que alimenta GET /pedidos/{id}/eventos.

Idempotente: o `event_id` vira o `_id`, então reentrega não duplica.

Rodar: python -m app.workers.auditoria
"""

import asyncio
import logging
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.db import close_db, eventos_collection, init_db
from app.messaging.kafka import consumir

logger = logging.getLogger(__name__)


async def registrar_evento(evento: dict[str, Any]) -> None:
    resultado = await eventos_collection().update_one(
        {"_id": evento["event_id"]},
        {"$setOnInsert": evento},
        upsert=True,
    )
    if resultado.upserted_id is None:
        logger.info("Evento %s já registrado; ignorando", evento["event_id"])
    else:
        logger.info("Evento %s %s registrado", evento["tipo"], evento["event_id"])


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    init_db(AsyncIOMotorClient(settings.mongo_uri, tz_aware=True))
    try:
        await consumir(settings.kafka_group_auditoria, registrar_evento)
    finally:
        close_db()


if __name__ == "__main__":
    asyncio.run(main())
