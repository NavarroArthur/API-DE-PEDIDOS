from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from app.config import settings

_client: AsyncIOMotorClient | None = None


def init_db(client: AsyncIOMotorClient) -> None:
    global _client
    _client = client


def close_db() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None


def _collection(name: str) -> AsyncIOMotorCollection:
    if _client is None:
        raise RuntimeError("Mongo client not initialized")
    return _client[settings.mongo_db][name]


def pedidos_collection() -> AsyncIOMotorCollection:
    return _collection("pedidos")


def eventos_collection() -> AsyncIOMotorCollection:
    return _collection("eventos")


async def ensure_indexes() -> None:
    pedidos = pedidos_collection()
    await pedidos.create_index("id", unique=True)
    await pedidos.create_index([("status", 1), ("criado_em", -1)])
    await pedidos.create_index("outbox.event_id")
    await eventos_collection().create_index([("pedido_id", 1), ("ocorrido_em", 1)])
