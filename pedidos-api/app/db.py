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


def pedidos_collection() -> AsyncIOMotorCollection:
    if _client is None:
        raise RuntimeError("Mongo client not initialized")
    return _client[settings.mongo_db]["pedidos"]
