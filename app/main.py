import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.db import close_db, init_db, pedidos_collection
from app.dependencies import kafka_publisher, rabbit_publisher
from app.routes.pedidos import router as pedidos_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = AsyncIOMotorClient(settings.mongo_uri)
    init_db(client)
    await rabbit_publisher.connect()
    await kafka_publisher.connect()
    yield
    await rabbit_publisher.close()
    await kafka_publisher.close()
    close_db()


app = FastAPI(title="Pedidos API", version="1.0.0", lifespan=lifespan)
app.include_router(pedidos_router)


@app.get("/health")
async def health() -> dict[str, str]:
    try:
        await pedidos_collection().database.command("ping")
        return {"status": "ok"}
    except Exception as exc:
        return {"status": "degraded", "detail": str(exc)}
