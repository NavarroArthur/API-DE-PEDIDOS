import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.db import close_db, ensure_indexes, init_db, pedidos_collection
from app.dependencies import kafka_publisher, rabbit_client
from app.outbox import rodar_relay
from app.routes.pedidos import router as pedidos_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(AsyncIOMotorClient(settings.mongo_uri, tz_aware=True))
    await ensure_indexes()
    await rabbit_client.connect()
    await kafka_publisher.connect()
    relay = asyncio.create_task(rodar_relay(rabbit_client, kafka_publisher))
    yield
    relay.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await relay
    await rabbit_client.close()
    await kafka_publisher.close()
    close_db()


app = FastAPI(title="Pedidos API", version="2.0.0", lifespan=lifespan)
app.include_router(pedidos_router)


@app.get("/health")
async def health(response: Response) -> dict[str, str]:
    checks: dict[str, str] = {}
    try:
        await pedidos_collection().database.command("ping")
        checks["mongo"] = "ok"
    except Exception as exc:
        checks["mongo"] = f"erro: {exc}"
    checks["rabbitmq"] = "ok" if rabbit_client.connected else "desconectado"
    checks["kafka"] = "ok" if kafka_publisher.connected else "desconectado"

    ok = all(v == "ok" for v in checks.values())
    if not ok:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if ok else "degraded", **checks}
