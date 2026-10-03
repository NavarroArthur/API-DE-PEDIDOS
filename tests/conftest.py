from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

from app.db import close_db, init_db
from app.main import app


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def publish(self, payload: dict[str, Any]) -> None:
        self.messages.append(payload)


@pytest_asyncio.fixture
async def db():
    init_db(AsyncMongoMockClient())
    yield
    close_db()


@pytest_asyncio.fixture
async def client(db):
    # ASGITransport não roda o lifespan: sem conexão real com Rabbit/Kafka.
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def rabbit() -> FakePublisher:
    return FakePublisher()


@pytest.fixture
def kafka() -> FakePublisher:
    return FakePublisher()
