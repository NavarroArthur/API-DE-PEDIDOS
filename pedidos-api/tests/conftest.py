from typing import Any

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from mongomock_motor import AsyncMongoMockClient

from app import dependencies
from app.db import close_db, init_db
from app.main import app


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def publish(self, payload: dict[str, Any]) -> None:
        self.messages.append(payload)


@pytest_asyncio.fixture
async def client():
    fake_rabbit = FakePublisher()
    fake_kafka = FakePublisher()

    app.dependency_overrides[dependencies.get_rabbitmq] = lambda: fake_rabbit
    app.dependency_overrides[dependencies.get_kafka] = lambda: fake_kafka

    init_db(AsyncMongoMockClient())
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        ac.fake_rabbit = fake_rabbit  # type: ignore[attr-defined]
        ac.fake_kafka = fake_kafka  # type: ignore[attr-defined]
        yield ac

    close_db()
    app.dependency_overrides.clear()
