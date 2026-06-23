import pytest


@pytest.mark.asyncio
async def test_cadastrar_pedido_retorna_201_e_status_pendente(client):
    payload = {"cliente": "Arthur Navarro", "produto": "Notebook", "quantidade": 2}
    response = await client.post("/pedidos", json=payload)

    assert response.status_code == 201
    body = response.json()
    assert body["id"]
    assert body["cliente"] == payload["cliente"]
    assert body["produto"] == payload["produto"]
    assert body["quantidade"] == payload["quantidade"]
    assert body["status"] == "PENDENTE"


@pytest.mark.asyncio
async def test_cadastrar_pedido_publica_em_rabbit_e_kafka(client):
    response = await client.post(
        "/pedidos",
        json={"cliente": "Cliente X", "produto": "Mouse", "quantidade": 1},
    )
    pedido_id = response.json()["id"]

    assert len(client.fake_rabbit.messages) == 1
    assert client.fake_rabbit.messages[0]["id"] == pedido_id
    assert client.fake_rabbit.messages[0]["status"] == "PENDENTE"

    assert len(client.fake_kafka.messages) == 1
    kafka_msg = client.fake_kafka.messages[0]
    assert kafka_msg["id"] == pedido_id
    assert kafka_msg["cliente"] == "Cliente X"
    assert kafka_msg["produto"] == "Mouse"
    assert kafka_msg["quantidade"] == 1


@pytest.mark.asyncio
async def test_listar_pedidos_retorna_todos_persistidos(client):
    pedidos = [
        {"cliente": "A", "produto": "X", "quantidade": 1},
        {"cliente": "B", "produto": "Y", "quantidade": 3},
        {"cliente": "C", "produto": "Z", "quantidade": 5},
    ]
    for p in pedidos:
        r = await client.post("/pedidos", json=p)
        assert r.status_code == 201

    response = await client.get("/pedidos")
    assert response.status_code == 200
    items = response.json()
    assert len(items) == 3
    assert {i["cliente"] for i in items} == {"A", "B", "C"}
    assert all(i["status"] == "PENDENTE" for i in items)


@pytest.mark.asyncio
async def test_listar_pedidos_vazio_retorna_lista_vazia(client):
    response = await client.get("/pedidos")
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_quantidade_zero_rejeitada(client):
    response = await client.post(
        "/pedidos",
        json={"cliente": "A", "produto": "X", "quantidade": 0},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_quantidade_negativa_rejeitada(client):
    response = await client.post(
        "/pedidos",
        json={"cliente": "A", "produto": "X", "quantidade": -3},
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_campos_obrigatorios(client):
    response = await client.post("/pedidos", json={"cliente": "A"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_cliente_vazio_rejeitado(client):
    response = await client.post(
        "/pedidos",
        json={"cliente": "", "produto": "X", "quantidade": 1},
    )
    assert response.status_code == 422
