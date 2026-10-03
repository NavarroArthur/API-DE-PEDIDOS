import pytest

NOVO = {"cliente": "Arthur Navarro", "produto": "Notebook", "quantidade": 2}


async def _criar(client, **override) -> dict:
    response = await client.post("/pedidos", json={**NOVO, **override})
    assert response.status_code == 201
    return response.json()


async def test_cadastrar_pedido_retorna_201_e_status_pendente(client):
    body = await _criar(client)

    assert body["id"]
    assert body["cliente"] == NOVO["cliente"]
    assert body["produto"] == NOVO["produto"]
    assert body["quantidade"] == NOVO["quantidade"]
    assert body["status"] == "PENDENTE"
    assert body["criado_em"] == body["atualizado_em"]
    assert [h["status"] for h in body["historico"]] == ["PENDENTE"]


async def test_resposta_nao_expoe_outbox(client):
    criado = await _criar(client)
    detalhe = (await client.get(f"/pedidos/{criado['id']}")).json()
    listagem = (await client.get("/pedidos")).json()

    assert "outbox" not in criado
    assert "outbox" not in detalhe
    assert "outbox" not in listagem["items"][0]


async def test_listar_pedidos_retorna_todos_persistidos(client):
    for cliente in ("A", "B", "C"):
        await _criar(client, cliente=cliente)

    body = (await client.get("/pedidos")).json()
    assert body["total"] == 3
    assert {i["cliente"] for i in body["items"]} == {"A", "B", "C"}


async def test_listar_pedidos_vazio(client):
    body = (await client.get("/pedidos")).json()
    assert body == {"items": [], "total": 0, "limit": 20, "offset": 0}


async def test_listar_ordena_mais_recente_primeiro(client):
    for cliente in ("primeiro", "segundo", "terceiro"):
        await _criar(client, cliente=cliente)

    items = (await client.get("/pedidos")).json()["items"]
    assert [i["cliente"] for i in items] == ["terceiro", "segundo", "primeiro"]


async def test_listar_paginado(client):
    for i in range(5):
        await _criar(client, cliente=f"c{i}")

    body = (await client.get("/pedidos", params={"limit": 2, "offset": 2})).json()
    assert body["total"] == 5
    assert body["limit"] == 2
    assert body["offset"] == 2
    assert [i["cliente"] for i in body["items"]] == ["c2", "c1"]


async def test_listar_filtra_por_status_e_cliente(client):
    a = await _criar(client, cliente="Ana")
    await _criar(client, cliente="Ana")
    await _criar(client, cliente="Bia")
    await client.patch(f"/pedidos/{a['id']}/status", json={"status": "CANCELADO"})

    cancelados = (await client.get("/pedidos", params={"status": "CANCELADO"})).json()
    assert [i["id"] for i in cancelados["items"]] == [a["id"]]

    da_ana = (await client.get("/pedidos", params={"cliente": "Ana"})).json()
    assert da_ana["total"] == 2

    pendentes_ana = (
        await client.get("/pedidos", params={"cliente": "Ana", "status": "PENDENTE"})
    ).json()
    assert pendentes_ana["total"] == 1


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 101}, {"offset": -1}])
async def test_listar_paginacao_invalida(client, params):
    response = await client.get("/pedidos", params=params)
    assert response.status_code == 422


async def test_listar_status_invalido(client):
    response = await client.get("/pedidos", params={"status": "PERDIDO"})
    assert response.status_code == 422


async def test_detalhar_pedido(client):
    criado = await _criar(client)
    response = await client.get(f"/pedidos/{criado['id']}")
    assert response.status_code == 200
    assert response.json() == criado


async def test_detalhar_inexistente_404(client):
    response = await client.get("/pedidos/nao-existe")
    assert response.status_code == 404


async def test_atualizar_status_valido(client):
    criado = await _criar(client)
    response = await client.patch(f"/pedidos/{criado['id']}/status", json={"status": "PROCESSANDO"})
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "PROCESSANDO"
    assert [h["status"] for h in body["historico"]] == ["PENDENTE", "PROCESSANDO"]
    assert body["atualizado_em"] >= body["criado_em"]


async def test_atualizar_status_transicao_invalida_409(client):
    criado = await _criar(client)
    response = await client.patch(f"/pedidos/{criado['id']}/status", json={"status": "ENTREGUE"})
    assert response.status_code == 409
    assert "PENDENTE -> ENTREGUE" in response.json()["detail"]


async def test_cancelado_e_terminal(client):
    criado = await _criar(client)
    url = f"/pedidos/{criado['id']}/status"
    assert (await client.patch(url, json={"status": "CANCELADO"})).status_code == 200
    assert (await client.patch(url, json={"status": "PROCESSANDO"})).status_code == 409


async def test_atualizar_status_inexistente_404(client):
    response = await client.patch("/pedidos/nao-existe/status", json={"status": "CANCELADO"})
    assert response.status_code == 404


async def test_atualizar_status_desconhecido_422(client):
    criado = await _criar(client)
    response = await client.patch(f"/pedidos/{criado['id']}/status", json={"status": "PERDIDO"})
    assert response.status_code == 422


async def test_eventos_inexistente_404(client):
    response = await client.get("/pedidos/nao-existe/eventos")
    assert response.status_code == 404


@pytest.mark.parametrize(
    "payload",
    [
        {"cliente": "A", "produto": "X", "quantidade": 0},
        {"cliente": "A", "produto": "X", "quantidade": -3},
        {"cliente": "A"},
        {"cliente": "", "produto": "X", "quantidade": 1},
        {"cliente": "A", "produto": "X" * 201, "quantidade": 1},
    ],
    ids=[
        "quantidade-zero",
        "quantidade-negativa",
        "faltando-campos",
        "cliente-vazio",
        "produto-longo",
    ],
)
async def test_validacao_cadastro(client, payload):
    response = await client.post("/pedidos", json=payload)
    assert response.status_code == 422
