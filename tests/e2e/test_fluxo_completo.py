"""Fluxo ponta a ponta contra o docker compose de pé.

docker compose up -d --build --wait
pytest -m e2e
"""

import os
import time

import httpx
import pytest

pytestmark = pytest.mark.e2e

BASE_URL = os.getenv("E2E_BASE_URL", "http://localhost:8000")
TIMEOUT = float(os.getenv("E2E_TIMEOUT", "60"))


def _esperar(condicao, descricao: str):
    limite = time.monotonic() + TIMEOUT
    ultimo = None
    while time.monotonic() < limite:
        ultimo = condicao()
        if ultimo:
            return ultimo
        time.sleep(0.5)
    pytest.fail(f"Timeout esperando {descricao}; último valor: {ultimo!r}")


@pytest.fixture
def api():
    with httpx.Client(base_url=BASE_URL, timeout=10) as client:
        assert client.get("/health").json()["status"] == "ok"
        yield client


def test_pedido_percorre_rabbit_e_kafka_ate_auditoria(api):
    criado = api.post(
        "/pedidos", json={"cliente": "E2E", "produto": "Teclado", "quantidade": 1}
    ).json()
    pedido_id = criado["id"]

    # Outbox -> RabbitMQ -> worker de processamento
    def enviado():
        pedido = api.get(f"/pedidos/{pedido_id}").json()
        return pedido if pedido["status"] == "ENVIADO" else None

    pedido = _esperar(enviado, "pedido ENVIADO pelo worker")
    assert [h["status"] for h in pedido["historico"]] == ["PENDENTE", "PROCESSANDO", "ENVIADO"]

    assert api.patch(f"/pedidos/{pedido_id}/status", json={"status": "ENTREGUE"}).status_code == 200

    # Outbox -> Kafka -> consumer de auditoria
    def auditoria_completa():
        eventos = api.get(f"/pedidos/{pedido_id}/eventos").json()
        return eventos if len(eventos) == 4 else None

    eventos = _esperar(auditoria_completa, "4 eventos na auditoria")
    assert eventos[0]["tipo"] == "PedidoCriado"
    assert [e["dados"]["para"] for e in eventos[1:]] == ["PROCESSANDO", "ENVIADO", "ENTREGUE"]


def test_cancelamento_interrompe_processamento(api):
    criado = api.post(
        "/pedidos", json={"cliente": "E2E", "produto": "Monitor", "quantidade": 1}
    ).json()
    pedido_id = criado["id"]

    resposta = api.patch(f"/pedidos/{pedido_id}/status", json={"status": "CANCELADO"})
    if resposta.status_code == 409:
        pytest.skip("Worker já despachou o pedido antes do cancelamento")
    assert resposta.status_code == 200

    time.sleep(5)
    pedido = api.get(f"/pedidos/{pedido_id}").json()
    assert pedido["status"] == "CANCELADO"
    assert "ENVIADO" not in [h["status"] for h in pedido["historico"]]
