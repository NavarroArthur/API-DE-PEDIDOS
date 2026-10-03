"""Outbox relay + workers, rodando contra mongomock e publishers fake."""

import pytest

from app.db import pedidos_collection
from app.models import PedidoCreate, StatusPedido
from app.outbox import publicar_pendentes
from app.services import (
    TRANSICOES,
    ConflitoConcorrencia,
    alterar_status,
    criar_pedido,
    obter_pedido,
)
from app.workers.auditoria import registrar_evento
from app.workers.processamento import processar_pedido

NOVO = PedidoCreate(cliente="Cliente X", produto="Mouse", quantidade=1)


async def _outbox(pedido_id: str) -> list[dict]:
    doc = await pedidos_collection().find_one({"id": pedido_id})
    return doc["outbox"]


async def test_criar_grava_evento_no_outbox(db):
    pedido = await criar_pedido(NOVO)

    outbox = await _outbox(pedido.id)
    assert len(outbox) == 1
    assert outbox[0]["tipo"] == "PedidoCriado"
    assert outbox[0]["pedido_id"] == pedido.id
    assert outbox[0]["dados"]["quantidade"] == 1


async def test_relay_publica_e_esvazia_outbox(db, rabbit, kafka):
    pedido = await criar_pedido(NOVO)

    publicados = await publicar_pendentes(rabbit, kafka)

    assert publicados == 1
    assert [m["tipo"] for m in kafka.messages] == ["PedidoCriado"]
    assert kafka.messages[0]["dados"]["cliente"] == "Cliente X"
    assert rabbit.messages == [{"event_id": kafka.messages[0]["event_id"], "pedido_id": pedido.id}]
    assert await _outbox(pedido.id) == []
    # Segunda passada não republica nada.
    assert await publicar_pendentes(rabbit, kafka) == 0


async def test_mudanca_de_status_vai_so_para_kafka(db, rabbit, kafka):
    pedido = await criar_pedido(NOVO)
    await publicar_pendentes(rabbit, kafka)

    await alterar_status(pedido.id, StatusPedido.CANCELADO)
    await publicar_pendentes(rabbit, kafka)

    assert [m["tipo"] for m in kafka.messages] == ["PedidoCriado", "PedidoStatusAlterado"]
    assert kafka.messages[1]["dados"] == {"de": "PENDENTE", "para": "CANCELADO"}
    assert len(rabbit.messages) == 1


async def test_relay_falha_mantem_evento_para_retentar(db, rabbit):
    class KafkaFora:
        async def publish(self, payload):
            raise ConnectionError("broker fora")

    pedido = await criar_pedido(NOVO)
    with pytest.raises(ConnectionError):
        await publicar_pendentes(rabbit, KafkaFora())

    assert len(await _outbox(pedido.id)) == 1
    assert rabbit.messages == []


async def test_alterar_status_com_esperado_divergente(db):
    pedido = await criar_pedido(NOVO)
    with pytest.raises(ConflitoConcorrencia):
        await alterar_status(pedido.id, StatusPedido.ENVIADO, esperado=StatusPedido.PROCESSANDO)


def test_estados_terminais():
    assert TRANSICOES[StatusPedido.ENTREGUE] == set()
    assert TRANSICOES[StatusPedido.CANCELADO] == set()
    assert StatusPedido.CANCELADO not in TRANSICOES[StatusPedido.ENVIADO]


async def test_worker_leva_pedido_ate_enviado(db):
    pedido = await criar_pedido(NOVO)

    await processar_pedido({"pedido_id": pedido.id}, delay=0)

    final = await obter_pedido(pedido.id)
    assert final.status == "ENVIADO"
    assert [h.status for h in final.historico] == ["PENDENTE", "PROCESSANDO", "ENVIADO"]
    assert [e["dados"]["para"] for e in (await _outbox(pedido.id))[1:]] == [
        "PROCESSANDO",
        "ENVIADO",
    ]


async def test_worker_retoma_de_processando(db):
    pedido = await criar_pedido(NOVO)
    await alterar_status(pedido.id, StatusPedido.PROCESSANDO)

    await processar_pedido({"pedido_id": pedido.id}, delay=0)

    assert (await obter_pedido(pedido.id)).status == "ENVIADO"


async def test_worker_ignora_cancelado(db):
    pedido = await criar_pedido(NOVO)
    await alterar_status(pedido.id, StatusPedido.CANCELADO)

    await processar_pedido({"pedido_id": pedido.id}, delay=0)

    final = await obter_pedido(pedido.id)
    assert final.status == "CANCELADO"
    assert len(final.historico) == 2


async def test_worker_ignora_pedido_inexistente(db):
    await processar_pedido({"pedido_id": "nao-existe"}, delay=0)


async def test_auditoria_idempotente_e_exposta_na_api(client, rabbit, kafka):
    criado = (
        await client.post("/pedidos", json={"cliente": "A", "produto": "X", "quantidade": 1})
    ).json()
    await client.patch(f"/pedidos/{criado['id']}/status", json={"status": "PROCESSANDO"})
    await publicar_pendentes(rabbit, kafka)

    for evento in kafka.messages:
        await registrar_evento(evento)
    # Reentrega (at-least-once) não pode duplicar.
    for evento in kafka.messages:
        await registrar_evento(evento)

    eventos = (await client.get(f"/pedidos/{criado['id']}/eventos")).json()
    assert [e["tipo"] for e in eventos] == ["PedidoCriado", "PedidoStatusAlterado"]
    assert eventos[1]["dados"] == {"de": "PENDENTE", "para": "PROCESSANDO"}
