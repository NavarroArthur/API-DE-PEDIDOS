"""Regras de negócio dos pedidos.

Toda mudança de estado grava o pedido e o evento correspondente no mesmo
documento (array `outbox`). Como o MongoDB garante atomicidade por documento,
não existe janela em que o pedido muda e o evento se perde. O relay em
`app.outbox` publica esses eventos depois.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

from app.db import pedidos_collection
from app.models import Pedido, PedidoCreate, StatusPedido, TipoEvento

TRANSICOES: dict[StatusPedido, set[StatusPedido]] = {
    StatusPedido.PENDENTE: {StatusPedido.PROCESSANDO, StatusPedido.CANCELADO},
    StatusPedido.PROCESSANDO: {StatusPedido.ENVIADO, StatusPedido.CANCELADO},
    StatusPedido.ENVIADO: {StatusPedido.ENTREGUE},
    StatusPedido.ENTREGUE: set(),
    StatusPedido.CANCELADO: set(),
}

PROJECAO_PUBLICA = {"_id": 0, "outbox": 0}


class PedidoNaoEncontrado(Exception):
    pass


class TransicaoInvalida(Exception):
    def __init__(self, atual: StatusPedido, novo: StatusPedido) -> None:
        super().__init__(f"Transição inválida: {atual.value} -> {novo.value}")
        self.atual = atual
        self.novo = novo


class ConflitoConcorrencia(Exception):
    pass


def _agora() -> datetime:
    # BSON guarda milissegundos; truncar aqui faz o POST devolver o mesmo valor do GET.
    agora = datetime.now(UTC)
    return agora.replace(microsecond=agora.microsecond // 1000 * 1000)


def _evento(tipo: TipoEvento, pedido_id: str, em: datetime, dados: dict[str, Any]) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "tipo": tipo.value,
        "pedido_id": pedido_id,
        "ocorrido_em": em.isoformat(),
        "dados": dados,
    }


async def criar_pedido(payload: PedidoCreate) -> Pedido:
    agora = _agora()
    pedido = Pedido(
        id=str(uuid.uuid4()),
        cliente=payload.cliente,
        produto=payload.produto,
        quantidade=payload.quantidade,
        status=StatusPedido.PENDENTE,
        criado_em=agora,
        atualizado_em=agora,
        historico=[{"status": StatusPedido.PENDENTE, "em": agora}],
    )
    doc = pedido.model_dump()
    doc["outbox"] = [
        _evento(
            TipoEvento.PEDIDO_CRIADO,
            pedido.id,
            agora,
            {
                "cliente": pedido.cliente,
                "produto": pedido.produto,
                "quantidade": pedido.quantidade,
                "status": pedido.status,
            },
        )
    ]
    await pedidos_collection().insert_one(doc)
    return pedido


async def obter_pedido(pedido_id: str) -> Pedido:
    doc = await pedidos_collection().find_one({"id": pedido_id}, PROJECAO_PUBLICA)
    if doc is None:
        raise PedidoNaoEncontrado(pedido_id)
    return Pedido(**doc)


async def listar_pedidos(
    status: StatusPedido | None = None,
    cliente: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[Pedido], int]:
    filtro: dict[str, Any] = {}
    if status is not None:
        filtro["status"] = status.value
    if cliente:
        filtro["cliente"] = cliente

    collection = pedidos_collection()
    total = await collection.count_documents(filtro)
    cursor = (
        collection.find(filtro, PROJECAO_PUBLICA)
        .sort([("criado_em", -1), ("_id", -1)])
        .skip(offset)
        .limit(limit)
    )
    return [Pedido(**doc) async for doc in cursor], total


async def alterar_status(
    pedido_id: str,
    novo: StatusPedido,
    esperado: StatusPedido | None = None,
) -> Pedido:
    """Muda o status validando a máquina de estados.

    `esperado` serve para quem só quer agir a partir de um estado conhecido
    (o worker). O update é condicional ao status lido, então duas mudanças
    concorrentes nunca se sobrescrevem: a segunda recebe ConflitoConcorrencia.
    """
    atual_pedido = await obter_pedido(pedido_id)
    atual = StatusPedido(atual_pedido.status)

    if esperado is not None and atual != esperado:
        raise ConflitoConcorrencia(f"Pedido {pedido_id} está {atual.value}")
    if novo not in TRANSICOES[atual]:
        raise TransicaoInvalida(atual, novo)

    agora = _agora()
    evento = _evento(
        TipoEvento.STATUS_ALTERADO,
        pedido_id,
        agora,
        {"de": atual.value, "para": novo.value},
    )
    resultado = await pedidos_collection().update_one(
        {"id": pedido_id, "status": atual.value},
        {
            "$set": {"status": novo.value, "atualizado_em": agora},
            "$push": {
                "historico": {"status": novo.value, "em": agora},
                "outbox": evento,
            },
        },
    )
    if resultado.matched_count == 0:
        raise ConflitoConcorrencia(f"Pedido {pedido_id} mudou durante a atualização")
    return await obter_pedido(pedido_id)
