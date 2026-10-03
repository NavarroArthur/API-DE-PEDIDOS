from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from app.db import eventos_collection
from app.models import (
    Evento,
    PaginaPedidos,
    Pedido,
    PedidoCreate,
    StatusPedido,
    StatusUpdate,
)
from app.services import (
    ConflitoConcorrencia,
    PedidoNaoEncontrado,
    TransicaoInvalida,
    alterar_status,
    criar_pedido,
    listar_pedidos,
    obter_pedido,
)

router = APIRouter(prefix="/pedidos", tags=["pedidos"])


def _nao_encontrado(pedido_id: str) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, f"Pedido {pedido_id} não encontrado")


@router.post("", response_model=Pedido, status_code=status.HTTP_201_CREATED)
async def cadastrar(payload: PedidoCreate) -> Pedido:
    return await criar_pedido(payload)


@router.get("", response_model=PaginaPedidos)
async def listar(
    status_: Annotated[StatusPedido | None, Query(alias="status")] = None,
    cliente: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PaginaPedidos:
    items, total = await listar_pedidos(status_, cliente, limit, offset)
    return PaginaPedidos(items=items, total=total, limit=limit, offset=offset)


@router.get("/{pedido_id}", response_model=Pedido)
async def detalhar(pedido_id: str) -> Pedido:
    try:
        return await obter_pedido(pedido_id)
    except PedidoNaoEncontrado:
        raise _nao_encontrado(pedido_id) from None


@router.patch("/{pedido_id}/status", response_model=Pedido)
async def atualizar_status(pedido_id: str, payload: StatusUpdate) -> Pedido:
    try:
        return await alterar_status(pedido_id, payload.status)
    except PedidoNaoEncontrado:
        raise _nao_encontrado(pedido_id) from None
    except (TransicaoInvalida, ConflitoConcorrencia) as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from None


@router.get("/{pedido_id}/eventos", response_model=list[Evento])
async def eventos(pedido_id: str) -> list[Evento]:
    """Trilha de auditoria montada pelo consumer Kafka (eventual: pode
    demorar alguns instantes para refletir a última mudança)."""
    try:
        await obter_pedido(pedido_id)
    except PedidoNaoEncontrado:
        raise _nao_encontrado(pedido_id) from None
    cursor = eventos_collection().find({"pedido_id": pedido_id}, {"_id": 0}).sort("ocorrido_em", 1)
    return [Evento(**doc) async for doc in cursor]
