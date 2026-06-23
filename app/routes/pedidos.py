import uuid

from fastapi import APIRouter, Depends, status

from app.db import pedidos_collection
from app.dependencies import get_kafka, get_rabbitmq
from app.messaging.kafka import KafkaPublisher
from app.messaging.rabbitmq import RabbitMQPublisher
from app.models import Pedido, PedidoCreate, StatusPedido

router = APIRouter(prefix="/pedidos", tags=["pedidos"])


@router.post("", response_model=Pedido, status_code=status.HTTP_201_CREATED)
async def criar_pedido(
    payload: PedidoCreate,
    rabbit: RabbitMQPublisher = Depends(get_rabbitmq),
    kafka: KafkaPublisher = Depends(get_kafka),
) -> Pedido:
    pedido = Pedido(
        id=str(uuid.uuid4()),
        cliente=payload.cliente,
        produto=payload.produto,
        quantidade=payload.quantidade,
        status=StatusPedido.PENDENTE,
    )
    await pedidos_collection().insert_one(pedido.model_dump(mode="json"))

    evento_rabbit = {"id": pedido.id, "status": pedido.status}
    evento_kafka = {
        "id": pedido.id,
        "cliente": pedido.cliente,
        "produto": pedido.produto,
        "quantidade": pedido.quantidade,
        "status": pedido.status,
    }
    await rabbit.publish(evento_rabbit)
    await kafka.publish(evento_kafka)
    return pedido


@router.get("", response_model=list[Pedido])
async def listar_pedidos() -> list[Pedido]:
    cursor = pedidos_collection().find({}, {"_id": 0})
    return [Pedido(**doc) async for doc in cursor]
