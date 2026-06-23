from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class StatusPedido(str, Enum):
    PENDENTE = "PENDENTE"
    PROCESSANDO = "PROCESSANDO"
    ENVIADO = "ENVIADO"
    ENTREGUE = "ENTREGUE"
    CANCELADO = "CANCELADO"


class PedidoCreate(BaseModel):
    cliente: str = Field(min_length=1, max_length=200)
    produto: str = Field(min_length=1, max_length=200)
    quantidade: int = Field(gt=0)


class Pedido(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    id: str
    cliente: str
    produto: str
    quantidade: int
    status: StatusPedido
