from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Any

from pydantic import AfterValidator, BaseModel, ConfigDict, Field


def _utc(valor: datetime) -> datetime:
    # Driver sem tz_aware (ou mongomock) devolve datetime ingênuo, que já é UTC.
    return valor.replace(tzinfo=UTC) if valor.tzinfo is None else valor.astimezone(UTC)


UtcDatetime = Annotated[datetime, AfterValidator(_utc)]


class StatusPedido(str, Enum):
    PENDENTE = "PENDENTE"
    PROCESSANDO = "PROCESSANDO"
    ENVIADO = "ENVIADO"
    ENTREGUE = "ENTREGUE"
    CANCELADO = "CANCELADO"


class TipoEvento(str, Enum):
    PEDIDO_CRIADO = "PedidoCriado"
    STATUS_ALTERADO = "PedidoStatusAlterado"


class PedidoCreate(BaseModel):
    cliente: str = Field(min_length=1, max_length=200)
    produto: str = Field(min_length=1, max_length=200)
    quantidade: int = Field(gt=0)


class StatusUpdate(BaseModel):
    status: StatusPedido


class HistoricoStatus(BaseModel):
    status: StatusPedido
    em: UtcDatetime


class Pedido(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    id: str
    cliente: str
    produto: str
    quantidade: int
    status: StatusPedido
    criado_em: UtcDatetime
    atualizado_em: UtcDatetime
    historico: list[HistoricoStatus] = []


class PaginaPedidos(BaseModel):
    items: list[Pedido]
    total: int
    limit: int
    offset: int


class Evento(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    event_id: str
    tipo: TipoEvento
    pedido_id: str
    ocorrido_em: datetime
    dados: dict[str, Any]
