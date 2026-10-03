"""Worker de processamento: consome `pedidos.processar` no RabbitMQ e leva o
pedido de PENDENTE a ENVIADO, simulando separação e despacho.

Rodar: python -m app.workers.processamento
"""

import asyncio
import logging
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient

from app.config import settings
from app.db import close_db, init_db
from app.messaging.rabbitmq import RabbitMQClient
from app.models import StatusPedido
from app.services import (
    ConflitoConcorrencia,
    PedidoNaoEncontrado,
    alterar_status,
    obter_pedido,
)

logger = logging.getLogger(__name__)

PROXIMO_PASSO = {
    StatusPedido.PENDENTE: StatusPedido.PROCESSANDO,
    StatusPedido.PROCESSANDO: StatusPedido.ENVIADO,
}


async def processar_pedido(mensagem: dict[str, Any], delay: float | None = None) -> None:
    """Avança o pedido a partir do estado em que ele está.

    Se a mensagem for reentregue no meio do caminho (ex.: worker caiu já com o
    pedido em PROCESSANDO), continua de onde parou. Pedido cancelado ou já
    enviado é ignorado.
    """
    pedido_id = mensagem["pedido_id"]
    delay = settings.processamento_delay if delay is None else delay

    while True:
        try:
            atual = StatusPedido((await obter_pedido(pedido_id)).status)
        except PedidoNaoEncontrado:
            logger.warning("Pedido %s não existe; descartando", pedido_id)
            return

        proximo = PROXIMO_PASSO.get(atual)
        if proximo is None:
            logger.info("Pedido %s em %s; nada a fazer", pedido_id, atual.value)
            return

        await asyncio.sleep(delay)
        try:
            await alterar_status(pedido_id, proximo, esperado=atual)
            logger.info("Pedido %s: %s -> %s", pedido_id, atual.value, proximo.value)
        except ConflitoConcorrencia:
            # Alguém (ex.: cancelamento) mudou o pedido no meio; relê e decide.
            continue


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    init_db(AsyncIOMotorClient(settings.mongo_uri, tz_aware=True))
    rabbit = RabbitMQClient()
    await rabbit.connect()
    try:
        await rabbit.consume(processar_pedido)
    finally:
        await rabbit.close()
        close_db()


if __name__ == "__main__":
    asyncio.run(main())
