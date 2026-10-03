# Pedidos API

![CI](https://github.com/NavarroArthur/API-DE-PEDIDOS/actions/workflows/ci.yml/badge.svg)

API de pedidos para e-commerce com arquitetura orientada a eventos: **FastAPI + MongoDB + RabbitMQ + Kafka**, tudo em Docker Compose.

O pedido é criado pela API, processado de forma assíncrona por um worker via RabbitMQ e cada mudança de estado vira um evento no Kafka, consumido por um serviço de auditoria. A publicação usa **transactional outbox** para não perder evento se um broker cair.

## Arquitetura

```mermaid
flowchart LR
    C[Cliente] -->|POST / PATCH| API[FastAPI]
    API -->|pedido + evento<br/>no mesmo documento| M[(MongoDB<br/>pedidos)]
    R[Relay do outbox<br/>dentro da API] -->|lê outbox| M
    R -->|PedidoCriado| Q[[RabbitMQ<br/>pedidos.processar]]
    R -->|todos os eventos<br/>key = pedido_id| K{{Kafka<br/>pedidos.eventos}}
    Q --> W[worker-processamento]
    W -->|PENDENTE → PROCESSANDO → ENVIADO| M
    Q -.->|falha| DLQ[[pedidos.processar.dlq]]
    K --> A[worker-auditoria]
    A -->|upsert por event_id| E[(MongoDB<br/>eventos)]
    API -->|GET /pedidos/id/eventos| E
```

| Peça | Papel |
|------|-------|
| **API** | Valida, aplica a máquina de estados e grava pedido + evento atomicamente |
| **Relay do outbox** | Task em background na API; publica eventos pendentes e remove do outbox |
| **RabbitMQ** | Fila de trabalho (comando "processe este pedido"), com dead-letter queue |
| **worker-processamento** | Consome a fila e avança o pedido até `ENVIADO` |
| **Kafka** (KRaft, sem Zookeeper) | Log de eventos de domínio, particionado por pedido |
| **worker-auditoria** | Consumer group que monta a trilha de auditoria |

### Decisões de projeto

- **Outbox no próprio documento.** Em vez de uma coleção `outbox` separada (que exigiria transação multi-documento e replica set), o evento vai num array `outbox` dentro do pedido. O MongoDB garante atomicidade por documento, então pedido e evento são gravados juntos ou nenhum dos dois.
- **At-least-once + consumidores idempotentes.** O relay só remove o evento depois de publicar; se cair no meio, republica. A auditoria usa `event_id` como `_id` (reentrega não duplica). O worker decide pelo estado atual do pedido, então reprocessar é seguro.
- **Concorrência otimista.** Mudança de status é um `update` condicionado ao status lido. Se o cliente cancela enquanto o worker processa, um dos dois perde e recebe conflito; o worker relê e respeita o cancelamento.
- **Ordem por pedido no Kafka.** A chave da mensagem é o `pedido_id`, então os eventos de um pedido ficam na mesma partição, em ordem.
- **RabbitMQ para trabalho, Kafka para fatos.** A fila distribui tarefas entre workers (ack/reject, DLQ); o tópico guarda o histórico que qualquer consumer group novo pode reler desde o início.

## Máquina de estados

```mermaid
stateDiagram-v2
    [*] --> PENDENTE
    PENDENTE --> PROCESSANDO: worker
    PENDENTE --> CANCELADO
    PROCESSANDO --> ENVIADO: worker
    PROCESSANDO --> CANCELADO
    ENVIADO --> ENTREGUE
    ENTREGUE --> [*]
    CANCELADO --> [*]
```

Transição fora do diagrama retorna `409 Conflict`.

## Executar

Pré-requisitos: Docker com Compose v2. Portas livres: `8000`, `27017`, `5672`, `15672`, `29092`.

```bash
docker compose up -d --build --wait
```

Swagger: http://localhost:8000/docs · RabbitMQ UI: http://localhost:15672 (`guest`/`guest`)

```bash
docker compose down        # mantém dados
docker compose down -v     # zera dados
```

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| `POST` | `/pedidos` | Cria pedido (`PENDENTE`) |
| `GET` | `/pedidos?status=&cliente=&limit=20&offset=0` | Lista paginada, mais recentes primeiro |
| `GET` | `/pedidos/{id}` | Detalhe com histórico de status |
| `PATCH` | `/pedidos/{id}/status` | Muda status (`{"status": "CANCELADO"}`) |
| `GET` | `/pedidos/{id}/eventos` | Trilha de auditoria vinda do Kafka |
| `GET` | `/health` | Mongo, RabbitMQ e Kafka; `503` se algum cair |

### Exemplo

```bash
curl -X POST localhost:8000/pedidos -H 'Content-Type: application/json' \
  -d '{"cliente": "Arthur Navarro", "produto": "Notebook", "quantidade": 2}'
```

```json
{
  "id": "9706978f-6bd4-48b4-b15c-ef01169536ed",
  "cliente": "Arthur Navarro",
  "produto": "Notebook",
  "quantidade": 2,
  "status": "PENDENTE",
  "criado_em": "2026-10-03T00:33:47.512000Z",
  "atualizado_em": "2026-10-03T00:33:47.512000Z",
  "historico": [{ "status": "PENDENTE", "em": "2026-10-03T00:33:47.512000Z" }]
}
```

Alguns segundos depois, `GET /pedidos/{id}` mostra `ENVIADO` e `GET /pedidos/{id}/eventos` traz:

```json
[
  { "tipo": "PedidoCriado",         "dados": { "cliente": "Arthur Navarro", "produto": "Notebook", "quantidade": 2, "status": "PENDENTE" } },
  { "tipo": "PedidoStatusAlterado", "dados": { "de": "PENDENTE", "para": "PROCESSANDO" } },
  { "tipo": "PedidoStatusAlterado", "dados": { "de": "PROCESSANDO", "para": "ENVIADO" } }
]
```

(campos `event_id`, `pedido_id` e `ocorrido_em` omitidos)

## Testes

```bash
python -m venv .venv
.venv\Scripts\activate             # Windows
# source .venv/bin/activate        # Linux/Mac
pip install -r requirements-dev.txt
pytest -v                          # 35 testes, sem precisar de Docker
```

Os testes unitários usam `mongomock-motor` e publishers fake: cobrem API, máquina de estados, relay do outbox (inclusive falha do broker), worker (retomada, cancelamento) e idempotência da auditoria.

Fluxo ponta a ponta contra a stack real:

```bash
docker compose up -d --build --wait
pytest -m e2e -v
```

A CI (GitHub Actions) roda lint (`ruff`), testes unitários e o e2e com a stack completa.

## Inspecionar a mensageria

```bash
# Eventos no Kafka
docker exec -it pedidos-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic pedidos.eventos --from-beginning --property print.key=true

# Filas e DLQ no RabbitMQ
docker exec pedidos-rabbitmq rabbitmqctl list_queues name messages

# Logs dos workers
docker compose logs -f worker-processamento worker-auditoria
```

## Estrutura

```
app/
├── main.py                  # FastAPI, lifespan, /health, inicia o relay
├── config.py                # Settings via variáveis de ambiente
├── db.py                    # Motor client, coleções e índices
├── models.py                # Schemas Pydantic
├── services.py              # Regras de negócio e máquina de estados
├── outbox.py                # Relay do transactional outbox
├── messaging/
│   ├── rabbitmq.py          # Publisher/consumer + topologia com DLQ
│   └── kafka.py             # Producer e loop de consumer
├── routes/pedidos.py
└── workers/
    ├── processamento.py     # Consumer RabbitMQ
    └── auditoria.py         # Consumer Kafka
tests/
├── test_pedidos.py          # API
├── test_mensageria.py       # Outbox, workers, auditoria
└── e2e/                     # Contra o docker compose
```

## Variáveis de ambiente

Padrões em [app/config.py](app/config.py); veja [.env.example](.env.example).

| Variável | Default |
|----------|---------|
| `MONGO_URI` | `mongodb://mongo:27017` |
| `MONGO_DB` | `ecommerce` |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` |
| `RABBITMQ_QUEUE` / `RABBITMQ_DLQ` | `pedidos.processar` / `pedidos.processar.dlq` |
| `KAFKA_BOOTSTRAP` | `kafka:9092` (do host: `localhost:29092`) |
| `KAFKA_TOPIC` | `pedidos.eventos` |
| `OUTBOX_POLL_INTERVAL` | `0.5` s |
| `PROCESSAMENTO_DELAY` | `2.0` s (simula separação/despacho) |

## Próximos passos

- Autenticação (JWT) e pedido com múltiplos itens e valor total
- Métricas (Prometheus) e tracing (OpenTelemetry) propagando o `event_id`
- Retry com backoff antes de mandar para a DLQ
