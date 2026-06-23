# Pedidos API

API de gerenciamento de pedidos para e-commerce. FastAPI + MongoDB + RabbitMQ + Kafka, tudo orquestrado via Docker Compose.

Ao cadastrar um pedido a API persiste no MongoDB, publica mensagem na fila RabbitMQ e evento no tópico Kafka.

## Stack

- **FastAPI** (Python 3.12, async)
- **MongoDB 7** (driver Motor async)
- **RabbitMQ 3.13** (aio-pika)
- **Kafka 7.6.1** + **Zookeeper** (aiokafka)
- **Pytest** + httpx + mongomock-motor

## Estrutura

```
pedidos-api/
├── app/
│   ├── config.py            # Settings via env vars
│   ├── db.py                # Motor client + collection
│   ├── dependencies.py      # Publishers singletons (FastAPI Depends)
│   ├── main.py              # FastAPI + lifespan + /health
│   ├── models.py            # PedidoCreate, Pedido, StatusPedido
│   ├── messaging/
│   │   ├── kafka.py         # Publisher Kafka + retry connect
│   │   └── rabbitmq.py      # Publisher Rabbit + retry connect
│   └── routes/pedidos.py    # POST /pedidos, GET /pedidos
├── tests/
│   ├── conftest.py          # AsyncClient + mongomock + FakePublisher
│   └── test_pedidos.py      # 8 testes
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── pytest.ini
├── .env.example
├── .dockerignore
└── .gitignore
```

## Pré-requisitos

- Docker Desktop (Windows/Mac) ou Docker Engine + Compose v2 (Linux)
- Portas livres: `8000`, `27017`, `5672`, `15672`, `29092`

## Executar

Um único comando sobe API + Mongo + Rabbit + Kafka + Zookeeper:

```bash
docker compose up --build
```

API disponível em `http://localhost:8000`. Kafka pode levar 30-90s para ficar `healthy` na primeira execução — a API faz retry automático.

Derrubar (mantém volume Mongo):
```bash
docker compose down
```

Derrubar e zerar dados:
```bash
docker compose down -v
```

## Endpoints

Swagger UI: `http://localhost:8000/docs`

### POST /pedidos — Cadastrar pedido

Request:
```json
{
  "cliente": "Arthur Navarro",
  "produto": "Notebook",
  "quantidade": 2
}
```

Response `201 Created`:
```json
{
  "id": "9706978f-6bd4-48b4-b15c-ef01169536ed",
  "cliente": "Arthur Navarro",
  "produto": "Notebook",
  "quantidade": 2,
  "status": "PENDENTE"
}
```

Efeitos colaterais:
- Documento inserido na coleção `ecommerce.pedidos` no MongoDB.
- Mensagem publicada na fila RabbitMQ `pedidos.criados` (payload mínimo `{id, status}`).
- Evento publicado no tópico Kafka `pedidos.criados` (payload completo).

### GET /pedidos — Listar pedidos

Response `200 OK`:
```json
[
  {
    "id": "9706978f-...",
    "cliente": "Arthur Navarro",
    "produto": "Notebook",
    "quantidade": 2,
    "status": "PENDENTE"
  }
]
```

### GET /health — Healthcheck

Response `200 OK`: `{"status": "ok"}` quando MongoDB está acessível.

## Modelo de Pedido

| Campo | Tipo | Regras |
|-------|------|--------|
| `id` | string (UUID4) | Gerado pela API |
| `cliente` | string | obrigatório, 1-200 chars |
| `produto` | string | obrigatório, 1-200 chars |
| `quantidade` | int | obrigatório, > 0 |
| `status` | enum | inicial sempre `PENDENTE` |

Status possíveis: `PENDENTE`, `PROCESSANDO`, `ENVIADO`, `ENTREGUE`, `CANCELADO`.

## Testes

8 testes cobrindo cadastro, listagem, publicação em Rabbit/Kafka e validações.

Os testes usam `mongomock-motor` e publishers fake — não precisam de Mongo/Rabbit/Kafka rodando.

```bash
python -m venv .venv
.venv\Scripts\activate           # Windows
# source .venv/bin/activate      # Linux/Mac
pip install -r requirements.txt
pytest -v
```

Saída esperada: `8 passed`.

## Validar mensageria após subir

### MongoDB
```bash
docker exec -it pedidos-mongo mongosh --quiet --eval "db.getSiblingDB('ecommerce').pedidos.find().pretty()"
```

### RabbitMQ
UI de gerenciamento: `http://localhost:15672` (login `guest` / `guest`) → tab **Queues and Streams** → fila `pedidos.criados`.

### Kafka
```bash
docker exec -it pedidos-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic pedidos.criados --from-beginning --timeout-ms 5000
```

## Variáveis de ambiente

Padrões definidos em [app/config.py](app/config.py). Override via `.env` (ver `.env.example`).

| Variável | Default |
|----------|---------|
| `MONGO_URI` | `mongodb://mongo:27017` |
| `MONGO_DB` | `ecommerce` |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` |
| `RABBITMQ_QUEUE` | `pedidos.criados` |
| `KAFKA_BOOTSTRAP` | `kafka:9092` |
| `KAFKA_TOPIC` | `pedidos.criados` |
| `CONNECT_RETRIES` | `30` |
| `CONNECT_RETRY_DELAY` | `2.0` |

## Portas expostas

| Serviço | Host | Container |
|---------|------|-----------|
| API | 8000 | 8000 |
| MongoDB | 27017 | 27017 |
| RabbitMQ AMQP | 5672 | 5672 |
| RabbitMQ UI | 15672 | 15672 |
| Kafka (externo) | 29092 | 29092 |

Para clientes Kafka rodando no host, usar `localhost:29092`. Dentro da rede do compose, usar `kafka:9092`.
