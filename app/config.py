from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "ecommerce"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbitmq_queue: str = "pedidos.processar"
    rabbitmq_dlx: str = "pedidos.dlx"
    rabbitmq_dlq: str = "pedidos.processar.dlq"
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic: str = "pedidos.eventos"
    kafka_group_auditoria: str = "auditoria"
    connect_retries: int = 30
    connect_retry_delay: float = 2.0
    outbox_poll_interval: float = 0.5
    processamento_delay: float = 2.0

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()
