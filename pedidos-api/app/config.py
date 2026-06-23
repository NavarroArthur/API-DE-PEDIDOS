from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongo_uri: str = "mongodb://mongo:27017"
    mongo_db: str = "ecommerce"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbitmq_queue: str = "pedidos.criados"
    kafka_bootstrap: str = "kafka:9092"
    kafka_topic: str = "pedidos.criados"
    connect_retries: int = 30
    connect_retry_delay: float = 2.0

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


settings = Settings()
