from app.messaging.kafka import KafkaPublisher
from app.messaging.rabbitmq import RabbitMQPublisher

rabbit_publisher = RabbitMQPublisher()
kafka_publisher = KafkaPublisher()


def get_rabbitmq() -> RabbitMQPublisher:
    return rabbit_publisher


def get_kafka() -> KafkaPublisher:
    return kafka_publisher
