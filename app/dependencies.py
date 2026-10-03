from app.messaging.kafka import KafkaPublisher
from app.messaging.rabbitmq import RabbitMQClient

rabbit_client = RabbitMQClient()
kafka_publisher = KafkaPublisher()
