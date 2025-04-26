"""
Unified, Pydantic- and asyncio-based Kafka client framework
with Avro serialization, Schema Registry integration, and
typed configuration for Python microservices.

Features:
    - KafkaConsumerService & KafkaConsumerWorker for high-throughput consumption
    - KafkaProducerClient for async, thread-safe message production
    - Pydantic models for consumer/producer settings with env/YAML loading
    - Handler registry and BaseMessageHandler for pipeline orchestration

Quickstart example:

>>>>    from idu_kafka_client import (
>>>>        KafkaConsumerService,
>>>>        KafkaProducerClient,
>>>>        KafkaConsumerSettings,
>>>>        KafkaProducerSettings,
>>>>        BaseMessageHandler,
>>>>    )
>>>>
>>>>    # Load settings
>>>>    consumer_settings = KafkaConsumerSettings.from_env()
>>>>    producer_settings = KafkaProducerSettings.from_env()
>>>>
>>>>    # Define event handler
>>>>    class MyEventHandler(BaseMessageHandler[MyEventModel]):
>>>>        async def handle(self, event, ctx):
>>>>            # business logic
>>>>            ...
>>>>
>>>>    # Create consumer service
>>>>    service = KafkaConsumerService(consumer_settings)
>>>>    service.register_handler(MyEventHandler())
>>>>    service.add_worker(topics=["my_topic"]).start()
>>>>
>>>>    # Produce an event
>>>>    producer = KafkaProducerClient(producer_settings)
>>>>    event = MyEventModel(...)
>>>>    await producer.start()
>>>>    await producer.send(event)

Exports:
    - KafkaConsumerService: Orchestrates multiple consumer workers
    - KafkaConsumerWorker: Low-level consumer polling and dispatch
    - BaseMessageHandler: Abstract base for message handlers
    - KafkaProducerClient: Async Kafka producer with Avro support
    - KafkaConsumerSettings, KafkaProducerSettings: Typed Pydantic settings
    - AvroEventModel: Schema-driven base AVRO event model
"""

from .consumer.handlers.base import BaseMessageHandler

# Consumer
from .consumer.service import KafkaConsumerService
from .consumer.worker import KafkaConsumerWorker

# Producer
from .producer import KafkaProducerClient

# Settings
from .settings import KafkaConsumerSettings, KafkaProducerSettings

__all__ = [
    # Consumer
    "KafkaConsumerService",
    "KafkaConsumerWorker",
    "BaseMessageHandler",
    # Producer
    "KafkaProducerClient",
    # Settings
    "KafkaConsumerSettings",
    "KafkaProducerSettings",
]
