"""
This package provides the Kafka consumer-side infrastructure for the idu_kafka_client library.

- Asynchronous and thread-safe Kafka message consumption
- Typed event processing using Pydantic/Avro schemas
- Flexible registration and management of event handlers
- Support for Schema Registry-based serialization
- Graceful startup and shutdown of consumer workers

Typical Usage:
    1. Instantiate `KafkaConsumerService` with Kafka consumer settings.
    2. Register event handlers by subclassing `BaseMessageHandler`.
    3. Add workers for specific Kafka topics.
    4. Start the service to begin consuming and processing messages.
    5. Stop the service cleanly on shutdown.

Usage example:
    from idu_kafka_client.consumer import KafkaConsumerService
    service = KafkaConsumerService(consumer_settings)
    service.register_handler(MyEventHandler())
    worker = service.add_worker(topics=["my_topic"])
    await service.start()
    # ...
    await service.stop()

Exports:
    - `BaseMessageHandler`: Abstract handler with pre-/post-processing and error management.
    - `EventHandlerRegistry`: Registry for associating event types with handler instances.
    - `KafkaConsumerService`: Orchestration service for managing multiple Kafka consumer workers.
    - `KafkaConsumerWorker`: Worker to consume Kafka messages, deserialize them into events, and dispatch to handlers.
"""

from .handlers import BaseMessageHandler, EventHandlerRegistry
from .service import KafkaConsumerService
from .worker import KafkaConsumerWorker

__all__ = [
    "BaseMessageHandler",
    "EventHandlerRegistry",
    "KafkaConsumerService",
    "KafkaConsumerWorker",
]
