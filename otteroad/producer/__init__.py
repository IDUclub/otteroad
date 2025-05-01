"""
This package provides a high-performance, thread-safe Kafka producer client with:

- Async/Await interface for message sending
- Avro serialization of messages
- Background polling thread for delivery reports
- Graceful shutdown and flushing of messages
- Configurable message queue limits and error handling

Usage example:

    from otteroad.producer import KafkaProducerClient
    from your_project.models import YourAvroEventModel
    from your_project.settings import producer_settings

    async def main():
        async with KafkaProducerClient(producer_settings) as producer:
            event = YourAvroEventModel(...)
            await producer.send(event)

Exports:
    - KafkaProducerClient: Async Kafka producer client with integrated Avro serialization.
"""

from .producer import KafkaProducerClient

__all__ = [
    "KafkaProducerClient",
]
