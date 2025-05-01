"""
This package provides Avro event models and serialization utilities with:

- Pydantic-based Avro model definitions with strong typing
- Automatic Avro schema generation and validation
- Confluent Schema Registry integration for schema management
- Efficient binary serialization and deserialization
- Model-based serialization for Kafka producers and consumers
- Caching and error handling for high-throughput applications

Usage examples:

    from otteroad.avro import AvroEventModel, AvroSerializerMixin

    # Define a custom Avro event
    class UserCreatedEvent(AvroEventModel):
        topic: ClassVar[str] = "user.created"
        user_id: UUID
        email: str
        created_at: datetime = datetime.now()

    # Serialize an event
    serializer = AvroSerializerMixin(schema_registry_client)
    bytes_message = serializer.serialize_message(UserCreatedEvent(user_id=..., email=...))

    # Deserialize a Kafka message
    event = serializer.deserialize_message(kafka_message)

Exports:
    - AvroEventModel: Base class for defining Avro-compatible event models
    - AvroSerializerMixin: Mixin for Kafka clients to handle Avro serialization
"""

from .model import AvroEventModel
from .serializer import AvroSerializerMixin

__all__ = [
    "AvroEventModel",
    "AvroSerializerMixin",
]
