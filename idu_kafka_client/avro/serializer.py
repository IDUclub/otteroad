"""
Provides reusable serialization components for Kafka clients with:
- Schema Registry integration
- Bidirectional Avro serialization
- Schema caching
- Model class resolution
- Error handling
"""

import json
import logging
import struct
from functools import lru_cache
from typing import TypeVar

from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient

from idu_kafka_client.avro.model import AvroEventModel
from idu_kafka_client.utils import LoggerProtocol

EventModel = TypeVar("EventModel", bound=AvroEventModel)


class AvroSerializerMixin:
    """
    Mixin class providing Avro serialization capabilities for Kafka clients.

    Features:
    - Schema Registry integration
    - Message serialization/deserialization
    - Schema caching for performance
    - Automatic model class resolution
    - Comprehensive error handling

    Args:
        schema_registry_client: Schema Registry client instance
        logger: Custom logger instance (default: module logger)
    """

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        *args,
        logger: LoggerProtocol | None = None,
        **kwargs,
    ):
        """
        Initialize serializer with Schema Registry client.

        Args:
            schema_registry_client: Configured Schema Registry client
            logger: Custom logger implementation
        """
        super().__init__(*args, **kwargs)
        self.logger = logger or logging.getLogger(__name__)
        self.schema_registry = schema_registry_client
        self._schema_cache: dict[int, type[AvroEventModel]] = {}

    @lru_cache(maxsize=100)
    def _get_schema_str(self, schema_id: int) -> str:  # pylint: disable=missing-raises-doc
        """
        Retrieve schema string from Schema Registry with caching.

        Args:
            schema_id: Schema Registry schema ID

        Returns:
            str: Avro schema JSON string

        Raises:
            SchemaRegistryError: If schema retrieval fails
        """
        try:
            return self.schema_registry.get_schema(schema_id).schema_str
        except Exception as e:
            self.logger.error("Schema fetch failed for ID %d: %s", schema_id, str(e), exc_info=True)
            raise

    def _get_model_class(self, schema_id: int) -> type[EventModel]:
        """
        Resolve AvroEventModel class from schema ID.

        Args:
            schema_id: Schema Registry schema ID

        Returns:
            type[AvroEventModel]: Corresponding model class

        Raises:
            ValueError: If no matching model class found
        """
        if schema_id not in self._schema_cache:
            schema_str = self._get_schema_str(schema_id)

            # Search through all registered subclasses
            for model in AvroEventModel.__subclasses__():
                if json.dumps(model.avro_schema(), separators=(",", ":")) == schema_str:
                    self._schema_cache[schema_id] = model
                    self.logger.debug("Cached model %s for schema ID %d", model.__name__, schema_id)
                    break
            else:
                error_msg = f"No registered model for schema ID {schema_id}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        return self._schema_cache[schema_id]

    def serialize_message(self, event: EventModel) -> bytes:
        """
        Serialize AvroEventModel instance to Confluent wire format.

        Args:
            event: Event model instance to serialize

        Returns:
            bytes: Serialized message bytes with header

        Raises:
            RuntimeError: If serialization fails
        """
        try:
            self.logger.debug("Serializing %s", type(event).__name__)
            return event.serialize(self.schema_registry)
        except Exception as e:
            error_msg = f"Serialization failed for {type(event).__name__}: {str(e)}"
            self.logger.exception(error_msg)
            raise RuntimeError(error_msg) from e

    def deserialize_message(self, message: Message) -> EventModel:
        """
        Deserialize Kafka message to AvroEventModel instance.

        Args:
            message: Raw Kafka message

        Returns:
            AvroEventModel: Deserialized event instance

        Raises:
            RuntimeError: If deserialization fails
            ValueError: For invalid message format
        """
        try:
            self.logger.debug("Deserializing message from %s", message.topic())
            value = message.value()

            if not value or len(value) < 5:
                raise ValueError("Invalid message: missing or incomplete value")

            # Extract Confluent header (magic byte + schema ID)
            magic, schema_id = struct.unpack(">bI", value[:5])
            if magic != 0:
                raise ValueError(f"Invalid magic byte: {magic}")

            self.logger.debug("Deserializing schema ID %d", schema_id)
            model_class = self._get_model_class(schema_id)
            return model_class.deserialize(value, self.schema_registry)

        except Exception as e:
            error_msg = f"Deserialization failed: {str(e)}"
            self.logger.exception(
                "Deserialization error in %s[%d]@%d: %s",
                message.topic(),
                message.partition(),
                message.offset(),
                error_msg,
            )
            raise RuntimeError(error_msg) from e
