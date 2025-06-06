"""Integration tests for AVRO serializer class are defined here."""

import struct
from datetime import datetime, timezone
from typing import Any, ClassVar
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from confluent_kafka import Message
from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad.avro import AvroEventModel, AvroSerializerMixin


class TestIntegrationAvroSerializerMixin:
    @pytest.fixture(scope="module")
    def schema_registry(self):
        """Fixture to provide a schema registry client for testing."""
        return SchemaRegistryClient({"url": "http://localhost:8081"})

    @pytest.fixture
    def user_model_cls(self):
        """Fixture to create a UserEvent class for testing."""

        class UserEvent(AvroEventModel):
            topic: ClassVar[str] = "avro.serializer.events.integration"
            namespace: ClassVar[str] = "avro_serializer.integration"
            user_id: UUID
            name: str
            email: str
            created_at: datetime = datetime.now(timezone.utc).replace(microsecond=0)
            login_count: int = 0

        UserEvent._avro_schema = None  # Schema is initially None

        return UserEvent

    @pytest.fixture
    def product_model_cls(self):
        """Fixture to create a ProductEvent class for testing complex structures."""

        class ProductEvent(AvroEventModel):
            topic: ClassVar[str] = "product.events.integration"
            namespace: ClassVar[str] = "product_event.integration"
            product_id: UUID
            tags: dict[str, int]
            versions: list[str]
            metadata: dict[str, Any] = {}

        return ProductEvent

    @pytest.fixture
    def serializer(self, schema_registry):
        """Fixture to initialize the AvroSerializerMixin instance."""
        return AvroSerializerMixin(schema_registry)

    def test_full_serialization_cycle(self, serializer, user_model_cls):
        """Test complete serialization/deserialization cycle."""
        # Register schema and prepare data
        user = user_model_cls(user_id=uuid4(), name="John Doe", email="john@example.com")

        # Serialize the event
        serialized = serializer.serialize_message(user)

        # Verify header format (magic byte and schema ID)
        magic, schema_id = struct.unpack(">bI", serialized[:5])
        assert magic == 0, "Invalid magic byte"
        assert schema_id > 0, "Invalid schema ID"

        # Mock message for deserialization
        message = MagicMock(spec=Message)
        message.value.return_value = serialized
        deserialized = serializer.deserialize_message(message)

        # Verify data integrity
        assert deserialized == user
        assert isinstance(deserialized.created_at, datetime)

    def test_schema_evolution(self, serializer, user_model_cls, schema_registry):
        """Test schema evolution and backward compatibility."""
        # Register the initial schema
        user_model_cls.register_schema(schema_registry)

        # Create a new version of the model
        class UserEventV2(user_model_cls):
            phone_number: str | None = None

        # Check schema compatibility with the registry
        assert UserEventV2.is_compatible_with(schema_registry)

        # Serialize using the new version
        new_user = UserEventV2(
            user_id=uuid4(), name="Alice Smith", email="alice@example.com", phone_number="+1234567890"
        )
        serialized = serializer.serialize_message(new_user)

        # Deserialize with the old version of the model
        message = MagicMock(spec=Message)
        message.value.return_value = serialized
        deserialized_old = user_model_cls.deserialize(serialized, schema_registry)

        # Ensure old version ignores new field
        assert deserialized_old.email == new_user.email
        assert not hasattr(deserialized_old, "phone_number")

    def test_complex_data_structures(self, serializer, product_model_cls):
        """Test serialization/deserialization with complex data structures."""
        # Prepare complex data
        product = product_model_cls(
            product_id=uuid4(), tags={"electronics": 5, "sale": 3}, versions=["1.0", "1.1"], metadata={"category": "A"}
        )

        # Full serialization/deserialization cycle
        serialized = serializer.serialize_message(product)
        message = MagicMock(spec=Message)
        message.value.return_value = serialized
        deserialized = serializer.deserialize_message(message)

        assert deserialized == product
        assert isinstance(deserialized.product_id, UUID)
        assert "electronics" in deserialized.tags

    @pytest.mark.filterwarnings("ignore::UserWarning:pydantic.*")
    def test_error_handling_scenarios(self, serializer, user_model_cls, caplog):
        """Test error handling scenarios for serialization and deserialization."""
        # Test for corrupted data
        message = MagicMock(spec=Message)
        message.value.return_value = b"invalid_data"

        serializer.deserialize_message(message)
        assert "Invalid magic byte" in caplog.text

        # Test schema mismatch error
        class InvalidModel(user_model_cls):
            email: int  # Invalid type for the field

        invalid_data = {"user_id": uuid4(), "name": 123, "email": 456}  # Invalid types

        invalid_user = InvalidModel.model_construct(**invalid_data)

        with pytest.raises(RuntimeError, match="Serialization failed"):
            serializer.serialize_message(invalid_user)

    def test_caching_mechanism(self, serializer, user_model_cls, schema_registry):
        """Test schema caching mechanism to improve efficiency."""
        # First call - register schema
        schema_id = user_model_cls.register_schema(schema_registry)

        # Clear cache
        serializer._schema_cache.clear()
        serializer._get_schema_str.cache_clear()
        schema_registry.get_schema = MagicMock(wraps=schema_registry.get_schema)

        # First schema request
        model_class = serializer._get_model_class(schema_id)
        assert model_class.__name__ == user_model_cls.__name__
        assert model_class.avro_schema() == user_model_cls.avro_schema()

        # Second schema request (should hit cache)
        prev_call_count = schema_registry.get_schema.call_count
        serializer._get_model_class(schema_id)
        assert schema_registry.get_schema.call_count == prev_call_count

    def test_logging_on_failure(self, serializer, caplog):
        """Test logging of errors during serialization/deserialization failures."""
        # Simulate a failed deserialization
        message = MagicMock(spec=Message)
        message.value.return_value = b"invalid"
        message.topic.return_value = "test_topic"
        message.partition.return_value = 1
        message.offset.return_value = 100

        serializer.deserialize_message(message)

        # Check if error is logged correctly
        assert "Invalid magic byte" in caplog.text
        assert "WARNING" in caplog.text
