"""Integration tests for AVRO event model are defined here."""

import json
import struct
from datetime import datetime, timedelta, timezone
from typing import ClassVar
from uuid import UUID, uuid4

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from pydantic import ValidationError

from otteroad.avro import AvroEventModel


class TestIntegrationAvroEventModel:
    @pytest.fixture(scope="module")
    def schema_registry(self):
        """Fixture to provide a schema registry client for testing."""
        return SchemaRegistryClient({"url": "http://localhost:8081"})

    @pytest.fixture
    def user_event_cls(self):
        """Fixture to define the `UserEvent` model class for testing."""

        class UserEvent(AvroEventModel):
            topic: ClassVar[str] = "avro.model.events"
            namespace: ClassVar[str] = "integration"
            schema_compatibility: ClassVar[str] = "FULL"
            user_id: UUID
            action: str
            timestamp: datetime
            tags: dict[str, int]
            version: int = 1

        return UserEvent

    @pytest.fixture
    def complex_event_cls(self):
        """Fixture to define the `ComplexEvent` model class for testing, including a nested event."""

        class ComplexEvent(AvroEventModel):
            class NestedEvent(AvroEventModel):
                topic: ClassVar[str] = "nested.events"
                namespace: ClassVar[str] = "integration"
                value: str

            topic: ClassVar[str] = "complex.events"
            namespace: ClassVar[str] = "integration"
            event_id: UUID
            nested: NestedEvent
            sequence: list[int]
            optional_field: str | None = None

        return ComplexEvent

    def test_full_cycle_with_real_registry(self, schema_registry, user_event_cls):
        """Test to ensure the full cycle of schema registration, serialization, and deserialization."""
        # Register the schema
        schema_id = user_event_cls.register_schema(schema_registry)
        assert isinstance(schema_id, int)

        # Check schema compatibility
        assert user_event_cls.is_compatible_with(schema_registry)

        # Create an instance of the event
        original = user_event_cls(
            user_id=uuid4(),
            action="login",
            timestamp=datetime.now(timezone.utc).replace(microsecond=0),
            tags={"attempt": 3},
        )

        # Serialize the event
        serialized = original.serialize(schema_registry)
        assert len(serialized) > 50

        # Deserialize the event
        deserialized = user_event_cls.deserialize(serialized, schema_registry)
        assert deserialized == original

    def test_schema_evolution(self, schema_registry, user_event_cls):
        """Test schema evolution. Ensures compatibility when adding new fields to a model."""
        # Register the first version of the schema
        user_event_cls.register_schema(schema_registry)

        # Create a new version of the model
        class UserEventV2(user_event_cls):
            schema_version: ClassVar[int] = 2
            new_field: str = "default"

        # Check compatibility of the new version
        assert UserEventV2.is_compatible_with(schema_registry)

        # Serialize the new version of the event
        new_event = UserEventV2(
            user_id=uuid4(),
            action="login",
            timestamp=datetime.now(timezone.utc).replace(microsecond=0),
            tags={"attempt": 1},
            new_field="extra",
        )
        serialized = new_event.serialize(schema_registry)

        # Deserialize using the old version
        old_event = user_event_cls.deserialize(serialized, schema_registry)
        assert old_event.schema_version == 1

    def test_complex_structure_serialization(self, schema_registry, complex_event_cls):
        """Test the serialization and deserialization of complex event structures,
        including nested events and optional fields."""
        # Register the complex event schema
        complex_event_cls.register_schema(schema_registry)

        # Create a complex event instance
        original = complex_event_cls(
            event_id=uuid4(),
            nested=complex_event_cls.NestedEvent(value="test"),
            sequence=[1, 2, 3],
            optional_field="present",
        )

        # Serialize and deserialize the complex event
        serialized = original.serialize(schema_registry)
        deserialized = complex_event_cls.deserialize(serialized, schema_registry)

        # Validate the results
        assert deserialized == original
        assert deserialized.nested.value == "test"
        assert deserialized.optional_field == "present"

    def test_avro_serializer_integration(self, schema_registry, user_event_cls):
        """Test the integration with the official Avro serializer to ensure it works as expected."""
        user_event_cls.register_schema(schema_registry)

        # Initialize the Avro serializer
        avro_serializer = AvroSerializer(
            schema_registry_client=schema_registry, schema_str=json.dumps(user_event_cls.avro_schema())
        )

        # Create a test event
        event = user_event_cls(user_id=uuid4(), action="logout", timestamp=datetime.now(), tags={"session": 5})

        # Serialize using the Avro serializer
        avro_data = avro_serializer(
            event.to_avro_dict(), SerializationContext(topic="some-topic", field=MessageField.VALUE)  # Specify the correct topic
        )

        # Check the serialized data for schema compatibility
        assert avro_data[1:5] == struct.pack(">I", user_event_cls._schema_id)

    def test_error_handling_scenarios(self, schema_registry, user_event_cls):
        """Test various error handling scenarios, including invalid data and schema incompatibility."""
        # Test handling of corrupted data
        with pytest.raises(ValueError):
            user_event_cls.deserialize(b"invalid_data", schema_registry)

        # Test validation errors (incorrect data types)
        with pytest.raises(ValidationError):
            user_event_cls(
                user_id=uuid4(),
                action={"test": 1},  # expected a string, but passing a dict
                timestamp=datetime.now(),
                tags={"test": 1},
            )

        # Register the original schema
        user_event_cls.register_schema(schema_registry)

        # Test schema incompatibility
        class UserEvent(user_event_cls):
            @classmethod
            def avro_schema(cls):
                """Modify the schema by removing the 'action' field."""
                schema = super().avro_schema()
                schema_copy = schema.copy()
                schema_copy["fields"] = [field for field in schema_copy["fields"] if field["name"] != "action"]
                return schema_copy

        # Check that the new schema is incompatible
        assert not UserEvent.is_compatible_with(schema_registry)

    def test_serialization_performance(self, schema_registry, user_event_cls, benchmark):
        """Benchmark the performance of serializing a large number of events."""
        events = [
            user_event_cls(
                user_id=uuid4(),
                action=f"action_{i}",
                timestamp=datetime.now() + timedelta(minutes=i),
                tags={"index": i},
            )
            for i in range(1000)
        ]

        def serialize_events():
            return [e.serialize(schema_registry) for e in events]

        benchmark(serialize_events)

    def test_deserialization_performance(self, schema_registry, user_event_cls, benchmark):
        """Benchmark the performance of deserializing a large number of events."""
        events = [
            user_event_cls(
                user_id=uuid4(),
                action=f"action_{i}",
                timestamp=datetime.now() + timedelta(minutes=i),
                tags={"index": i},
            )
            for i in range(1000)
        ]
        serialized_events = [e.serialize(schema_registry) for e in events]

        def deserialize_events():
            return [user_event_cls.deserialize(e, schema_registry) for e in serialized_events]

        benchmark(deserialize_events)
