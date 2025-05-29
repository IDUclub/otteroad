"""Unit tests for AVRO event model are defined here."""

import json
import struct
from datetime import date, datetime, timezone
from enum import Enum
from typing import ClassVar, Union
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import fastavro
import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import Schema
from pydantic import Field, ValidationError

from otteroad.avro import AvroEventModel


class MyEnum(Enum):
    """Enum for testing purposes."""

    FOO = "foo"
    BAR = "bar"


class UserEvent(AvroEventModel):
    """Example Avro Event Model: User Event with UUID, string action, and timestamp."""

    topic: ClassVar[str] = "user.events"
    namespace: ClassVar[str] = "user_event"
    user_id: UUID
    action: str
    timestamp: datetime
    optional_field: int | None = Field(None, description="optional field")


class NestedModel(AvroEventModel):
    """Nested model used in a more complex event."""

    topic: ClassVar[str] = "nested.events"
    namespace: ClassVar[str] = "nested_event"
    value: Union[int, str, None]
    optional_field: int | None


class EnumModel(AvroEventModel):
    """Example using Enum as a field."""

    topic: ClassVar[str] = "enum.events"
    namespace: ClassVar[str] = "enum_event"
    enum_field: MyEnum


class ComplexModel(AvroEventModel):
    """Complex model using other models."""

    topic: ClassVar[str] = "complex.events"
    namespace: ClassVar[str] = "complex_event"
    nested: NestedModel
    ids: list[UUID]
    metadata: dict[str, int]


class TestAvroEventModel:

    @pytest.fixture
    def mock_registry(self):
        """Fixture to mock schema registry client."""
        mock_registry = MagicMock(spec=SchemaRegistryClient)
        mock_registry.register_schema.return_value = 123
        mock_registry.get_schema.return_value = Schema(
            schema_type="AVRO",
            schema_str=json.dumps(UserEvent.avro_schema()),
        )
        return mock_registry

    @pytest.fixture
    def user_event(self):
        """Fixture for a UUID event."""
        return UserEvent(
            user_id=uuid4(),
            action="login",
            timestamp=datetime.now(timezone.utc).replace(microsecond=0),
            optional_field=42,
        )

    def test_avro_schema_generation_basic_types(self):
        """Verify basic Avro schema generation."""
        schema = UserEvent.avro_schema()

        assert schema["type"] == "record"
        assert schema["name"] == "UserEvent"
        assert schema["namespace"] == "user.events.user_event"

        fields = {f["name"]: f["type"] for f in schema["fields"]}
        assert fields["user_id"] == {"type": "string", "logicalType": "uuid"}
        assert fields["action"] == "string"
        assert fields["timestamp"] == {"type": "long", "logicalType": "timestamp-millis"}
        assert fields["optional_field"] == ["null", "long"]

    def test_avro_schema_with_nested_model(self):
        """Verify Avro schema with nested model."""
        schema = ComplexModel.avro_schema()
        nested_field = next(f for f in schema["fields"] if f["name"] == "nested")

        assert nested_field["type"]["type"] == "record"
        assert nested_field["type"]["name"] == "NestedModel"

    def test_avro_schema_with_enum(self):
        """ "Verify Avro schema with Enum field."""
        schema = EnumModel.avro_schema()
        enum_field = next(f for f in schema["fields"] if f["name"] == "enum_field")

        assert enum_field["type"]["type"] == "enum"
        assert enum_field["type"]["name"] == "MyEnum"
        assert enum_field["type"]["symbols"] == ["FOO", "BAR"]

    def test_optional_field_handling(self):
        """Handling of optional fields in Avro schema"""
        schema = UserEvent.avro_schema()
        optional_field = next(f for f in schema["fields"] if f["name"] == "optional_field")

        assert optional_field["type"] == ["null", "long"]
        assert optional_field["default"] is None

    def test_to_avro_dict_conversion(self, user_event):
        """Convert event to Avro dictionary."""
        avro_dict = user_event.to_avro_dict()

        assert avro_dict["user_id"] == str(user_event.user_id)
        assert avro_dict["timestamp"] == int(user_event.timestamp.timestamp() * 1000)
        assert avro_dict["optional_field"] == 42

    def test_serialize_deserialize_roundtrip(self, mock_registry, user_event):
        """Serialize and deserialize an event."""
        # Serialization
        serialized = user_event.serialize(mock_registry)

        # Check header
        magic, schema_id = struct.unpack(">bI", serialized[:5])
        assert magic == 0
        assert schema_id == 123

        # Deserialize
        deserialized = UserEvent.deserialize(serialized, mock_registry)

        assert deserialized.model_dump() == user_event.model_dump()

    def test_immutable_model(self):
        """Ensure immutable fields can't be changed."""
        event = UserEvent(user_id=uuid4(), action="login", timestamp=datetime.now(timezone.utc))

        with pytest.raises(ValidationError):
            event.action = "logout"

    def test_extra_fields_forbidden(self):
        """Ensure extra fields are not allowed."""
        with pytest.raises(ValidationError):
            UserEvent(user_id=uuid4(), action="login", timestamp=datetime.now(), extra_field="invalid")

    def test_register_schema_caching(self, mock_registry):
        """Schema registration caching functionality."""
        UserEvent._schema_id = None

        # Register schema first time
        schema_id = UserEvent.register_schema(mock_registry)
        assert schema_id == 123

        # On second call, cache is used
        mock_registry.register_schema.reset_mock()
        assert UserEvent.register_schema(mock_registry) == 123
        mock_registry.register_schema.assert_not_called()

    def test_schema_compatibility_check(self, mock_registry):
        """Check schema compatibility."""
        mock_registry.test_compatibility.return_value = True

        assert UserEvent.is_compatible_with(mock_registry) is True
        mock_registry.test_compatibility.assert_called_once()

        mock_registry.set_compatibility.side_effect = SchemaRegistryError(
            http_status_code=500, error_code=101, error_message="error"
        )

        assert UserEvent.is_compatible_with(mock_registry) is False

    def test_deserialize_invalid_data(self, mock_registry):
        """Handle deserialization with invalid data."""
        invalid_data = b"invalid"

        with pytest.raises(ValueError):
            UserEvent.deserialize(invalid_data, mock_registry)

    def test_unsupported_type_handling(self):
        """Handle unsupported types in model fields."""

        class InvalidModel(AvroEventModel):
            topic: ClassVar[str] = "invalid.events"
            namespace: ClassVar[str] = "invalid_event"
            value: complex  # Unsupported type

        # Get schema and check 'value' field type is 'string'
        schema = InvalidModel.avro_schema()
        value_field = next(field for field in schema["fields"] if field["name"] == "value")
        assert value_field["type"] == "string"

    def test_enum_serialization(self):
        """Enum field serialization."""
        event = EnumModel(enum_field=MyEnum.BAR)

        avro_dict = event.to_avro_dict()
        assert avro_dict["enum_field"] == "BAR"

    def test_nested_model_serialization(self):
        """Nested model serialization."""
        nested = NestedModel(value="test", optional_field=1)
        event = ComplexModel(nested=nested, ids=[uuid4()], metadata={"key": 42})

        avro_dict = event.to_avro_dict()
        assert isinstance(avro_dict["nested"], dict)
        assert avro_dict["nested"]["value"] == "test"

    def test_date_handling(self):
        """Handle date fields correctly."""

        class DateModel(AvroEventModel):
            topic: ClassVar[str] = "date.events"
            namespace: ClassVar[str] = "date_event"
            d: date

        test_date = date(2023, 1, 1)
        event = DateModel(d=test_date)

        avro_dict = event.to_avro_dict()
        assert avro_dict["d"] == (test_date - date(1970, 1, 1)).days

    @patch("fastavro.schemaless_reader")
    def test_schema_resolution_error(self, mock_reader, mock_registry):
        """Schema resolution errors are handled correctly."""
        mock_reader.side_effect = fastavro.read.SchemaResolutionError("Incompatible")

        with pytest.raises(ValueError):
            UserEvent.deserialize(b"\x00\x00\x00\x00\x01{}", mock_registry)

    def test_default_values_in_schema(self):
        """Default values in schema."""

        class DefaultModel(AvroEventModel):
            topic: ClassVar[str] = "default.events"
            namespace: ClassVar[str] = "default_event"
            field: str = "default_value"

        schema = DefaultModel.avro_schema()
        field_schema = next(f for f in schema["fields"] if f["name"] == "field")
        assert field_schema["default"] == "default_value"

    def test_model_representation(self):
        """Model representation."""

        event = UserEvent(user_id=uuid4(), action="login", timestamp=datetime.now(timezone.utc))

        assert str(event) == UserEvent.schema_subject()
        assert f"<{UserEvent.__name__}" in repr(event)
