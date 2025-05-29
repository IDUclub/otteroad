"""Unit tests for AVRO serializer class are defined here."""

import json
import logging
import struct
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Message
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from otteroad.avro import AvroEventModel, AvroSerializerMixin


class MyModel(AvroEventModel):
    """A test model inheriting from AvroEventModel to demonstrate serialization and deserialization."""

    topic: ClassVar[str] = "test.topic"
    namespace: ClassVar[str] = "test_subject"
    field: int


class TestAvroSerializerMixin:
    @pytest.fixture
    def mock_schema_registry(self):
        """Mocked schema registry client for testing purposes."""
        return MagicMock(spec=SchemaRegistryClient)

    @pytest.fixture
    def serializer(self, mock_schema_registry):
        """Create an instance of AvroSerializerMixin using the mocked schema registry."""
        return AvroSerializerMixin(mock_schema_registry)

    @pytest.fixture
    def valid_message(self):
        """Create a valid Kafka message with serialized data for testing deserialization."""
        schema_registry_mock = MagicMock()
        schema_registry_mock.register_schema.return_value = 1
        serialized_data = MyModel(field=42).serialize(schema_registry_mock)

        message = MagicMock(spec=Message)
        message.value.return_value = serialized_data
        message.topic.return_value = "test_topic"
        message.partition.return_value = 0
        message.offset.return_value = 123

        return message

    def test_serialize_message_success(self, serializer):
        """Test the successful serialization of a message."""
        mock_event = MagicMock(spec=MyModel)
        mock_event.serialize.return_value = b"serialized_data"

        result = serializer.serialize_message(mock_event)

        mock_event.serialize.assert_called_once_with(serializer.schema_registry)
        assert result == b"serialized_data"

    def test_serialize_message_failure(self, serializer):
        """Test failure during serialization with an error raised."""
        mock_event = MagicMock()
        mock_event.serialize.side_effect = ValueError("Test error")

        with pytest.raises(RuntimeError) as exc_info:
            serializer.serialize_message(mock_event)

        assert "Test error" in str(exc_info.value)

    def test_deserialize_message_success(self, serializer, valid_message):
        """Test the successful deserialization of a message."""
        schema_str = json.dumps(MyModel.avro_schema(), separators=(",", ":"))
        serializer.schema_registry.get_schema.return_value = SimpleNamespace(schema_str=schema_str)

        result = serializer.deserialize_message(valid_message)

        assert isinstance(result, MyModel)
        assert result.field == 42

    def test_deserialize_invalid_message_length(self, serializer):
        """Test deserialization with an invalid message length."""
        message = MagicMock(spec=Message)
        message.value.return_value = b"short"

        with pytest.raises(RuntimeError):
            serializer.deserialize_message(message)

    def test_deserialize_invalid_magic_byte(self, serializer):
        """Test deserialization with an invalid magic byte."""
        message = MagicMock(spec=Message)
        message.value.return_value = b"\x01\x00\x00\x00\x01" + b"morebytes"

        with pytest.raises(RuntimeError) as exc_info:
            serializer.deserialize_message(message)

        assert "Invalid magic byte" in str(exc_info.value)

    def test_get_model_class_caching(self, serializer):
        """
        Test caching mechanism for schema model class lookup.
        """
        schema_id = 123
        serializer._schema_cache[schema_id] = MyModel

        result = serializer._get_model_class(schema_id)

        assert result == MyModel
        serializer.schema_registry.get_schema.assert_not_called()

    def test_get_model_class_not_found(self, serializer, caplog):
        """Test behavior when the model class for a schema ID is not found."""
        schema_id = 999
        fake_schema = MagicMock()
        fake_schema.schema_str = '{"type": "test"}'
        serializer.schema_registry.get_schema.return_value = fake_schema

        with caplog.at_level(logging.WARNING):
            model_class = serializer._get_model_class(schema_id)

        assert model_class is None
        assert "No registered model" in caplog.text

    def test_deserialize_returns_none_for_unregistered_schema(self, serializer):
        """Test deserialization of an unregistered schema."""
        schema_str = '{"type": "record", "name": "Unknown", "fields": []}'
        schema_id = 456
        message = MagicMock(spec=Message)
        payload = b"\x00" + struct.pack(">I", schema_id) + b"{}"
        message.value.return_value = payload
        message.topic.return_value = "test_topic"
        message.partition.return_value = 0
        message.offset.return_value = 123

        serializer.schema_registry.get_schema.return_value = SimpleNamespace(schema_str=schema_str)

        result = serializer.deserialize_message(message)

        assert result is None

    def test_get_schema_str_caching(self, serializer):
        """Test caching for schema string retrieval based on schema ID."""
        schema_id = 123
        fake_schema = MagicMock()
        fake_schema.schema_str = '{"type": "test"}'
        serializer.schema_registry.get_schema.return_value = fake_schema

        # First call
        result1 = serializer._get_schema_str(schema_id)
        # Second call (should be cached)
        result2 = serializer._get_schema_str(schema_id)

        assert result1 == result2 == '{"type": "test"}'
        serializer.schema_registry.get_schema.assert_called_once_with(schema_id)

    def test_get_schema_str_failure_logs_error(self, serializer, caplog):
        """Test behavior when schema string retrieval fails."""
        schema_id = 999
        serializer.schema_registry.get_schema.side_effect = RuntimeError("Registry error")

        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="Registry error"):
                serializer._get_schema_str(schema_id)

        assert "Schema fetch failed" in caplog.text
        assert str(schema_id) in caplog.text

    def test_error_logging_on_deserialization_failure(self, serializer, caplog):
        """Test that errors during deserialization are logged correctly."""
        message = MagicMock(spec=Message)
        message.topic.return_value = "test_topic"
        message.partition.return_value = 0
        message.offset.return_value = 123
        message.value = b"invalid_data"

        with pytest.raises(RuntimeError):
            serializer.deserialize_message(message)

        assert "Deserialization error" in caplog.text

    def test_model_class_resolution_logic(self, serializer):
        """Test the resolution of the model class based on schema ID."""
        schema_id = 123
        schema_str = json.dumps(MyModel.avro_schema(), separators=(",", ":"))
        fake_schema = MagicMock()
        fake_schema.schema_str = schema_str
        serializer.schema_registry.get_schema.return_value = fake_schema

        result = serializer._get_model_class(schema_id)

        assert result == MyModel
        assert serializer._schema_cache[schema_id] == MyModel

    def test_compatibility_with_avro_serializer(self, serializer):
        """Test compatibility with the Avro serializer by checking magic byte and schema ID."""
        mock_event = MagicMock(spec=MyModel)
        schema_id = 123
        serializer.schema_registry.register_schema.return_value = schema_id

        magic_byte = b"\x00"
        schema_id_bytes = struct.pack(">I", schema_id)  # 4 bytes
        payload = b"data"

        mock_event.serialize.return_value = magic_byte + schema_id_bytes + payload
        serializer.schema_registry.register_schema.return_value = schema_id

        serialized = serializer.serialize_message(mock_event)

        magic, received_schema_id = struct.unpack(">bI", serialized[:5])

        assert magic == 0
        assert received_schema_id == schema_id

    def test_validation_error_handling(self, serializer):
        """Test error handling for invalid message data that leads to a validation error."""
        schema_id = 123
        invalid_data = b"\x00\x00\x00\x00\x01{}"
        message = MagicMock(spec=Message)
        message.value = invalid_data
        schema_str = json.dumps(MyModel.avro_schema())
        serializer.schema_registry.get_schema.return_value = Schema(schema_id, "AVRO", schema_str)

        with pytest.raises(RuntimeError):
            serializer.deserialize_message(message)

    def test_deserialize_message_none_value(self, serializer):
        """Test deserialization with None value."""
        message = MagicMock(spec=Message)
        message.value.return_value = None
        message.topic.return_value = "test_topic"
        message.partition.return_value = 0
        message.offset.return_value = 123

        with pytest.raises(RuntimeError, match="Invalid message: missing or incomplete value"):
            serializer.deserialize_message(message)
