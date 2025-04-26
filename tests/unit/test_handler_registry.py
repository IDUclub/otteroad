"""Unit tests for handler registry class are defined here."""

from typing import ClassVar
from unittest.mock import MagicMock

import pytest

from idu_kafka_client.avro import AvroEventModel
from idu_kafka_client.consumer.handlers import BaseMessageHandler, EventHandlerRegistry


class TestEventHandlerRegistry:
    @pytest.fixture
    def registry(self):
        """Fixture for creating a new instance of the EventHandlerRegistry."""
        return EventHandlerRegistry()

    @pytest.fixture
    def mock_handler(self):
        """Fixture for creating a mock handler with a test event model."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.topic"
            schema_subject: ClassVar[str] = "test_subject"

        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = TestEvent
        return handler

    @pytest.fixture
    def another_mock_handler(self):
        """Fixture for creating another mock handler with a different test event model."""

        class AnotherTestEvent(AvroEventModel):
            topic: ClassVar[str] = "another.topic"
            schema_subject: ClassVar[str] = "another_subject"

        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = AnotherTestEvent
        return handler

    def test_register_handler_success(self, registry, mock_handler):
        """Test that a handler is successfully registered in the registry."""
        registry.register(mock_handler)
        assert str(mock_handler.event_type) in registry.handlers

    def test_register_invalid_handler_type(self, registry):
        """Test that an error is raised when trying to register an invalid handler type
        that is not a subclass of `BaseMessageHandler`."""
        invalid_handler = MagicMock()
        with pytest.raises(TypeError, match="Handler must be a subclass of BaseMessageHandler"):
            registry.register(invalid_handler)

    def test_register_duplicate_handler(self, registry, mock_handler):
        """Test that an error is raised when trying to register the same handler twice."""
        registry.register(mock_handler)
        with pytest.raises(ValueError):
            registry.register(mock_handler)

        registry.register(mock_handler, overwrite=True)
        assert len(registry.handlers) == 1

    def test_unregister_handler(self, registry, mock_handler):
        """Test that a handler can be unregistered from the registry."""
        registry.register(mock_handler)
        registry.unregister(mock_handler.event_type)
        assert str(mock_handler.event_type) not in registry.handlers

    def test_unregister_nonexistent_handler(self, registry):
        """Test that trying to unregister a nonexistent handler raises a `KeyError`."""
        with pytest.raises(KeyError, match="No handler for"):
            registry.unregister("nonexistent.event")

    def test_get_handler_by_class(self, registry, mock_handler):
        """Test that a handler can be retrieved by its event class."""
        registry.register(mock_handler)
        handler = registry.get_handler(mock_handler.event_type)
        assert handler == mock_handler

    def test_get_handler_by_instance(self, registry, mock_handler):
        """Test that a handler can be retrieved by an instance of its event type."""
        registry.register(mock_handler)
        event_instance = mock_handler.event_type()
        handler = registry.get_handler(event_instance)
        assert handler == mock_handler

    def test_get_nonexistent_handler(self, registry):
        """Test that retrieving a nonexistent handler returns `None`."""

        class UnknownEvent(AvroEventModel):
            topic: ClassVar[str] = "unknown.topic"
            schema_subject: ClassVar[str] = "unknown_subject"

        assert registry.get_handler(UnknownEvent) is None

    def test_has_handler(self, registry, mock_handler):
        """Test the `has_handler` method to check if a handler is present in the registry."""
        registry.register(mock_handler)
        assert registry.has_handler(mock_handler.event_type)
        assert registry.has_handler(str(mock_handler.event_type))
        assert not registry.has_handler("nonexistent.event")

    def test_handlers_property(self, registry, mock_handler, another_mock_handler):
        """Test the `handlers` property to ensure it returns a copy of the registered handlers."""
        registry.register(mock_handler)
        registry.register(another_mock_handler)

        handlers = registry.handlers
        assert len(handlers) == 2
        assert handlers is not registry._handlers  # Check if handlers list is a copy

    def test_contains_operator(self, registry, mock_handler):
        """Test the `in` operator for checking if an event type is in the registry."""
        registry.register(mock_handler)
        assert mock_handler.event_type in registry
        assert str(mock_handler.event_type) in registry
        assert "invalid.event" not in registry

    def test_reverse_mapping_cleanup(self, registry, mock_handler):
        """Test that the registry behaves correctly after unregistering a handler,
        ensuring that reverse mappings are cleaned up."""
        registry.register(mock_handler)
        registry.unregister(mock_handler.event_type)

    def test_multiple_registrations(self, registry, mock_handler, another_mock_handler):
        """Test that multiple handlers can be registered without issue and that each
        handler can be retrieved correctly by its event type."""
        registry.register(mock_handler)
        registry.register(another_mock_handler)

        assert len(registry.handlers) == 2
        assert registry.get_handler(mock_handler.event_type) == mock_handler
        assert registry.get_handler(another_mock_handler.event_type) == another_mock_handler

    def test_handler_str_representation(self, registry, mock_handler):
        """Test the string representation of the handler's event type and ensure it
        is correctly reflected in the registry."""
        registry.register(mock_handler)
        event_str = str(mock_handler.event_type)
        assert event_str in registry.handlers
        assert registry.has_handler(event_str)
