"""Integration tests for handler registry class are defined here."""

import asyncio
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from confluent_kafka import Message

from otteroad.avro import AvroEventModel
from otteroad.consumer.handlers import BaseMessageHandler, EventHandlerRegistry
from otteroad.utils import LoggerProtocol


class TestIntegrationEventHandlerRegistry:
    @pytest.fixture
    def registry(self):
        """Fixture to create an EventHandlerRegistry instance for testing."""
        return EventHandlerRegistry()

    @pytest.fixture
    def test_logger(self):
        """Fixture to create a mock logger for testing."""
        logger = MagicMock(spec=LoggerProtocol)
        logger.debug = MagicMock()
        logger.info = MagicMock()
        logger.error = MagicMock()
        return logger

    @pytest.fixture
    def user_created_event(self):
        """Fixture to create a mock UserCreatedEvent class."""

        class UserCreatedEvent(AvroEventModel):
            topic: ClassVar[str] = "user.events"
            namespace: ClassVar[str] = "user_created"
            user_id: str
            email: str

        return UserCreatedEvent

    @pytest.fixture
    def order_created_event(self):
        """Fixture to create a mock OrderCreatedEvent class."""

        class OrderCreatedEvent(AvroEventModel):
            topic: ClassVar[str] = "order.events"
            namespace: ClassVar[str] = "order_created"
            order_id: str
            amount: float

        return OrderCreatedEvent

    @pytest.fixture
    def user_created_handler(self, user_created_event):
        """Fixture to create a mock handler for UserCreatedEvent."""

        class UserCreatedHandler(BaseMessageHandler[user_created_event]):
            def __init__(self, logger: LoggerProtocol = None):
                super().__init__(logger)
                self.handle_mock = AsyncMock()

            async def handle(self, event: user_created_event, ctx: Message):
                """Handle the event."""
                await self.handle_mock(event, ctx)

            async def on_startup(self):
                """Startup hook."""
                pass

            async def on_shutdown(self):
                """Shutdown hook."""
                pass

        return UserCreatedHandler

    @pytest.fixture
    def order_created_handler(self, order_created_event):
        """Fixture to create a mock handler for OrderCreatedEvent."""

        class OrderCreatedHandler(BaseMessageHandler[order_created_event]):
            def __init__(self, logger: LoggerProtocol = None):
                super().__init__(logger)
                self.process_mock = AsyncMock()

            async def handle(self, event: order_created_event, ctx: Message):
                """Handle the event."""
                await self.process_mock(event, ctx)

            async def on_startup(self):
                """Startup hook."""
                pass

            async def on_shutdown(self):
                """Shutdown hook."""
                pass

        return OrderCreatedHandler

    @pytest.mark.asyncio
    async def test_full_lifecycle_processing(
        self,
        registry,
        test_logger,
        user_created_handler,
        order_created_handler,
        user_created_event,
        order_created_event,
    ):
        """Test the full lifecycle of event processing with registration and handling."""
        # Initialize handlers
        user_handler = user_created_handler(test_logger)
        order_handler = order_created_handler(test_logger)

        # Register handlers
        registry.register(user_handler)
        registry.register(order_handler)

        # Create test events
        user_event = user_created_event(user_id="123", email="test@example.com")
        order_event = order_created_event(order_id="ORD-456", amount=99.99)

        # Mock Kafka messages
        user_message = MagicMock()
        user_message.topic.return_value = "user.events"
        order_message = MagicMock()
        order_message.topic.return_value = "order.events"

        # Process events
        await registry.get_handler(user_event).handle(user_event, user_message)
        await registry.get_handler(order_event).handle(order_event, order_message)

        # Check that handlers were called
        user_handler.handle_mock.assert_awaited_once_with(user_event, user_message)
        order_handler.process_mock.assert_awaited_once_with(order_event, order_message)

    @pytest.mark.asyncio
    async def test_handler_replacement(self, registry, test_logger, user_created_handler, user_created_event):
        """Test handler replacement in the registry."""
        # Register first handler
        handler_v1 = user_created_handler(test_logger)
        registry.register(handler_v1)

        # Register new handler that replaces the old one
        handler_v2 = user_created_handler(test_logger)
        registry.register(handler_v2, overwrite=True)

        # Ensure that the new handler is being used
        event = user_created_event(user_id="456", email="new@test.com")
        await registry.get_handler(event).handle(event, MagicMock())

        handler_v1.handle_mock.assert_not_called()
        handler_v2.handle_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cross_component_interaction(self, registry, test_logger, user_created_handler, user_created_event):
        """Test cross-component interaction with the handler registry."""
        # Initialize handler and register it
        user_handler = user_created_handler(test_logger)
        registry.register(user_handler)

        # Simulate an external system
        class EventProcessor:
            def __init__(self, registry):
                self.registry = registry

            async def process(self, event, message):
                handler = self.registry.get_handler(event)
                await handler.handle(event, message)

        processor = EventProcessor(registry)
        event = user_created_event(user_id="789", email="processor@test.com")
        message = MagicMock()

        # Process event using external processor
        await processor.process(event, message)

        # Verify that handler was called correctly
        user_handler.handle_mock.assert_awaited_once_with(event, message)

    def test_registry_state_consistency(self, registry, user_created_handler, user_created_event):
        """Test consistency of the registry's internal state."""
        handler = user_created_handler()
        event_type = user_created_event

        registry.register(handler)
        assert str(event_type) in registry.handlers

        registry.unregister(event_type)
        assert str(event_type) not in registry.handlers

    @pytest.mark.asyncio
    async def test_error_scenarios(self, registry, test_logger, order_created_handler, order_created_event):
        """Test error scenarios like invalid handler registration and missing handlers."""
        # Test registering an invalid handler
        invalid_handler = MagicMock()  # Not a BaseMessageHandler
        with pytest.raises(TypeError):
            registry.register(invalid_handler)

        # Test getting a handler for a non-registered event
        event = order_created_event(order_id="000", amount=0.0)
        assert registry.get_handler(event) is None

        # Register valid handler after an error
        valid_handler = order_created_handler(test_logger)
        registry.register(valid_handler)
        assert registry.has_handler(order_created_event)

    @pytest.mark.asyncio
    async def test_concurrent_registration(self, registry, user_created_handler):
        """Test concurrent registration of handlers."""

        async def register_handlers():
            handlers = [user_created_handler() for _ in range(10)]
            for handler in handlers:
                registry.register(handler, overwrite=True)

        await asyncio.gather(register_handlers(), register_handlers())

        # After concurrent execution, only one handler should remain
        assert len(registry.handlers) == 1

    @pytest.mark.asyncio
    async def test_metrics_integration(self, registry, test_logger, user_created_handler, user_created_event):
        """Test integration with a metrics collection system."""

        class MetricsCollector:
            def __init__(self):
                self.metrics = []

            def log_metric(self, name, value):
                self.metrics.append((name, value))

        metrics = MetricsCollector()
        handler = user_created_handler(test_logger)
        handler.post_process = AsyncMock(side_effect=lambda: metrics.log_metric("processed", 1))

        registry.register(handler)
        event = user_created_event(user_id="metrics", email="metrics@test.com")

        # Process event and collect metrics
        await registry.get_handler(event).process(event, MagicMock())
        await asyncio.sleep(0.1)  # Give some time for post_process to execute

        assert ("processed", 1) in metrics.metrics
