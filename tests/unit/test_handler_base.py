"""Unit tests for BaseMessageHandler are defined here."""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from confluent_kafka import Message
from pydantic import BaseModel

from otteroad.consumer.handlers import BaseMessageHandler
from otteroad.utils import LoggerAdapter, LoggerProtocol


class TestBaseMessageHandler:
    @pytest.fixture
    def test_event_model(self):
        """Fixture for creating a test event model using Pydantic."""

        class TestEvent(BaseModel):
            id: int
            name: str

        return TestEvent

    @pytest.fixture
    def concrete_handler_cls(self, test_event_model):
        """Fixture for creating a concrete handler class based on `BaseMessageHandler` for testing purposes."""

        class ConcreteHandler(BaseMessageHandler[test_event_model]):
            async def on_startup(self):
                pass

            async def on_shutdown(self):
                pass

            async def handle(self, event: test_event_model, ctx: Message):
                pass

        return ConcreteHandler

    @pytest.fixture
    def handler(self, concrete_handler_cls):
        """Fixture for creating an instance of the concrete handler with the mocked logger."""
        return concrete_handler_cls()

    @pytest.fixture
    def mock_message(self):
        """Fixture for creating a mock Kafka message."""
        msg = MagicMock(spec=Message)
        msg.topic.return_value = "test_topic"
        msg.partition.return_value = 0
        msg.offset.return_value = 100
        return msg

    def test_event_type_inference(self, concrete_handler_cls, test_event_model):
        """Test that the event type is correctly inferred and matches the event model."""
        assert concrete_handler_cls._event_type == test_event_model
        assert concrete_handler_cls().event_type == test_event_model

    def test_missing_generic_type(self):
        """Test that an error is raised when the generic type for the event is not provided."""
        with pytest.raises(TypeError) as exc_info:

            class InvalidHandler(BaseMessageHandler):
                async def on_startup(self): ...

                async def on_shutdown(self): ...

                async def handle(self, event, ctx): ...

        assert "Cannot determine EventT" in str(exc_info.value)

    def test_manual_event_type_override(self):
        """Test that the event type can be manually overridden by a subclass."""

        class ManualEvent(BaseModel):
            field: str

        class ManualHandler(BaseMessageHandler[ManualEvent]):
            async def on_startup(self): ...

            async def on_shutdown(self): ...

            async def handle(self, event, ctx): ...

        assert ManualHandler._event_type == ManualEvent

    @pytest.mark.asyncio
    async def test_lifecycle_methods(self, handler):
        """Test that the lifecycle methods `on_startup` and `on_shutdown` are invoked correctly."""
        handler.on_startup = AsyncMock()
        handler.on_shutdown = AsyncMock()

        await handler.on_startup()
        await handler.on_shutdown()

        handler.on_startup.assert_awaited_once()
        handler.on_shutdown.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_process_flow(self, handler, mock_message, test_event_model):
        """Test the flow of the `process` method, ensuring that `pre_process`, `handle`, and `post_process`
        are called in the correct order."""
        handler.pre_process = AsyncMock(return_value=(test_event_model(id=1, name="test"), mock_message))
        handler.handle = AsyncMock()
        handler.post_process = AsyncMock()

        event = test_event_model(id=1, name="test")

        await handler.process(event, mock_message)

        handler.pre_process.assert_awaited_once_with(event, mock_message)
        handler.handle.assert_awaited_once_with(event, mock_message)
        handler.post_process.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_handling_flow(self, handler, mock_message, test_event_model):
        """Test the error handling flow, ensuring that errors during the `handle` method are properly caught
        and processed by `handle_error`."""
        test_error = ValueError("Test error")
        handler.handle = AsyncMock(side_effect=test_error)
        handler.handle_error = AsyncMock()

        event = test_event_model(id=1, name="test")

        await handler.process(event, mock_message)

        handler.handle_error.assert_awaited_once_with(test_error, event, mock_message)

    @pytest.mark.asyncio
    async def test_pre_process_logging(self, handler, mock_message):
        """Test that logging occurs correctly during the `pre_process` method."""
        event = handler.event_type(id=1, name="test")

        with patch.object(handler._logger, "debug") as mock_debug:
            await handler.pre_process(event, mock_message)
            mock_debug.assert_called_once_with(
                "Pre-processing message",
                event_model=type(event).__name__,
                topic="test_topic",
                partition=0,
                offset=100,
            )

    @pytest.mark.asyncio
    async def test_post_process_logging(self, handler):
        """Test that logging occurs correctly during the `post_process` method."""
        with patch.object(handler._logger, "debug") as mock_debug:
            await handler.post_process()
            mock_debug.assert_called_once_with("Post-processing completed successfully")

    @pytest.mark.asyncio
    async def test_handle_error_behavior(self, handler, mock_message, test_event_model):
        """
        Test that errors during the `handle_error` method are logged correctly.
        """
        test_error = RuntimeError("Critical error")
        event = test_event_model(id=1, name="test")

        with patch.object(handler._logger, "error") as mock_error:
            with pytest.raises(type(test_error)):
                await handler.handle_error(test_error, event, mock_message)

            mock_error.assert_called_once_with(
                "Error processing message",
                event_model=type(event).__name__,
                topic="test_topic",
                partition=0,
                offset=100,
                error=repr(test_error),
                exc_info=True,
            )

    def test_abstract_method_implementation(self):
        """Test that a `TypeError` is raised if a `BaseMessageHandler` subclass does not implement
        all required abstract methods."""

        class InvalidHandler(BaseMessageHandler[BaseModel]):
            pass

        with pytest.raises(TypeError) as exc_info:
            InvalidHandler()

        assert "Can't instantiate abstract class" in str(exc_info.value)

    def test_logger_injection(self, concrete_handler_cls):
        """Test that the logger is correctly injected into the handler class."""
        mock_logger = MagicMock()
        handler = concrete_handler_cls(logger=mock_logger)
        assert handler._logger._logger == mock_logger

    def test_default_logger(self, concrete_handler_cls):
        """Test that the default logger is used when no logger is provided."""
        handler = concrete_handler_cls()
        assert isinstance(handler._logger._logger, logging.Logger)
        assert handler._logger._logger.name.endswith("handlers.base")
