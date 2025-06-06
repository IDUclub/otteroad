"""Unit tests for Kafka producer client are defined here."""

import asyncio
import threading
from typing import ClassVar
from unittest.mock import ANY, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from confluent_kafka import KafkaError, KafkaException

from otteroad import (
    KafkaProducerClient,
    KafkaProducerSettings,
)
from otteroad.avro import AvroEventModel
from otteroad.utils import LoggerAdapter


class TestKafkaProducerClient:
    @pytest.fixture(autouse=True)
    def mock_schema_registry_client(self):
        """Fixture to mock the SchemaRegistryClient."""
        with patch("otteroad.producer.producer.SchemaRegistryClient") as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance
            yield mock_client_instance

    @pytest.fixture
    def mock_producer_settings(self):
        """Fixture for providing default producer settings for the KafkaProducerClient."""
        return KafkaProducerSettings(bootstrap_servers=["localhost:9092"], acks="all")

    @pytest.fixture
    def mock_logger(self):
        """Fixture for creating a mock logger that tracks logging calls."""
        logger = MagicMock(spec=LoggerAdapter)
        logger.debug = MagicMock()
        logger.info = MagicMock()
        logger.error = MagicMock()
        return logger

    @pytest.fixture
    def mock_event(self):
        """Fixture for creating a mock Avro event to be used in message sending tests."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.topic"
            namespace: ClassVar[str] = "test_event"
            data: str

        return TestEvent(data="test")

    @pytest_asyncio.fixture
    async def producer_client(self, mock_producer_settings, mock_logger):
        """Fixture for creating an instance of KafkaProducerClient for async tests."""
        with patch("otteroad.producer.producer.Producer") as mock_producer:
            producer_client = KafkaProducerClient(producer_settings=mock_producer_settings)
            producer_client._producer = mock_producer.return_value
            producer_client._logger = mock_logger.return_value
            yield producer_client
            await producer_client.close()

    @pytest.mark.asyncio
    async def test_initialization(self, mock_producer_settings):
        """Test the initialization of the KafkaProducerClient."""
        with (
            patch("otteroad.producer.producer.Producer") as mock_producer,
            patch("otteroad.producer.producer.SchemaRegistryClient") as mock_sr,
        ):
            KafkaProducerClient(mock_producer_settings)
            mock_sr.assert_called_once()
            mock_producer.assert_called_once()

    def test_init_raises_when_no_loop(self, monkeypatch, mock_producer_settings):
        """Raise if no event loop is available and not provided manually."""
        monkeypatch.setattr(asyncio, "get_running_loop", Mock(side_effect=RuntimeError("no loop")))
        with pytest.raises(RuntimeError, match="No event loop provided and not running in async context"):
            KafkaProducerClient(producer_settings=mock_producer_settings)

    @pytest.mark.asyncio
    async def test_start_stop_flow(self, producer_client):
        """Test the start and stop flow of the producer producer_client."""
        await producer_client.start()
        assert producer_client.is_running
        producer_client._logger.info.assert_called_with("Producer client started")

        await producer_client.close()
        assert not producer_client.is_running
        producer_client._logger.info.assert_called_with("Producer shutdown completed")

    def test_poll_loop_logs_exception(self, producer_client):
        producer_client._cancelled.clear()
        producer_client._producer.poll = Mock(side_effect=ValueError("test poll error"))
        thread = threading.Thread(target=producer_client._poll_loop)
        thread.start()
        thread.join(timeout=1)
        producer_client._logger.error.assert_called_with(
            "Polling error", error=repr(ValueError("test poll error")), exc_info=True
        )

    @pytest.mark.asyncio
    async def test_send_message_success(self, producer_client, mock_event):
        """Test the successful sending of a message using the producer producer_client."""
        future = asyncio.Future()
        future.set_result(MagicMock())

        with (
            patch.object(producer_client._loop, "create_future", return_value=future),
            patch("asyncio.to_thread", return_value=b"serialized"),
        ):
            await producer_client.start()
            await producer_client.send(mock_event)

            producer_client._producer.produce.assert_called_once_with(
                topic="test.topic", value=b"serialized", key=None, headers=None, on_delivery=ANY
            )
            producer_client._logger.info.assert_called_with("Message successfully sent", topic="test.topic")

    @pytest.mark.asyncio
    async def test_send_message_timeout(self, producer_client, mock_event):
        """Test that a timeout exception is handled correctly when sending a message."""
        future = asyncio.Future()
        future.set_exception(asyncio.TimeoutError())

        with (
            patch.object(producer_client._loop, "create_future", return_value=future),
            patch("asyncio.to_thread", return_value=b"serialized"),
            pytest.raises(RuntimeError) as exc_info,
        ):
            await producer_client.start()
            await producer_client.send(mock_event, timeout=0.1)

        assert "Message delivery timeout" in str(exc_info.value)
        producer_client._logger.error.assert_any_call("Message delivery timeout", topic="test.topic")

    @pytest.mark.asyncio
    async def test_send_raises_if_not_running(self, producer_client, mock_event):
        producer_client._producer = Mock()
        producer_client._loop = asyncio.get_running_loop()
        producer_client._cancelled.set()  # not running
        with pytest.raises(RuntimeError, match="Producer must be started before sending messages"):
            await producer_client.send(mock_event, topic="test")

    @pytest.mark.asyncio
    async def test_send_kafka_exception_in_delivery_handler(self, producer_client, mock_event):
        producer_client._poll_thread = None
        producer_client._cancelled.clear()
        error = KafkaError(1)

        def fake_produce(**kwargs):
            on_delivery = kwargs["on_delivery"]
            msg = Mock(topic=lambda: "t", partition=lambda: 0, offset=lambda: 123)
            on_delivery(error, msg)

        producer_client._producer.produce = fake_produce

        await producer_client.start()
        with pytest.raises(KafkaException):
            await producer_client.send(mock_event, topic="test")

        await asyncio.sleep(1)

        found = False
        for call in producer_client._logger.error.call_args_list:
            if (
                call[0][0] == "Delivery failed"
                and call[1].get("topic") == "test"
                and repr(error) in call[1].get("error", "")
            ):
                found = True
                break

        assert found, "Expected 'Delivery failed' log with KafkaError not found"

    @pytest.mark.asyncio
    async def test_flush_behavior(self, producer_client):
        """Test the flush behavior of the Kafka producer producer_client."""
        with patch.object(producer_client._producer, "flush") as mock_flush:
            await producer_client.start()
            await producer_client.flush()
            mock_flush.assert_called_once_with(30.0)

            # Timeout case
            mock_flush.side_effect = TimeoutError("Timeout")
            with pytest.raises(RuntimeError, match="timeout"):
                await producer_client.flush(timeout=0.1)

    @pytest.mark.asyncio
    async def test_flush_when_not_running(self, producer_client):
        """Test flush when the producer client is not running."""
        producer_client._cancelled.set()
        await producer_client.flush()
        producer_client._logger.debug.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_fails_with_general_exception(self, producer_client):
        error = RuntimeError("flush error")
        with patch.object(producer_client._producer, "flush") as mock_flush:
            await producer_client.start()
            await producer_client.flush()

            # Timeout case
            mock_flush.side_effect = error
            with pytest.raises(RuntimeError, match="flush error"):
                await producer_client.flush(timeout=0.1)

            producer_client._logger.error.assert_any_call("Flush failed", error=repr(error), exc_info=True)

    @pytest.mark.asyncio
    async def test_context_manager(self, producer_client):
        """Test the context manager functionality of the producer producer_client."""
        async with producer_client as producer_client:
            assert producer_client.is_running
        assert not producer_client.is_running

    @pytest.mark.asyncio
    async def test_error_handling(self, producer_client, mock_event):
        """Test error handling during message sending."""
        # Test serialization error
        with (
            patch.object(producer_client, "serialize_message", side_effect=ValueError("Serialization error")),
            pytest.raises(ValueError),
        ):
            await producer_client.start()
            await producer_client.send(mock_event)

        # Test delivery error
        future = asyncio.Future()
        future.set_exception(KafkaException(KafkaError._ALL_BROKERS_DOWN))

        with patch.object(producer_client._loop, "create_future", return_value=future), pytest.raises(KafkaException):
            await producer_client.send(mock_event)

    @pytest.mark.asyncio
    async def test_topic_resolution(self, producer_client, mock_event):
        """Test topic resolution in the message sending flow."""
        future = asyncio.Future()
        future.set_result(MagicMock())

        with (
            patch.object(producer_client._loop, "create_future", return_value=future),
            patch("asyncio.to_thread", return_value=b"serialized"),
        ):
            await producer_client.start()
            await producer_client.send(mock_event, topic="custom.topic")

            producer_client._producer.produce.assert_called_once_with(
                topic="custom.topic", value=b"serialized", key=None, headers=None, on_delivery=ANY
            )

        # Test missing topic
        with pytest.raises(ValueError):
            await producer_client.send(MagicMock(spec=AvroEventModel))

    @pytest.mark.asyncio
    async def test_close_warns_if_thread_does_not_stop(self, producer_client):
        producer_client._cancelled.clear()
        fake_thread = MagicMock()
        fake_thread.is_alive.return_value = True
        producer_client._poll_thread = fake_thread

        await producer_client.close()
        producer_client._logger.warning.assert_called_with("Poll thread did not terminate gracefully")
