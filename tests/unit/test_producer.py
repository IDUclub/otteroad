"""Unit tests for Kafka producer client are defined here."""

import asyncio
from typing import ClassVar
from unittest.mock import ANY, MagicMock, patch

import pytest
import pytest_asyncio
from confluent_kafka import KafkaError, KafkaException

from idu_kafka_client import (
    KafkaProducerClient,
    KafkaProducerSettings,
)
from idu_kafka_client.avro import AvroEventModel
from idu_kafka_client.utils import LoggerProtocol


class TestKafkaProducerClient:
    @pytest.fixture(autouse=True)
    def mock_schema_registry_client(self):
        """Fixture to mock the SchemaRegistryClient."""
        with patch("idu_kafka_client.producer.producer.SchemaRegistryClient") as mock_client_cls:
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
        logger = MagicMock(spec=LoggerProtocol)
        logger.debug = MagicMock()
        logger.info = MagicMock()
        logger.error = MagicMock()
        return logger

    @pytest.fixture
    def mock_event(self):
        """Fixture for creating a mock Avro event to be used in message sending tests."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.topic"
            schema_subject: ClassVar[str] = "test_event"
            data: str

        return TestEvent(data="test")

    @pytest_asyncio.fixture
    async def producer_client(self, mock_producer_settings, mock_logger):
        """Fixture for creating an instance of KafkaProducerClient for async tests."""
        with patch("idu_kafka_client.producer.producer.Producer") as mock_producer:
            client = KafkaProducerClient(producer_settings=mock_producer_settings, logger=mock_logger)
            client._producer = mock_producer.return_value
            yield client
            await client.close()

    @pytest.mark.asyncio
    async def test_initialization(self, mock_producer_settings, mock_logger):
        """Test the initialization of the KafkaProducerClient."""
        with (
            patch("idu_kafka_client.producer.producer.Producer") as mock_producer,
            patch("idu_kafka_client.producer.producer.SchemaRegistryClient") as mock_sr,
        ):
            KafkaProducerClient(mock_producer_settings, mock_logger)
            mock_sr.assert_called_once()
            mock_producer.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_stop_flow(self, producer_client, mock_logger):
        """Test the start and stop flow of the producer client."""
        await producer_client.start()
        assert producer_client.is_running
        mock_logger.info.assert_called_with("Producer client started")

        await producer_client.close()
        assert not producer_client.is_running
        mock_logger.info.assert_called_with("Producer shutdown completed")

    @pytest.mark.asyncio
    async def test_send_message_success(self, producer_client, mock_event):
        """Test the successful sending of a message using the producer client."""
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
            producer_client._logger.info.assert_called_with("Message successfully sent to %s", "test.topic")

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
        producer_client._logger.error.assert_any_call("Message delivery timeout to %s", "test.topic")

    @pytest.mark.asyncio
    async def test_flush_behavior(self, producer_client):
        """Test the flush behavior of the Kafka producer client."""
        with patch.object(producer_client._producer, "flush") as mock_flush:
            await producer_client.start()
            await producer_client.flush()
            mock_flush.assert_called_once_with(30.0)

            # Timeout case
            mock_flush.side_effect = TimeoutError("Timeout")
            with pytest.raises(RuntimeError, match="timeout"):
                await producer_client.flush(timeout=0.1)

    @pytest.mark.asyncio
    async def test_context_manager(self, producer_client):
        """Test the context manager functionality of the producer client."""
        async with producer_client as client:
            assert client.is_running
        assert not client.is_running

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
