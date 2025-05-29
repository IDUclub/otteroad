"""Unit tests for Kafka consumer service are defined here."""

import asyncio
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from otteroad.avro import AvroEventModel
from otteroad.consumer import BaseMessageHandler, KafkaConsumerService, KafkaConsumerWorker
from otteroad.settings import KafkaConsumerSettings
from otteroad.utils import LoggerAdapter


class TestKafkaConsumerService:

    @pytest.fixture
    def mock_consumer_settings(self):
        """Fixture for mocking Kafka consumer settings."""
        return KafkaConsumerSettings(
            bootstrap_servers=["localhost:9092"],
            group_id="test-group",
            schema_registry_url="http://localhost:8081",
        )

    @pytest.fixture
    def mock_logger(self):
        """Fixture for mocking a logger."""
        logger = MagicMock(spec=LoggerAdapter)
        logger.info = MagicMock()
        logger.debug = MagicMock()
        logger.error = MagicMock()
        return logger

    @pytest.fixture
    def service(self, mock_consumer_settings, mock_logger):
        """Fixture for KafkaConsumerService instance."""
        service = KafkaConsumerService(consumer_settings=mock_consumer_settings)
        service._logger = mock_logger.return_value
        return service

    @pytest.fixture
    def mock_worker(self):
        """Fixture for mocking a Kafka consumer worker."""
        worker = MagicMock(spec=KafkaConsumerWorker)
        worker.start = AsyncMock()
        worker.stop = AsyncMock()
        return worker

    @pytest.fixture(autouse=True)
    def mock_schema_registry_client(self):
        """Fixture for mocking the schema registry client."""
        with patch("otteroad.consumer.service.SchemaRegistryClient") as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance
            yield mock_client_instance

    def test_initialization(self, mock_consumer_settings, mock_logger):
        """Test the initialization of the KafkaConsumerService."""
        service = KafkaConsumerService(mock_consumer_settings, mock_logger)

        # Assert that the service is correctly initialized with the provided settings
        assert service._settings == mock_consumer_settings
        assert service._handler_registry is not None
        assert service._schema_registry is not None
        assert service._workers == []
        mock_logger.info.assert_not_called()

    @pytest.mark.asyncio
    async def test_add_worker_single_topic(self, service):
        """Test adding a worker for a single topic."""
        result = service.add_worker("test-topic")

        # Assert that the worker was successfully added for the specified topic
        assert result is service
        assert len(service._workers) == 1
        worker = service._workers[0]
        assert isinstance(worker, KafkaConsumerWorker)
        service._logger.info.assert_called_with("Created worker for topics", topics="test-topic")

    @pytest.mark.asyncio
    async def test_add_worker_multiple_topics(self, service):
        """Test adding a worker for multiple topics."""
        result = service.add_worker(["topic1", "topic2"])

        # Assert that the worker was successfully added for the specified topics
        assert result is service
        assert len(service._workers) == 1
        service._logger.info.assert_called_with("Created worker for topics", topics="topic1, topic2")

    @pytest.mark.asyncio
    async def test_add_worker_with_settings_override(self, service):
        """Test adding a worker with overridden consumer settings."""
        custom_settings = {"max.poll.interval.ms": 600000}
        service.add_worker("test-topic", consumer_settings=custom_settings)

        # Assert that the overridden settings were applied correctly
        worker = service._workers[0]
        assert worker._settings["max.poll.interval.ms"] == 600000

    def test_add_worker_no_topics_error(self, service):
        """Test adding a worker with no topics specified (should raise an error)."""
        with pytest.raises(ValueError) as exc_info:
            service.add_worker([])

        # Assert that the appropriate error message is raised
        assert "At least one topic must be specified" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_start_workers(self, service, mock_worker):
        """Test starting multiple Kafka consumer workers."""
        service._workers = [mock_worker, mock_worker]

        await service.start()

        # Assert that the workers' start methods were called the correct number of times
        assert mock_worker.start.await_count == 2
        service._logger.info.assert_any_call("Initializing consumer workers", num_workers=2)
        service._logger.debug.assert_called_with("All workers started successfully")

    @pytest.mark.asyncio
    async def test_stop_workers(self, service, mock_worker):
        """Test stopping multiple Kafka consumer workers."""
        service._workers = [mock_worker, mock_worker]

        await service.stop()

        # Assert that the workers' stop methods were called the correct number of times
        assert mock_worker.stop.await_count == 2
        assert len(service._workers) == 0
        service._logger.info.assert_any_call("Initiating consumer service shutdown")
        service._logger.info.assert_called_with("Shutdown completed successfully")

    def test_register_handler(self, service):
        """Test registering a message handler."""
        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = MagicMock(__name__="TestEvent")

        service.register_handler(handler)

        # Assert that the handler is successfully registered
        assert service._handler_registry.handlers
        service._logger.info.assert_called_with("Registered handler for event", event_model="TestEvent")

    def test_unregister_handler_by_type(self, service):
        """Test unregistering a handler by event type."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test"
            namespace: ClassVar[str] = "test"

        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = TestEvent

        service.register_handler(handler)
        service.unregister_handler(TestEvent)

        # Assert that the handler is successfully unregistered by type
        service._logger.info.assert_called_with("Unregistered handler", event_type="TestEvent")

    def test_unregister_handler_by_name(self, service):
        """Test unregistering a handler by event type name."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test"
            namespace: ClassVar[str] = "test"

        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = TestEvent

        service.register_handler(handler)
        service.unregister_handler(str(TestEvent))

        # Assert that the handler is successfully unregistered by name
        service._logger.info.assert_called_with("Unregistered handler", event_type=str(TestEvent))

    @pytest.mark.asyncio
    async def test_concurrent_start_stop(self, service):
        """Test concurrent starting and stopping of workers."""
        worker1 = MagicMock(start=AsyncMock(), stop=AsyncMock())
        worker2 = MagicMock(start=AsyncMock(), stop=AsyncMock())
        service._workers = [worker1, worker2]

        # Run both start and stop concurrently and ensure they complete correctly
        await asyncio.gather(service.start(), service.stop())

        # Assert that both start and stop methods were called once for each worker
        worker1.start.assert_awaited_once()
        worker1.stop.assert_awaited_once()
