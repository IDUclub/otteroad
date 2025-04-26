"""Unit tests for Kafka consumer worker are defined here."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from confluent_kafka import Consumer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad.consumer import EventHandlerRegistry, KafkaConsumerWorker


class TestKafkaConsumerWorker:
    """Test suite for KafkaConsumerWorker class."""

    @pytest.fixture
    def mock_schema_registry(self):
        """Fixture for mocking the SchemaRegistryClient."""
        return MagicMock(spec=SchemaRegistryClient)

    @pytest.fixture
    def mock_handler_registry(self):
        """Fixture for mocking the EventHandlerRegistry."""
        registry = MagicMock(spec=EventHandlerRegistry)
        registry.get_handler.return_value = MagicMock(process=Mock())
        return registry

    @pytest.fixture
    def valid_message(self):
        """Fixture for mocking a valid Kafka message."""
        mock_msg = MagicMock(spec=Message)
        mock_msg.value.return_value = b"valid-avro"
        mock_msg.topic.return_value = "test"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 123
        mock_msg.error.return_value = None
        return mock_msg

    @pytest.fixture
    def mock_kafka_consumer(self):
        """Fixture for mocking the Kafka Consumer."""
        consumer = MagicMock(spec=Consumer)
        consumer.poll.return_value = None
        consumer.subscribe = MagicMock()
        consumer.assign = MagicMock()
        consumer.unassign = MagicMock()
        consumer.commit = MagicMock()
        consumer.close = MagicMock()
        return consumer

    @pytest_asyncio.fixture
    async def consumer_worker(self, mock_schema_registry, mock_handler_registry, mock_kafka_consumer):
        """Fixture for setting up a KafkaConsumerWorker instance."""
        with patch("otteroad.consumer.worker.Consumer", return_value=mock_kafka_consumer):
            worker = KafkaConsumerWorker(
                consumer_config={
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "dummy",
                    "enable.auto.commit": False,
                },
                schema_registry=mock_schema_registry,
                handler_registry=mock_handler_registry,
                topics=["test-topic"],
            )
            yield worker
            await worker.stop()

    @pytest.mark.asyncio
    async def test_initialization(self, consumer_worker):
        """Test the initialization of KafkaConsumerWorker."""
        # Check if the worker is initialized correctly
        assert consumer_worker._topics == ["test-topic"]
        assert consumer_worker._loop is not None
        assert consumer_worker._poll_thread is None

    @pytest.mark.asyncio
    async def test_start_stop(self, consumer_worker, mock_kafka_consumer):
        """Test starting and stopping the KafkaConsumerWorker."""
        await consumer_worker.start()

        # Assert that the poll thread is alive after starting
        assert consumer_worker._poll_thread is not None
        assert consumer_worker._poll_thread.is_alive()

        poll_thread = consumer_worker._poll_thread  # Save the thread reference before stopping
        await consumer_worker.stop()

        # Assert that the poll thread is not alive after stopping
        assert not poll_thread.is_alive()
        assert consumer_worker._poll_thread is None

    @pytest.mark.asyncio
    async def test_poll_loop_enqueues_messages(self, consumer_worker, mock_kafka_consumer, valid_message):
        """Test that the poll loop enqueues messages."""
        # Arrange: simulate a valid message and override the process loop
        mock_kafka_consumer.poll.side_effect = [valid_message, None]
        original_process_loop = consumer_worker._process_loop
        consumer_worker._process_loop = AsyncMock()  # Stub to prevent actual processing

        # Act: start the worker and wait for a message to be enqueued
        await consumer_worker.start()
        try:
            msg = await asyncio.wait_for(consumer_worker._queue.get(), timeout=1.0)

            # Assert that the enqueued message matches the valid message
            assert msg is valid_message
            consumer_worker._queue.task_done()  # Manually mark the task as done
        finally:
            # Restore the original process loop before stopping
            consumer_worker._process_loop = original_process_loop
            await consumer_worker.stop()

    @pytest.mark.asyncio
    async def test_handle_message_success(self, consumer_worker, mock_handler_registry, valid_message):
        """Test handling a valid message successfully."""
        # Arrange: mock the event deserialization and handler
        mock_event = MagicMock()
        consumer_worker.deserialize_message = Mock(return_value=mock_event)

        # Act: handle the message
        await consumer_worker._handle_message(valid_message)

        # Assert: check if the handler was called correctly
        mock_handler_registry.get_handler.assert_called_once_with(mock_event)
        handler = mock_handler_registry.get_handler.return_value
        handler.process.assert_called_once_with(mock_event, valid_message)
        assert consumer_worker._commit_queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_handle_message_deserialization_error(self, consumer_worker, caplog, valid_message):
        """Test handling a message with deserialization error."""
        # Arrange: simulate deserialization error
        consumer_worker.deserialize_message = Mock(side_effect=Exception("Deserialization error"))

        # Act: handle the message
        await consumer_worker._handle_message(valid_message)

        # Assert: check that the error is logged and the commit queue is not empty
        assert "Failed to process message" in caplog.text
        assert not consumer_worker._commit_queue.empty()

    def test_on_assign_with_invalid_partitions(self, consumer_worker, mock_kafka_consumer):
        """Test that invalid partitions are not assigned."""
        # Arrange: mock the partition validation and invalid partitions
        test_partitions = [MagicMock()]
        consumer_worker._validate_partition = lambda p: False

        # Act: call on_assign with invalid partitions
        consumer_worker._on_assign_sync(mock_kafka_consumer, test_partitions)

        # Assert: check that no partitions are assigned
        mock_kafka_consumer.assign.assert_called_once_with([])

    def test_on_assign_revoke_callbacks(self, consumer_worker, mock_kafka_consumer):
        """Test that assign and revoke callbacks are correctly handled."""
        test_partitions = [MagicMock()]

        # Arrange: mock the partition validation to return True
        consumer_worker._validate_partition = lambda p: True

        # Act: call on_assign and on_revoke
        consumer_worker._on_assign_sync(mock_kafka_consumer, test_partitions)
        mock_kafka_consumer.assign.assert_called_once_with(test_partitions)

        consumer_worker._on_revoke_sync(mock_kafka_consumer, test_partitions)
        mock_kafka_consumer.commit.assert_called_once_with(offsets=test_partitions, asynchronous=False)
        mock_kafka_consumer.unassign.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_handler_execution(self, consumer_worker, mock_handler_registry, valid_message):
        """Test async handler execution for a message."""

        async def async_process(event, msg):
            pass

        mock_handler = MagicMock(process=AsyncMock(side_effect=async_process))
        mock_handler_registry.get_handler.return_value = mock_handler

        mock_event = MagicMock()
        consumer_worker.deserialize_message = Mock(return_value=mock_event)

        # Act: handle the message
        await consumer_worker._handle_message(valid_message)

        # Assert: check if async handler was executed
        mock_handler.process.assert_awaited_once_with(mock_event, valid_message)

    @pytest.mark.asyncio
    async def test_sync_handler_execution(self, consumer_worker, mock_handler_registry, valid_message):
        """Test sync handler execution for a message."""

        def sync_process(event, msg):
            pass

        mock_handler = MagicMock(process=Mock(side_effect=sync_process))
        mock_handler_registry.get_handler.return_value = mock_handler

        mock_event = MagicMock()
        consumer_worker.deserialize_message = Mock(return_value=mock_event)

        # Act: handle the message
        await consumer_worker._handle_message(valid_message)

        # Assert: check if sync handler was executed
        mock_handler.process.assert_called_once_with(mock_event, valid_message)
