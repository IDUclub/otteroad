"""Unit tests for Kafka consumer worker are defined here."""

import asyncio
import threading
import time
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
import pytest_asyncio
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
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

    def test_init_without_event_loop(self, monkeypatch, mock_schema_registry, mock_handler_registry):
        """Test initializing consumer with invalid loop."""
        monkeypatch.setattr(asyncio, "get_running_loop", Mock(side_effect=RuntimeError("no loop")))
        with pytest.raises(RuntimeError, match="No event loop provided and not running in async context"):
            KafkaConsumerWorker(
                consumer_config={"bootstrap.servers": "localhost:9092", "group.id": "dummy"},
                schema_registry=mock_schema_registry,
                handler_registry=mock_handler_registry,
                topics=["test-topic"],
            )

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

    def test_poll_loop_handles_errors(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test handling errors in poll loop."""
        error_msg = MagicMock()
        error_msg.error.return_value = True
        mock_kafka_consumer.poll.side_effect = [error_msg, Exception("Poll exception")]

        consumer_worker._cancelled.clear()
        consumer_worker._poll_loop()

        assert "Consumer error" in caplog.text
        assert "Critical error in poll loop" in caplog.text

    def test_process_commits_handles_errors(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test handling errors in process commits."""
        msg = MagicMock()
        consumer_worker._commit_queue.put(msg)
        mock_kafka_consumer.commit.side_effect = Exception("Commit exception")

        consumer_worker._process_commits()

        assert "Commit error" in caplog.text

    @pytest.mark.asyncio
    async def test_process_loop_handles_errors(self, consumer_worker, valid_message, caplog):
        """Test handling errors in process loop."""
        consumer_worker._queue.put_nowait(valid_message)
        consumer_worker._handle_message = AsyncMock(side_effect=Exception("Processing error"))

        task = asyncio.create_task(consumer_worker._process_loop())
        await asyncio.sleep(0.1)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        assert "Processing error" in caplog.text

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
        assert consumer_worker._commit_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_message_skips_none_event(self, consumer_worker, valid_message):
        """Test handling a message with skips none event."""
        consumer_worker.deserialize_message = MagicMock(return_value=None)
        consumer_worker._handler_registry.get_handler = MagicMock()
        consumer_worker._settings["enable.auto.commit"] = False

        await consumer_worker._handle_message(valid_message)

        consumer_worker._handler_registry.get_handler.assert_not_called()

        assert not consumer_worker._commit_queue.empty()
        assert consumer_worker._commit_queue.get_nowait() == valid_message

    def test_shutdown_consumer_handles_errors(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test handling error while consumer shutdown."""
        mock_kafka_consumer.close.side_effect = Exception("Close exception")

        consumer_worker._shutdown_consumer()

        assert "Error closing consumer" in caplog.text

    @pytest.mark.asyncio
    async def test_stop_warns_if_poll_thread_alive(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test non-gracefully stopping."""
        consumer_worker._poll_thread = threading.Thread(target=lambda: time.sleep(2))
        consumer_worker._poll_thread.start()

        await consumer_worker.stop(timeout=0.1)

        assert "Poll thread did not exit gracefully" in caplog.text

    def test_on_assign_with_invalid_partitions(self, consumer_worker, mock_kafka_consumer):
        """Test that invalid partitions are not assigned."""
        # Arrange: mock the partition validation and invalid partitions
        test_partitions = [MagicMock()]
        consumer_worker._validate_partition = lambda p: False

        # Act: call on_assign with invalid partitions
        consumer_worker._on_assign_sync(mock_kafka_consumer, test_partitions)

        # Assert: check that no partitions are assigned
        mock_kafka_consumer.assign.assert_called_once_with([])

    def test_on_assign_sync_handles_kafka_errors(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test handling errors on assign."""
        for error_code, log_message in [
            (KafkaError._ALL_BROKERS_DOWN, "All brokers unavailable"),
            (KafkaError._UNKNOWN_TOPIC, "Topic does not exist"),
            (KafkaError._UNKNOWN_PARTITION, "Invalid partitions"),
            (KafkaError._FATAL, "Kafka error during assignment"),
        ]:
            kafka_error = KafkaError(error_code)
            kafka_exception = KafkaException(kafka_error)
            mock_kafka_consumer.assign.side_effect = kafka_exception

            consumer_worker._on_assign_sync(mock_kafka_consumer, [MagicMock()])

            assert log_message in caplog.text
            mock_kafka_consumer.assign.side_effect = None

    def test_on_revoke_sync_handles_errors(self, consumer_worker, mock_kafka_consumer, caplog):
        """Test handling errors on revoke."""
        kafka_error = KafkaError(KafkaError._NO_OFFSET)
        kafka_exception = KafkaException(kafka_error)
        mock_kafka_consumer.commit.side_effect = kafka_exception

        consumer_worker._on_revoke_sync(mock_kafka_consumer, [MagicMock()])

        assert "No offsets to commit for partitions" in caplog.text

        mock_kafka_consumer.commit.side_effect = Exception("Commit exception")
        consumer_worker._on_revoke_sync(mock_kafka_consumer, [MagicMock()])

        assert "Unexpected error during revoke" in caplog.text

    def test_on_revoke_sync_logs_commit_error(self, mock_kafka_consumer, consumer_worker, caplog):
        partitions = [MagicMock()]
        kafka_error = MagicMock()
        kafka_error.code.return_value = KafkaError.UNKNOWN  # не _NO_OFFSET

        mock_kafka_consumer.commit.side_effect = KafkaException(kafka_error)

        consumer_worker._on_revoke_sync(mock_kafka_consumer, partitions)

        assert "Commit error on revoke" in caplog.text

    def test_on_revoke_sync_logs_error_if_unassign_fails(self, mock_kafka_consumer, consumer_worker, caplog):
        partitions = [MagicMock()]
        mock_kafka_consumer.unassign.side_effect = Exception("unassign failed")

        consumer_worker._on_revoke_sync(mock_kafka_consumer, partitions)

        assert "Error unassigning partitions" in caplog.text

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

    def test_on_assign_sync_warns_and_unassigns_if_no_assignment_after_assign(
        self, mock_kafka_consumer, consumer_worker, caplog
    ):
        partitions = [MagicMock()]
        valid_partitions = partitions

        mock_kafka_consumer.assignment.return_value = []  # симулируем, что assign ничего не назначил

        consumer_worker._validate_partition = lambda p: True
        consumer_worker._on_assign_sync(mock_kafka_consumer, partitions)

        mock_kafka_consumer.assign.assert_called_once_with(valid_partitions)
        mock_kafka_consumer.unassign.assert_called_once()
        assert "Consumer has no assigned partitions after assignment attempt" in caplog.text

    def test_on_assign_sync_logs_error_if_assignment_check_fails(self, mock_kafka_consumer, consumer_worker, caplog):
        partitions = [MagicMock()]

        consumer_worker._validate_partition = lambda p: True
        mock_kafka_consumer.assignment.side_effect = Exception("assignment failure")

        consumer_worker._on_assign_sync(mock_kafka_consumer, partitions)

        assert "Failed to reset consumer state" in caplog.text

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
