"""Integration tests for Kafka consumer service are defined here."""

import asyncio
import logging
import uuid
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from otteroad import (
    BaseMessageHandler,
    KafkaConsumerService,
    KafkaConsumerSettings,
)
from otteroad.avro import AvroEventModel


class TestIntegrationKafkaConsumerService:
    @pytest.fixture(scope="module")
    def kafka_admin(self):
        """Fixture to provide Kafka Admin Client."""
        return AdminClient({"bootstrap.servers": "localhost:9092"})

    @pytest.fixture(scope="module")
    def test_topic(self):
        """Fixture to create a unique test Kafka topic."""
        return f"test-topic-{uuid.uuid4().hex}"

    @pytest.fixture(scope="module", autouse=True)
    def setup_kafka(self, kafka_admin, test_topic):
        """Fixture to setup and teardown Kafka topics."""
        new_topic = NewTopic(test_topic, num_partitions=2, replication_factor=1)
        fs = kafka_admin.create_topics([new_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is created

        yield  # Teardown happens after the test completes

        fs = kafka_admin.delete_topics([test_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is deleted

    @pytest.fixture
    def test_event_model(self):
        """Fixture for defining a test event model."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.events"
            namespace: ClassVar[str] = "test_event_service"
            data: str

        return TestEvent

    @pytest.fixture
    def mock_handler(self, test_event_model):
        """Fixture for creating a mock message handler."""
        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = test_event_model
        handler.process = AsyncMock()
        return handler

    @pytest_asyncio.fixture
    async def consumer_service(self, test_topic, caplog):
        """Fixture for Kafka consumer service."""
        caplog.set_level(logging.DEBUG)
        settings = KafkaConsumerSettings(
            bootstrap_servers="localhost:9092",
            group_id=f"test-group-{uuid.uuid4().hex}",
            auto_offset_reset="earliest",
            schema_registry_url="http://localhost:8081",
        )

        # Initialize the consumer service and add a worker for the test topic
        service = KafkaConsumerService(settings, logger=logging.getLogger())
        service.add_worker(test_topic)
        yield service
        await service.stop()

    @pytest.mark.asyncio
    async def test_end_to_end_message_processing(self, test_topic, test_event_model, mock_handler, consumer_service):
        """Test for end-to-end message processing."""
        consumer_service.register_handler(mock_handler)

        # Start the consumer service
        await consumer_service.start()

        # Produce a test message and serialize it
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        event = test_event_model(data="payload")
        serialized = event.serialize(consumer_service._schema_registry)
        producer.produce(test_topic, value=serialized)
        producer.flush()

        # Wait for message processing
        await asyncio.sleep(5)

        # Ensure that the handler was called and the processed data is correct
        mock_handler.process.assert_awaited_once()
        processed_event = mock_handler.process.call_args[0][0]
        assert processed_event.data == "payload"

    @pytest.mark.asyncio
    async def test_multiple_workers_rebalance(self, test_topic, consumer_service, test_event_model):
        """Test for Kafka rebalance and handling multiple workers."""
        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = test_event_model

        # Fake handler for simulating processing
        async def fake_handler(*args, **kwargs):
            await asyncio.sleep(1)

        handler.process = fake_handler

        consumer_service.register_handler(handler)
        consumer_service.add_worker(test_topic)

        # Start the consumer service
        await consumer_service.start()

        # Ensure that there are two workers
        workers = consumer_service._workers
        assert len(workers) == 2

        # Produce messages to both partitions
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        event = test_event_model(data="payload")
        serialized = event.serialize(consumer_service._schema_registry)
        for _ in range(10):
            producer.produce(test_topic, value=serialized, partition=0)
            producer.produce(test_topic, value=serialized, partition=1)
        producer.flush()

        await asyncio.sleep(10)

        # Check that the workers have messages to process
        assert any(w._queue.qsize() > 0 for w in consumer_service._workers)

    @pytest.mark.asyncio
    async def test_graceful_shutdown_with_pending_messages(self, test_topic, consumer_service, test_event_model):
        """Test for graceful shutdown of the service while processing messages."""
        await consumer_service.start()

        # Produce a large number of messages
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        event = test_event_model(data="payload")
        serialized = event.serialize(consumer_service._schema_registry)
        for _ in range(1000):
            producer.produce(test_topic, value=serialized)
        producer.flush()

        await asyncio.sleep(1.0)

        # Immediately stop the consumer service
        await consumer_service.stop()

        # Ensure that all worker tasks have been completed
        for worker in consumer_service._workers:
            assert worker._process_task.done()

    @pytest.mark.asyncio
    async def test_schema_evolution_handling(self, test_topic, consumer_service):
        """Test schema evolution and handling new schema versions."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.events"
            namespace: ClassVar[str] = "test_event_service"
            schema_version: ClassVar[int] = 2
            data: str
            new_field: int = 0

        handler = MagicMock(spec=BaseMessageHandler)
        handler.event_type = TestEvent
        handler.process = AsyncMock()

        # Register the new schema version
        TestEvent.register_schema(consumer_service._schema_registry)
        consumer_service.register_handler(handler)
        await consumer_service.start()

        # Produce a message with the new schema version
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        event = TestEvent(data="test", new_field=42)
        producer.produce(test_topic, value=event.serialize(consumer_service._schema_registry))
        producer.flush()

        # Wait for processing and verify handler was called
        await asyncio.sleep(10)
        assert handler.process.call_count == 1

    @pytest.mark.asyncio
    async def test_error_resilience(self, test_topic, consumer_service, mock_handler, caplog):
        """Test resilience of the service when handling erroneous messages."""
        consumer_service.register_handler(mock_handler)
        await consumer_service.start()

        # Produce an invalid message
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        producer.produce(test_topic, value=b"invalid-avro")
        producer.flush()

        # Verify that the error was logged
        await asyncio.sleep(10)
        assert "Failed to process message" in caplog.text
        assert "Deserialization error" in caplog.text

        # Ensure service continues running
        assert all(not w._cancelled.is_set() for w in consumer_service._workers)
