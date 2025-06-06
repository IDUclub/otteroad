"""Integration tests for Kafka consumer worker are defined here."""

import asyncio
import logging
import time
import uuid
from typing import ClassVar

import pytest
import pytest_asyncio
from confluent_kafka import Consumer, Message, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad import BaseMessageHandler, KafkaConsumerSettings
from otteroad.avro import AvroEventModel
from otteroad.consumer import EventHandlerRegistry, KafkaConsumerWorker


class TestIntegrationKafkaConsumerWorker:
    @pytest.fixture(scope="module")
    def kafka_admin(self):
        """Fixture to provide Kafka Admin Client."""
        return AdminClient({"bootstrap.servers": "localhost:9092"})

    @pytest.fixture(scope="module")
    def schema_registry(self):
        """Fixture to provide a schema registry client for testing."""
        return SchemaRegistryClient({"url": "http://localhost:8081"})

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
    def test_event_model(self, test_topic):
        """Fixture to define an Avro event model for testing."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = test_topic
            namespace: ClassVar[str] = "test_event"
            data: str

        return TestEvent

    @pytest.fixture
    def handler_instance(self, test_event_model):
        """Fixture to create a handler instance for processing events."""

        class SimpleTestHandler(BaseMessageHandler[test_event_model]):
            def __init__(self):
                super().__init__()
                self.received_events = []

            async def handle(self, event: AvroEventModel, ctx: Message) -> None:
                self.received_events.append(event)

            async def on_startup(self):
                pass

            async def on_shutdown(self):
                pass

        return SimpleTestHandler()

    @pytest.fixture
    def handler_registry(self, handler_instance):
        """Fixture to register event handlers in a handler registry."""
        registry = EventHandlerRegistry()
        registry.register(handler_instance)
        return registry

    @pytest.fixture
    def consumer_settings(self):
        """Fixture to define Kafka consumer settings for testing."""
        return KafkaConsumerSettings(
            bootstrap_servers="localhost:9092",
            group_id=f"test-group-{uuid.uuid4().hex}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

    @pytest_asyncio.fixture
    async def consumer_worker(self, schema_registry, handler_registry, consumer_settings, test_topic, caplog):
        """Fixture to initialize and start a Kafka consumer worker for processing messages."""
        caplog.set_level(logging.DEBUG)
        worker = KafkaConsumerWorker(
            consumer_config=consumer_settings.get_config(),
            schema_registry=schema_registry,
            handler_registry=handler_registry,
            topics=[test_topic],
            logger=logging.getLogger(),
        )
        await worker.start()
        yield worker
        await worker.stop()

    @pytest.mark.asyncio
    async def test_message_processing_flow(self, test_topic, test_event_model, handler_instance, consumer_worker):
        """Test end-to-end message processing through the consumer worker."""
        # Prepare a test event
        test_data = {"data": "integration-test"}
        event = test_event_model(**test_data)

        # Serialize and send the message
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        serialized = event.serialize(consumer_worker.schema_registry)
        producer.produce(test_topic, value=serialized)
        producer.flush()

        # Wait for processing
        await asyncio.sleep(10)

        # Check if handler was invoked
        assert len(handler_instance.received_events) == 1
        received = handler_instance.received_events[0]
        assert isinstance(received, test_event_model)
        assert received.data == "integration-test"

    @pytest.mark.asyncio
    async def test_offset_committing(self, test_topic, consumer_worker, test_event_model):
        """Test offset committing after processing a message."""
        # Check if the consumer has committed offset
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": consumer_worker._settings["group.id"],
                "auto.offset.reset": "earliest",
            }
        )
        tp = TopicPartition(test_topic, 0)
        committed = consumer.committed([tp])
        assert committed[0].offset < 0

        # Send a test message
        test_data = {"data": "test-commit"}
        event = test_event_model(**test_data)
        serialized = event.serialize(consumer_worker.schema_registry)
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        producer.produce(test_topic, value=serialized)
        producer.flush()

        # Wait for processing
        await asyncio.sleep(5)

        # Get topic metadata and check for committed offsets
        metadata = consumer.list_topics(timeout=10)
        partitions = metadata.topics[test_topic].partitions.keys()
        committed = consumer.committed([TopicPartition(test_topic, p) for p in partitions])

        # Ensure at least one partition has a committed offset >= 1
        assert any(tp.offset >= 1 for tp in committed)
        consumer.close()

    @pytest.mark.asyncio
    async def test_error_handling(self, test_topic, consumer_worker, caplog):
        """Test error handling when a message cannot be processed."""
        # Send an invalid message
        producer = Producer({"bootstrap.servers": "localhost:9092"})
        producer.produce(test_topic, value=b"invalid-message")
        producer.flush()

        # Wait for error handling
        await asyncio.sleep(5)

        # Check logs for error messages
        assert "Invalid magic byte" in caplog.text

    @pytest.mark.asyncio
    async def test_rebalance_handling(self, test_topic, consumer_worker, caplog):
        """Test consumer rebalance handling."""
        # Simulate rebalance
        test_consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": consumer_worker._settings["group.id"],
                "auto.offset.reset": "latest",
            }
        )

        test_consumer.subscribe([test_topic])
        await asyncio.sleep(15)  # Wait for rebalance

        # Check logs for partition assignment
        assert "Assigned partitions" in caplog.text
        test_consumer.close()

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, consumer_worker):
        """Test graceful shutdown of the consumer worker."""
        # Stop the consumer worker
        await consumer_worker.stop()
        assert consumer_worker._poll_thread is None
        assert consumer_worker._process_task.done()

    @pytest.mark.asyncio
    async def test_throughput_performance(self, test_topic, consumer_worker):
        """Performance test to measure throughput."""
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        # Generate 1000 messages
        for i in range(1000):
            producer.produce(test_topic, value=f"message-{i}".encode())
        producer.flush()

        # Wait for message processing
        start_time = time.time()
        while consumer_worker._queue.qsize() > 0 and time.time() - start_time < 10:
            await asyncio.sleep(0.1)

        # Ensure all messages were processed
        assert consumer_worker._queue.qsize() == 0
