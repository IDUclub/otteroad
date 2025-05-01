"""Integration tests for Kafka producer client are defined here."""

import asyncio
import logging
import time
import uuid
from typing import ClassVar

import pytest
import pytest_asyncio
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic import ValidationError

from otteroad.avro import AvroEventModel
from otteroad.producer import KafkaProducerClient
from otteroad.settings import KafkaProducerSettings


class TestIntegrationKafkaProducerClient:
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
        new_topic = NewTopic(test_topic, num_partitions=1, replication_factor=1)
        fs = kafka_admin.create_topics([new_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is created

        yield  # Teardown happens after the test completes

        fs = kafka_admin.delete_topics([test_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is deleted

    @pytest.fixture
    def test_event_model(self):
        """Fixture to provide an example AvroEventModel."""

        class TestEvent(AvroEventModel):
            topic: ClassVar[str] = "test.events"
            namespace: ClassVar[str] = "test_event"
            data: str

        return TestEvent

    @pytest_asyncio.fixture
    async def producer_client(self, caplog):
        """Fixture to provide an async KafkaProducerClient instance."""
        caplog.set_level(logging.DEBUG)
        settings = KafkaProducerSettings(bootstrap_servers="localhost:9092", acks="all")
        async with KafkaProducerClient(settings, logger=logging.getLogger()) as client:
            yield client

    @pytest.mark.asyncio
    async def test_send_and_receive_message(self, test_topic, test_event_model, producer_client):
        """Test sending and receiving a Kafka message."""
        # Send a message to Kafka
        event = test_event_model(data="integration-test")
        await producer_client.send(event, topic=test_topic)

        # Read the message from Kafka
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"test-group-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])

        msg = consumer.poll(10.0)
        assert msg is not None  # Assert message is received
        serialized = await asyncio.to_thread(producer_client.serialize_message, event)
        assert msg.value() == serialized  # Assert message content matches the event
        consumer.close()

    @pytest.mark.asyncio
    async def test_schema_registration(self, test_topic, test_event_model, producer_client, schema_registry):
        """Test schema registration and retrieval from the Schema Registry."""
        await producer_client.send(test_event_model(data="schema-test"), topic=test_topic)

        # Retrieve schema from the registry
        latest_schema = schema_registry.get_latest_version(test_event_model.schema_subject())
        assert latest_schema is not None
        assert "data" in latest_schema.schema.schema_str  # Assert field is present in schema

    @pytest.mark.asyncio
    async def test_delivery_timeout(self, test_topic, test_event_model, producer_client):
        """Test message delivery timeout."""
        invalid_settings = KafkaProducerSettings(bootstrap_servers="invalid:9092")
        client = KafkaProducerClient(invalid_settings)
        await client.__aenter__()

        try:
            await client.send(test_event_model(data="timeout-test"), topic=test_topic, timeout=0.5)
        except RuntimeError as exc:
            assert "timeout" in str(exc).lower()  # Assert timeout error occurs
        finally:
            try:
                await client.__aexit__(None, None, None)
            except RuntimeError as exc:
                assert "flush timeout" in str(exc).lower()  # Assert timeout during flush

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, test_topic, test_event_model, producer_client):
        """Test graceful shutdown of producer client."""
        await producer_client.send(test_event_model(data="shutdown-test"), topic=test_topic)

        # Check if message is delivered after shutdown
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"test-group-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])

        msg = consumer.poll(10.0)
        assert msg is not None  # Assert message is received
        consumer.close()

    @pytest.mark.asyncio
    async def test_high_throughput(self, test_topic, test_event_model, producer_client):
        """Test high throughput by sending multiple messages."""
        for i in range(1000):
            await producer_client.send(test_event_model(data=f"message-{i}"), topic=test_topic)

        # Check if all messages are delivered
        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"test-group-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])

        messages = []
        start_time = time.time()
        while len(messages) < 1000 and time.time() - start_time < 30:
            msg = consumer.poll(1.0)
            if msg:
                messages.append(msg)

        assert len(messages) == 1000  # Assert all messages are received
        consumer.close()

    @pytest.mark.asyncio
    async def test_error_handling(self, test_topic, producer_client, caplog):
        """Test error handling when sending an invalid message."""

        class InvalidEvent(AvroEventModel):
            topic: ClassVar[str] = "invalid.events"
            namespace: ClassVar[str] = "invalid_event"
            number: int

        with pytest.raises(ValidationError):
            await producer_client.send(InvalidEvent(number="not-a-number"), topic=test_topic)

    @pytest.mark.asyncio
    async def test_header_support(self, test_topic, test_event_model, producer_client):
        """Test support for message headers."""
        headers = {"trace-id": b"12345", "source": b"test"}

        await producer_client.send(
            test_event_model(data="header-test"),
            topic=test_topic,
            headers=headers,
        )

        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"test-group-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])

        found_msg = None
        timeout = 10.0
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.headers() is not None:
                found_msg = msg
                break

        consumer.close()

        assert found_msg is not None, "Message with unique key not found"
        assert dict(found_msg.headers()) == headers  # Assert headers are correct
