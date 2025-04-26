"""Integration tests for BaseMessageHandler are defined here."""

import asyncio
import logging
import time
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from confluent_kafka import Consumer, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel

from otteroad.consumer.handlers import BaseMessageHandler


class TestIntegrationBaseMessageHandler:
    @pytest.fixture(scope="module")
    def kafka_admin(self):
        """Fixture to manage Kafka test topics"""
        return AdminClient({"bootstrap.servers": "localhost:9092"})

    @pytest.fixture(scope="module")
    def test_topic(self):
        """Fixture for the test topic name."""
        return "test_integration_topic"

    @pytest.fixture(scope="module")
    def dlq_topic(self):
        """Fixture for the dead-letter queue topic name."""
        return "test_dlq_topic"

    @pytest.fixture(scope="module", autouse=True)
    def setup_kafka(self, kafka_admin, test_topic, dlq_topic):
        """Fixture to create and delete Kafka test topics."""
        # Create topics
        new_topics = [
            NewTopic(test_topic, num_partitions=1, replication_factor=1),
            NewTopic(dlq_topic, num_partitions=1, replication_factor=1),
        ]
        fs = kafka_admin.create_topics(new_topics)
        for _, f in fs.items():
            f.result()

        yield

        # Delete topics after tests
        fs = kafka_admin.delete_topics([test_topic, dlq_topic])
        for _, f in fs.items():
            f.result()

    @pytest.fixture
    def test_event_model(self):
        """Fixture to define the test event model."""

        class TestEvent(BaseModel):
            id: int
            data: str

        return TestEvent

    @pytest.fixture
    def test_handler_cls(self, test_event_model, dlq_topic):
        """Fixture for the custom message handler class."""

        class TestIntegrationHandler(BaseMessageHandler[test_event_model]):
            processed_events = []
            dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})

            async def on_startup(self):
                self.logger.info("Handler startup completed")

            async def on_shutdown(self):
                self.logger.info("Handler shutdown completed")

            async def handle(self, event: test_event_model, ctx: Message):
                """Handle the incoming event"""
                self.processed_events.append(event)
                if event.id == 666:
                    raise ValueError("Simulated error")

            async def handle_error(self, error: Exception, event: test_event_model, ctx: Message):
                """Handle error by producing the event to the DLQ"""
                self.dlq_producer.produce(dlq_topic, value=ctx.value(), headers={"error": str(error)})
                self.dlq_producer.flush()

        return TestIntegrationHandler

    @pytest.fixture(scope="module")
    def kafka_consumer(self, test_topic, dlq_topic):
        """Fixture for Kafka consumer."""
        consumer = Consumer(
            {"bootstrap.servers": "localhost:9092", "group.id": "test-group", "auto.offset.reset": "earliest"}
        )
        consumer.subscribe([test_topic, dlq_topic])
        return consumer

    @pytest_asyncio.fixture
    async def handler(self, test_handler_cls):
        """Fixture to initialize the message handler."""
        handler = test_handler_cls(logger=logging.getLogger("integration_test"))
        await handler.on_startup()
        yield handler
        await handler.on_shutdown()

    @pytest.mark.asyncio
    async def test_full_message_lifecycle(
        self,
        handler,
        test_topic,
        dlq_topic,
        kafka_consumer,
        test_event_model,
    ):
        """Test the full lifecycle of a message: successful processing and DLQ handling on error."""
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        valid_event = test_event_model(id=1, data="valid")
        invalid_event = test_event_model(id=666, data="invalid")

        # Produce valid and invalid events
        for event in [valid_event, invalid_event]:
            producer.produce(test_topic, value=event.model_dump_json().encode("utf-8"))
        producer.flush()

        # Poll and process the messages
        start_time = time.time()
        while len(handler.processed_events) < 2 and time.time() - start_time < 10:
            msg = kafka_consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            event = test_event_model.model_validate_json(msg.value())
            await handler.process(event, msg)

        # Verify valid event is processed and error is sent to DLQ
        assert len(handler.processed_events) == 2
        assert valid_event in handler.processed_events

        dlq_msg = kafka_consumer.poll(5.0)
        assert dlq_msg is not None
        assert b"Simulated error" in dlq_msg.headers()[0][1]

    @pytest.mark.asyncio
    async def test_error_recovery(self, handler, test_topic, kafka_consumer, test_event_model):
        """Test error recovery after an exception during processing."""
        producer = Producer({"bootstrap.servers": "localhost:9092"})

        error_event = test_event_model(id=666, data="error")
        valid_event = test_event_model(id=2, data="recovery")

        # Produce error and valid events
        for event in [error_event, valid_event]:
            producer.produce(test_topic, value=event.model_dump_json().encode())
        producer.flush()

        # Process messages and ensure the valid event is processed after error
        kafka_consumer.subscribe([test_topic])

        processed = []
        start_time = time.time()
        while len(processed) < 2 and time.time() - start_time < 10:
            msg = kafka_consumer.poll(1.0)
            if msg is None:
                continue

            event = test_event_model.model_validate_json(msg.value())
            try:
                await handler.process(event, msg)
                processed.append(event)
            except:
                pass

        assert valid_event in handler.processed_events

    @pytest.mark.asyncio
    async def test_lifecycle_hooks(self, handler, caplog):
        """Test lifecycle hooks logging."""
        caplog.set_level(logging.INFO, logger="integration_test")

        await handler.on_startup()
        await handler.on_shutdown()

        # Verify lifecycle log messages
        assert any("Handler startup completed" in r.message for r in caplog.records)
        assert any("Handler shutdown completed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_concurrent_processing(self, handler, test_topic, test_event_model):
        """Test concurrent message processing."""
        # Generate 500 events
        events = [test_event_model(id=i, data=f"conc_{i}") for i in range(500)]

        # Process events concurrently
        async def process_batch():
            tasks = []
            for event in events:
                msg = MagicMock()
                msg.value.return_value = event.model_dump_json().encode()
                tasks.append(handler.process(event, msg))
            await asyncio.gather(*tasks)

        await process_batch()

        # Verify all events are processed
        assert len(handler.processed_events) == 500
        assert all(e.id in range(500) for e in handler.processed_events)
