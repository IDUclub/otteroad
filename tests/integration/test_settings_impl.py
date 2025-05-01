"""Integration tests for Kafka settings classes are defined here."""

import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import ValidationError

from otteroad.settings import KafkaConsumerSettings, KafkaProducerSettings


class TestIntegrationKafkaSettings:
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
        new_topic = NewTopic(test_topic, num_partitions=1, replication_factor=1)
        fs = kafka_admin.create_topics([new_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is created

        yield  # Teardown happens after the test completes

        fs = kafka_admin.delete_topics([test_topic])
        for _, f in fs.items():
            f.result()  # Ensure the topic is deleted

    def test_consumer_group_management(self, test_topic):
        """Test for managing consumer group using KafkaConsumerSettings."""
        group_id = f"test-group-{uuid.uuid4().hex}"
        settings = KafkaConsumerSettings(
            group_id=group_id, bootstrap_servers="localhost:9092", auto_offset_reset="earliest"
        )

        # Create and subscribe a consumer to the test topic
        consumer = Consumer(settings.get_config())
        consumer.subscribe([test_topic])

        # Check that the consumer has joined the group and topic is available
        time.sleep(1)
        metadata = consumer.list_topics()
        assert test_topic in metadata.topics
        consumer.close()

    def test_producer_delivery_guarantees(self, test_topic):
        """Test to ensure producer delivery guarantees with idempotence and transactions."""
        settings = KafkaProducerSettings(
            enable_idempotence=True,
            acks="all",
            max_in_flight=1,
            transactional_id=f"tx-{uuid.uuid4().hex}",
        )

        # Create a producer and initialize transactions
        producer = Producer(settings.get_config())
        producer.init_transactions()

        # Consumer to check the delivery of messages
        consumer = Consumer(
            {"bootstrap.servers": "localhost:9092", "group.id": "delivery-check", "auto.offset.reset": "earliest"}
        )
        consumer.subscribe([test_topic])

        # Send and commit a message in a transaction
        producer.begin_transaction()
        producer.produce(test_topic, value=b"test")
        producer.commit_transaction()

        producer.flush()

        # Check that the message was successfully delivered
        msg = consumer.poll(5.0)
        assert msg is not None
        assert msg.value() == b"test"
        consumer.close()

    def test_consumer_offset_management(self, test_topic):
        """Test for managing consumer offsets."""
        group_id = f"offset-group-{uuid.uuid4().hex}"
        settings = KafkaConsumerSettings(
            group_id=group_id,
            enable_auto_commit=True,
            auto_commit_interval_ms=500,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
        )

        consumer = Consumer(settings.get_config())
        consumer.subscribe([test_topic])
        consumer.poll(0)  # Join the consumer group

        def drain_consumer(consumer, timeout=0.5):
            """Drains all the old messages until the end."""
            while True:
                msg = consumer.poll(timeout)
                if msg is None:
                    break

        drain_consumer(consumer)

        producer = Producer({"bootstrap.servers": "localhost:9092"})
        producer.produce(test_topic, value=b"offset-test")
        producer.flush()

        # Wait for the expected message
        for _ in range(10):
            msg = consumer.poll(1.0)
            if msg and msg.value() == b"offset-test":
                break
        else:
            assert False, "Expected message 'offset-test' not received"

        # Check the offset commit
        time.sleep(1)  # Wait for auto commit
        tp = TopicPartition(test_topic, 0)
        offsets = consumer.committed([tp])
        assert offsets[0].offset >= msg.offset() + 1
        consumer.close()

    def test_invalid_configuration_handling(self):
        """Test for handling invalid producer and consumer configurations."""
        # Invalid producer configuration test
        with pytest.raises(ValidationError):
            KafkaProducerSettings(enable_idempotence=True, acks="1")  # "acks" should be "all"

        # Invalid consumer configuration test
        with pytest.raises(ValidationError):
            KafkaConsumerSettings(group_id="invalid-test", enable_auto_commit=True, auto_commit_interval_ms=0)

    def test_custom_properties_integration(self, test_topic):
        """Test for custom properties in producer configuration."""
        custom_settings = KafkaProducerSettings(
            bootstrap_servers="localhost:9092",
            custom_properties={"linger.ms": 500, "message.timeout.ms": 2000},
        )

        consumer = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"check-custom-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.subscribe([test_topic])
        consumer.poll(0)  # Join the consumer group

        def drain_consumer(consumer, timeout=0.5):
            """Drains all old messages."""
            while True:
                msg = consumer.poll(timeout)
                if msg is None:
                    break

        drain_consumer(consumer)

        producer = Producer(custom_settings.get_config())
        producer.produce(test_topic, value=b"custom-props")
        producer.flush()

        # Check that the message was delivered with custom properties
        for _ in range(10):
            msg = consumer.poll(1.0)
            if msg and msg.value() == b"custom-props":
                break
        else:
            assert False, "Expected message 'custom-props' not received"

        consumer.close()

    def test_schema_registry_integration(self):
        """Test for schema registry integration and auto registration."""
        settings = KafkaProducerSettings(
            schema_registry_url="http://localhost:8081", schema_registry_auto_register_schemas=True
        )

        # Create a schema registry client
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr_client = SchemaRegistryClient({"url": settings.schema_registry_url})

        # Check the schema registry connection
        subjects = sr_client.get_subjects()
        assert isinstance(subjects, list)
