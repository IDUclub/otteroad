"""librdkafka logging settings of the producer and the consumer, checked against confluent-kafka itself."""

import logging

import pytest
from confluent_kafka import Consumer, Producer

from otteroad.utils import LoggerAdapter
from otteroad.utils.logger import kafka_logger_config


@pytest.mark.parametrize("disable", [True, False])
def test_confluent_kafka_accepts_the_log_settings(disable):
    """librdkafka accepts log_level 0..7 only: logging.INFO (20) made the clients fail to start"""
    config = kafka_logger_config(LoggerAdapter(logging.getLogger("otteroad.test")), disable)
    assert config["log_level"] == (0 if disable else 6)
    Producer({"bootstrap.servers": "127.0.0.1:1"} | config).flush(0)
    Consumer({"bootstrap.servers": "127.0.0.1:1", "group.id": "otteroad-test"} | config).close()


def test_internal_kafka_logs_reach_the_adapter(caplog):
    """confluent-kafka calls logger.log(level, fmt, *args) from poll()"""
    adapter = LoggerAdapter(logging.getLogger("otteroad.test"))
    config = kafka_logger_config(adapter, disable_internal_kafka_logs=False) | {"log_level": 7, "debug": "broker"}
    producer = Producer({"bootstrap.servers": "127.0.0.1:1"} | config)
    with caplog.at_level(logging.DEBUG, logger="otteroad.test"):
        producer.poll(1)
    assert any("rdkafka#producer" in record.getMessage() for record in caplog.records)
