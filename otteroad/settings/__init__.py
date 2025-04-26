"""
This package provides robust, Pydantic-based configuration models for Kafka clients, enabling:

- Typed validation of consumer and producer settings
- Loading from environment variables, .env files, YAML, and custom objects
- Automatic field validation and transformation
- Support for Avro Schema Registry configuration
- Consumer and producer-specific tuning (offsets, batching, idempotence, transactions, etc.)

Usage Example:

    from otteroad.settings import KafkaConsumerSettings, KafkaProducerSettings

    # Load consumer settings from environment
    consumer_settings = KafkaConsumerSettings.from_env(env_prefix="KAFKA_")

    # Load producer settings from YAML file
    producer_settings = KafkaProducerSettings.from_yaml("config.yaml", key="kafka")

    # Access typed values
    bootstrap = consumer_settings.bootstrap_servers
    acks = producer_settings.acks

Exports:
    - KafkaConsumerSettings: Consumer-specific Pydantic settings model
    - KafkaProducerSettings: Producer-specific Pydantic settings model
"""

from .consumer import KafkaConsumerSettings
from .producer import KafkaProducerSettings

__all__ = [
    "KafkaConsumerSettings",
    "KafkaProducerSettings",
]
