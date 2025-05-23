"""
Provides a service layer for managing multiple Kafka consumer workers with:
- Dynamic worker creation
- Handler registration system
- Schema registry integration
- Lifecycle management for consumer workers
"""

import logging
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad.avro.model import AvroEventModel
from otteroad.consumer.handlers.base import BaseMessageHandler
from otteroad.consumer.handlers.registry import EventHandlerRegistry
from otteroad.consumer.worker import KafkaConsumerWorker
from otteroad.settings import KafkaConsumerSettings
from otteroad.utils import LoggerAdapter, LoggerProtocol


class KafkaConsumerService:
    """
    Orchestration service for managing multiple Kafka consumer workers.

    Features:
    - Worker lifecycle management
    - Schema registry configuration
    - Event handler registration
    - Thread-safe operations

    Args:
        consumer_settings: Configuration settings for Kafka consumers
        logger: Custom logger instance (default: module-level logger)
    """

    def __init__(
        self,
        consumer_settings: KafkaConsumerSettings,
        logger: LoggerProtocol | None = None,
        disable_internal_kafka_logs: bool = False,
    ):
        self._logger = LoggerAdapter(logger or logging.getLogger(__name__))
        self._logger_config = {"logger": self._logger, "log_level": logging.INFO if disable_internal_kafka_logs else 0}
        self._settings = consumer_settings
        self._handler_registry = EventHandlerRegistry()
        self._workers: list[KafkaConsumerWorker] = []
        self._schema_registry = SchemaRegistryClient(self._settings.get_schema_registry_config())

    def add_worker(
        self, topics: str | list[str], consumer_settings: dict[str, Any] | None = None
    ) -> "KafkaConsumerService":
        """
        Create and register a new consumer worker instance.

        Args:
            topics: Kafka topic(s) to subscribe to
            consumer_settings: Additional consumer configuration overrides

        Returns:
            KafkaConsumerService: Self instance with added consumer worker

        Raises:
            ValueError: If no topics are provided
        """
        # Normalize topics to list format
        topic_list = [topics] if isinstance(topics, str) else topics
        if not topic_list:
            raise ValueError("At least one topic must be specified")

        # Merge base config with custom settings
        config = self._settings.get_config().copy()
        if consumer_settings:
            config.update(consumer_settings)
        config.update(self._logger_config)

        # Create and register worker
        worker = KafkaConsumerWorker(
            consumer_config=config,
            schema_registry=self._schema_registry,
            handler_registry=self._handler_registry,
            topics=topic_list,
            logger=self._logger,
        )

        self._workers.append(worker)
        self._logger.info("Created worker for topics", topics=", ".join(topic_list))

        return self

    async def start(self) -> None:
        """Start all registered consumer workers."""
        self._logger.info("Initializing consumer workers", num_workers=len(self._workers))
        for worker in self._workers:
            await worker.start()
        self._logger.debug("All workers started successfully")

    async def stop(self) -> None:
        """Gracefully shutdown all workers with cleanup."""
        self._logger.info("Initiating consumer service shutdown")
        for worker in self._workers:
            await worker.stop()
        self._workers.clear()
        self._logger.info("Shutdown completed successfully")

    def register_handler(self, handler: BaseMessageHandler) -> None:
        """
        Register an event handler in the global registry.

        Args:
            handler: Message handler instance to register
        """
        self._handler_registry.register(handler)
        self._logger.info("Registered handler for event", event_type=handler.event_type.__name__)

    def unregister_handler(self, event_type: type[AvroEventModel] | str) -> None:
        """
        Remove handler registration for a specific event type.

        Args:
            event_type: Event class or schema name to unregister
        """
        self._handler_registry.unregister(event_type)
        type_name = event_type.__name__ if isinstance(event_type, type) else str(event_type)
        self._logger.info("Unregistered handler", event_type=type_name)
