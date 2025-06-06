"""
This module provides a high-performance, thread-safe Kafka producer client with:
- Async/Await interface
- Avro message serialization
- Background polling thread
- Graceful shutdown with message flush
- Configurable queue limits
"""

import asyncio
import logging
import threading
from typing import ClassVar, TypeVar

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad.avro.model import AvroEventModel
from otteroad.avro.serializer import AvroSerializerMixin
from otteroad.settings import KafkaProducerSettings
from otteroad.utils import LoggerAdapter, LoggerProtocol

EventModel = TypeVar("EventModel", bound=AvroEventModel)


class KafkaProducerClient(AvroSerializerMixin):
    """
    Async Kafka producer client with integrated Avro serialization.

    Features:
    - Automatic schema registration/retrieval
    - Async message sending with delivery callbacks
    - Configurable message queue limits
    - Graceful shutdown with pending message flush

    Args:
        producer_settings: Configuration settings for Kafka producer
        logger: Custom logger instance (default: module-level logger)
        loop: Explicit asyncio event loop (default: current running loop)

    Attributes:
        _MAX_QUEUE_SIZE: Maximum number of messages in producer queue
        _POLL_INTERVAL: Polling interval in seconds for delivery reports

    Raises:
        RuntimeError: If no running event loop and none provided
    """

    _MAX_QUEUE_SIZE: ClassVar[int] = 10000  # Prevents memory overconsumption
    _POLL_INTERVAL: ClassVar[float] = 0.1  # 10ms polling interval

    def __init__(
        self,
        producer_settings: KafkaProducerSettings,
        logger: LoggerProtocol | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
        disable_internal_kafka_logs: bool = False,
    ):
        """
        Initialize Kafka producer client with schema registry.

        Args:
            producer_settings: Kafka producer configuration settings
            logger: Custom logger implementation
            loop: Asyncio event loop for async operations

        Raises:
            RuntimeError: If no event loop available and not running in async context
        """
        self._logger = LoggerAdapter(logger or logging.getLogger(__name__))
        logger_config = {"logger": self._logger, "log_level": logging.INFO if disable_internal_kafka_logs else 0}

        # Initialize schema registry and serialization
        schema_registry = SchemaRegistryClient(producer_settings.get_schema_registry_config())
        super().__init__(schema_registry_client=schema_registry, logger=logger)

        # Configure Kafka producer with queue limits
        self._settings = producer_settings
        config = producer_settings.get_config()
        config.update(logger_config)
        self._producer = Producer(config)

        # Event loop management
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError as e:
            raise RuntimeError("No event loop provided and not running in async context") from e

        # Thread control
        self._cancelled = threading.Event()
        self._poll_thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        """
        Check if producer is actively running.

        Returns:
            bool: True if polling thread is active, False otherwise
        """
        return self._poll_thread is not None and self._poll_thread.is_alive()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def start(self) -> None:
        """
        Start background polling thread.

        Raises:
            RuntimeError: If called from different event loop than initialized with
        """
        assert self._loop is asyncio.get_running_loop(), "Please, create objects with the same loop as running with"
        assert not self.is_running, "Did you call `start` twice?"

        # Clear any previous cancellation state
        self._cancelled.clear()

        # Start polling in separate daemon thread
        self._poll_thread = threading.Thread(target=self._poll_loop, daemon=True, name="kafka-producer-poll")
        self._poll_thread.start()
        self._logger.info("Producer client started")

    def _poll_loop(self) -> None:
        """Background thread polling for delivery reports."""
        while not self._cancelled.is_set():
            try:
                # Process available delivery reports
                self._producer.poll(self._POLL_INTERVAL)
            except Exception as e:  # pylint: disable=broad-except
                self._logger.error("Polling error", error=repr(e), exc_info=True)
                break

    async def send(  # pylint: disable=missing-raises-doc
        self,
        event: EventModel,
        topic: str | None = None,
        key: bytes | None = None,
        headers: dict[str, bytes] | None = None,
        timeout: float = 30.0,
    ) -> None:
        """
        Send an Avro-serialized message to Kafka.

        Args:
            event: AvroEventModel instance to serialize and send
            topic: Target topic override (default: event.topic)
            key: Optional message key bytes
            headers: Optional message headers
            timeout: Maximum seconds to wait for delivery confirmation

        Raises:
            RuntimeError: If producer not running or delivery timeout
            ValueError: If topic resolution fails
            KafkaException: For message delivery errors
        """
        if not self.is_running:
            raise RuntimeError("Producer must be started before sending messages")

        target_topic = topic or self._resolve_topic(event)

        try:
            future = self._loop.create_future()
            serialized = await asyncio.to_thread(self.serialize_message, event)

            def delivery_handler(err, msg):
                """Handle Kafka delivery report callback."""
                if err:
                    exc = KafkaException(err)
                    self._loop.call_soon_threadsafe(future.set_exception, exc)
                    self._logger.error("Delivery failed", topic=target_topic, error=repr(exc))
                else:
                    self._loop.call_soon_threadsafe(future.set_result, msg)
                    self._logger.debug(
                        "Message delivered", topic=msg.topic(), partition=msg.partition(), offset=msg.offset()
                    )

            # Thread-safe message production
            self._producer.produce(
                topic=target_topic,
                value=serialized,
                key=key,
                headers=headers,
                on_delivery=delivery_handler,
            )

            await asyncio.wait_for(future, timeout)
            self._logger.info("Message successfully sent", topic=target_topic, event_model=type(event).__name__)

        except asyncio.TimeoutError as e:
            self._logger.error("Message delivery timeout", topic=target_topic)
            raise RuntimeError("Message delivery timeout") from e
        except Exception as e:
            self._logger.error("Failed to send message", topic=target_topic, error=repr(e), exc_info=True)
            raise

    @staticmethod
    def _resolve_topic(event: EventModel) -> str:
        """
        Validate and extract topic from event model.

        Args:
            event: AvroEventModel instance

        Returns:
            str: Validated topic name

        Raises:
            ValueError: If event lacks topic attribute
        """
        if not hasattr(event, "topic") or not event.topic:
            raise ValueError(f"Event {type(event).__name__} must have 'topic' attribute if it was not provided")
        return event.topic

    async def flush(self, timeout: float = 30.0) -> None:  # pylint: disable=missing-raises-doc
        """
        Ensure all queued messages are delivered.

        Args:
            timeout: Maximum seconds to wait for flush completion

        Raises:
            RuntimeError: If flush operation times out
        """
        if not self.is_running:
            return

        try:
            # Blocking flush in separate thread with timeout
            await asyncio.wait_for(asyncio.to_thread(self._producer.flush, timeout), timeout=timeout)
            self._logger.debug("All pending messages flushed")
        except asyncio.TimeoutError as e:
            self._logger.warning("Flush operation timed out")
            raise RuntimeError("Flush timeout exceeded") from e
        except Exception as e:
            self._logger.error("Flush failed", error=repr(e), exc_info=True)
            raise

    async def close(self) -> None:
        """
        Gracefully shutdown producer with message flush.

        Ensures:
        - All queued messages are delivered
        - Background thread is stopped
        - Resources are cleaned up
        """
        if not self.is_running:
            return

        self._cancelled.set()

        try:
            # Attempt to deliver remaining messages
            await self.flush()
        finally:
            # Stop polling thread
            if self._poll_thread:
                self._poll_thread.join(timeout=5.0)
                if self._poll_thread.is_alive():
                    self._logger.warning("Poll thread did not terminate gracefully")
                self._poll_thread = None

        self._logger.info("Producer shutdown completed")
