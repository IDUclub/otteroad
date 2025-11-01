"""
This module provides KafkaConsumerWorker, a high-level worker for consuming messages from Kafka topics,
deserializing Avro-encoded payloads, and dispatching them to registered handlers.
"""

import asyncio
import queue
import threading
from typing import Any, ClassVar

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient

from otteroad.avro.serializer import AvroSerializerMixin
from otteroad.consumer.handlers.registry import EventHandlerRegistry
from otteroad.utils import LoggerAdapter


class KafkaConsumerWorker(AvroSerializerMixin):
    """
    Worker to consume Kafka messages, deserialize them into events, and dispatch to handlers.

    Combines a background polling thread with an asyncio processing loop. Handles Avro deserialization,
    handler lookup, error management, and offset committing.

    Args:
        consumer_config (dict[str, Any]): Configuration dictionary for confluent_kafka.Consumer.
        schema_registry (SchemaRegistryClient): Client for interacting with the Confluent Schema Registry.
        handler_registry (EventHandlerRegistry): Registry mapping event types to handlers.
        topics (Union[str, List[str]]): Topic name or list of topic names to subscribe to.
        logger (Optional[LoggerProtocol]): Optional custom logger, defaults to module logger.
        loop (Optional[asyncio.AbstractEventLoop]): Asyncio event loop to schedule tasks on.

    Attributes:
        MAX_QUEUE_SIZE: Maximum number of messages in producer queue
        POLL_INTERVAL: Polling interval in seconds for delivery reports
        PAUSE_THRESHOLD: Maximum number of messages to pause consuming
        RESUME_THRESHOLD: Minimum number of messages to resume consuming
        HANDLE_MESSAGE_ERROR_INTERVAL: Sleeping interval in seconds to retry handling messages

    Raises:
        RuntimeError: If asyncio loop cannot be determined when creating the worker.
    """

    MAX_QUEUE_SIZE: ClassVar[int] = 10000  # Prevents memory overconsumption
    POLL_INTERVAL: ClassVar[float] = 0.1  # 10ms polling interval
    PAUSE_THRESHOLD: ClassVar[int] = 8000  # Max number of messages in queue to pause polling
    RESUME_THRESHOLD: ClassVar[int] = 4000  # Min number of messages in queue to resume polling
    HANDLE_MESSAGE_ERROR_INTERVAL: ClassVar[float] = 60.0  # Interval beetwen retries handling messages

    def __init__(
        self,
        consumer_config: dict[str, Any],
        schema_registry: SchemaRegistryClient,
        handler_registry: EventHandlerRegistry,
        topics: list[str],
        logger: LoggerAdapter | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        """
        Initialize the KafkaConsumerWorker and underlying Avro serializer.

        Args:
            consumer_config: Settings for Kafka consumer initialization.
            schema_registry: Confluent Schema Registry client instance.
            handler_registry: Registry of event type handlers.
            topics: Single topic or list of topics to subscribe to.
            logger: Custom logger implementing LoggerProtocol. Defaults to module logger.
            loop: Asyncio event loop; defaults to current running loop.

        Raises:
            RuntimeError: If no running asyncio loop is available.
        """

        # Use provided logger or module-level logger
        self._logger = logger

        # Initialize base Avro serializer
        super().__init__(schema_registry_client=schema_registry, logger=logger)

        # Copy settings to avoid mutating caller's dict
        self._settings = consumer_config.copy()
        self._handler_registry = handler_registry
        self._topics = topics

        # Event loop management
        try:
            self._loop = loop or asyncio.get_running_loop()
        except RuntimeError as e:
            raise RuntimeError("No event loop provided and not running in async context") from e

        # Create Kafka consumer instance
        self._consumer = Consumer(self._settings)

        # Use asyncio queue to track outstanding tasks
        self._queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self._is_paused = False

        # Threading and task control
        self._cancelled = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._process_task: asyncio.Task | None = None

        # Internal queue to buffer commit requests
        self._commit_queue: queue.Queue[Message] = queue.Queue()

    async def start(self) -> None:
        """
        Start the consumer worker: subscribe, start poll thread and processing task.

        Raises:
            AssertionError: If called outside the worker's event loop.
        """
        assert self._loop is asyncio.get_running_loop(), "Please, create objects with the same loop as running with"
        assert self._poll_thread is None, "Did you call `start` twice?"

        # Clear any previous cancellation state
        self._cancelled.clear()

        # Subscribe to the configured topics
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign_sync,
            on_revoke=self._on_revoke_sync,
        )

        # Start polling in separate daemon thread
        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"kafka-consumer-{'/'.join(self._topics)}",
        )
        self._poll_thread.start()

        # Schedule the asynchronous processing loop
        self._process_task = self._loop.create_task(self._process_loop())
        self._logger.info("Consumer started for topics", topics=self._topics)

    def _enqueue_safely(self, msg: Message) -> None:
        """
        Enqueue a message into the asyncio queue from a background thread.
        Blocks until there is space in the queue.
        """
        try:
            future = asyncio.run_coroutine_threadsafe(self._queue.put(msg), self._loop)
            future.result(timeout=self._settings.get("max.pool.interval.ms", 300000) / 2)
        except TimeoutError:
            self._logger.warning("Message dropped: queue full after timeout")

    def _poll_loop(self) -> None:
        """
        Poll loop running in background thread: fetch messages and enqueue for processing.

        Raises:
            Exception: Logs any unexpected errors then shuts down consumer.
        """
        try:  # pylint: disable=too-many-try-statements
            while not self._cancelled.is_set():
                # First, process any pending commits
                self._process_commits()

                # Poll Kafka with a short timeout
                msg = self._consumer.poll(self.POLL_INTERVAL)
                if msg is None:
                    continue

                if msg.error():
                    self._logger.error("Consumer error", error=msg.error(), exc_info=True)
                    continue

                self._logger.info(
                    "Received message",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )

                # Safely enqueue the message for asynchronous processing
                self._enqueue_safely(msg)

                if not self._is_paused and self._queue.qsize() > self.PAUSE_THRESHOLD:
                    self._logger.warning("Pausing consumer...")
                    assignments = self._consumer.assignment()
                    if assignments:
                        self._consumer.pause(assignments)
                        self._is_paused = True
        except Exception as e:  # pylint: disable=broad-except
            self._logger.error("Critical error in poll loop", error=repr(e), exc_info=True)
        finally:
            # Ensure the consumer is closed on exit
            self._shutdown_consumer()

    def _process_commits(self) -> None:
        """Process buffered commit requests synchronously."""
        while True:
            try:
                msg = self._commit_queue.get_nowait()
                self._consumer.commit(msg, asynchronous=False)
                self._logger.info("Committed offset", offset=msg.offset())
            except queue.Empty:
                break
            except Exception as e:  # pylint: disable=broad-except
                self._logger.error("Commit error", error=repr(e), exc_info=True)

    def _shutdown_consumer(self) -> None:
        """Safely close Kafka consumer instance."""
        try:
            self._consumer.close()
            self._logger.info("Kafka consumer closed")
        except Exception as e:  # pylint: disable=broad-except
            self._logger.error("Error closing consumer", error=repr(e), exc_info=True)

    async def stop(self, timeout: float = 5.0) -> None:
        """
        Stop the worker: signal poll loop, wait for threads and tasks to finish.

        Args:
            timeout (float): Time in seconds to wait for poll thread to join.
        """
        # Signal the poll loop to exit
        self._cancelled.set()

        # Wait for the poll thread to finish
        if self._poll_thread:
            self._poll_thread.join(timeout=timeout)
            if self._poll_thread.is_alive():
                self._logger.warning("Poll thread did not exit gracefully")
            self._poll_thread = None

        # Wait for queued messages to be fully processed
        await self._queue.join()

        # Cancel the processing task
        if self._process_task:
            self._process_task.cancel()
            try:
                await self._process_task
            except asyncio.CancelledError:
                self._logger.debug("Processing task cancelled")

        self._logger.info("Consumer worker stopped")

    async def _process_loop(self) -> None:
        """
        Asyncio loop to process enqueued messages sequentially.

        Raises:
            asyncio.CancelledError: When processing loop is cancelled.
        """
        try:
            while True:
                # Wait for the next message
                msg = await self._queue.get()
                try:
                    await self._handle_message(msg)
                except Exception as e:  # pylint: disable=broad-except
                    self._logger.error("Processing error", error=repr(e), exc_info=True)
                finally:
                    # Mark message done regardless of success/failure
                    self._queue.task_done()

                if self._is_paused and self._queue.qsize() < self.RESUME_THRESHOLD:
                    self._logger.info("Resuming consumer...")
                    assignments = self._consumer.assignment()
                    if assignments:
                        self._consumer.resume(assignments)
                        self._is_paused = False
        except asyncio.CancelledError:
            self._logger.info("Processing loop cancelled")

    async def _handle_message(self, msg: Message) -> None:
        """
        Deserialize message and dispatch to registered handler.

        Args:
            msg (Message): Raw Kafka message.

        Raises:
            Exception: Logs any errors during deserialization or handling.
        """
        attempt = 1
        while True:
            should_commit: bool = False
            try:  # pylint: disable=too-many-try-statements
                # Deserialize payload to event object
                event = self.deserialize_message(msg)
                if event is None:
                    should_commit = True
                    break

                # Lookup handler based on event type or content
                handler = self._handler_registry.get_handler(event)
                if handler is None:
                    self._logger.warning("Handler not found for event", event_model=event)
                    should_commit = True
                    break

                # Execute handler, using thread if synchronous
                if asyncio.iscoroutinefunction(handler.process):
                    await handler.process(event, msg)
                else:
                    await asyncio.to_thread(handler.process, event, msg)

                should_commit = True

                self._logger.info(
                    "Message successfully processed",
                    event_model=type(event).__name__,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )

            except Exception as e:  # pylint: disable=broad-except
                self._logger.error(
                    "Failed to process message",
                    attempt=attempt,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    error=repr(e),
                    exc_info=True,
                )
                attempt += 1
                await asyncio.sleep(self.HANDLE_MESSAGE_ERROR_INTERVAL)

            finally:
                # If auto-commit is disabled, buffer for commit
                if should_commit and not self._settings.get("enable.auto.commit", True):
                    self._commit_queue.put(msg)
                    break  # pylint: disable=lost-exception

    def _validate_partition(self, partition: TopicPartition) -> bool:
        """
        Validate whether the specified partition exists for the given topic.

        Args:
            partition (TopicPartition): The topic and partition to validate.

        Returns:
            bool: True if the partition exists for the topic, False otherwise.
        """
        metadata = self._consumer.list_topics(partition.topic)
        return partition.partition in metadata.topics[partition.topic].partitions

    def _on_assign_sync(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Synchronous callback for partition assignment.
        Handles Kafka-specific errors, validates assignment, and ensures graceful recovery.

        Args:
            consumer (Consumer): Kafka Consumer instance.
            partitions (List[TopicPartition]): List of assigned partitions.
        """
        try:
            self._logger.info(
                "Assigned partitions",
                partitions=[f"{p.topic}:{p.partition}@{p.offset}" for p in partitions],
            )

            # Additional check: partitions exist
            valid_partitions = [p for p in partitions if self._validate_partition(p)]
            if len(valid_partitions) != len(partitions):
                self._logger.warning(
                    "Attempted to assign invalid partitions",
                    valid=[f"{p.topic}:{p.partition}@{p.offset}" for p in valid_partitions],
                    all=[f"{p.topic}:{p.partition}@{p.offset}" for p in partitions],
                )

            # We assign only valid partitions
            consumer.assign(valid_partitions)
            self._logger.debug(
                "Partitions assigned successfully",
                valid=[f"{p.topic}:{p.partition}@{p.offset}" for p in valid_partitions],
            )

        except KafkaException as e:
            kafka_error = e.args[0]
            error_code = kafka_error.code()

            # Kafka specific error handling
            if error_code == KafkaError._ALL_BROKERS_DOWN:
                self._logger.error("All brokers unavailable", error=kafka_error.str())
            elif error_code == KafkaError._UNKNOWN_TOPIC:
                self._logger.error("Topic does not exist", topics=[p.topic for p in partitions])
            elif error_code == KafkaError._UNKNOWN_PARTITION:
                self._logger.warning("Invalid partitions", partitions=[p.partition for p in partitions])
            else:
                self._logger.error(
                    "Kafka error during assignment", error_code=error_code, error=kafka_error.str(), exc_info=True
                )

        except Exception as e:  # pylint: disable=broad-exception-caught
            self._logger.error("Unexpected assignment error", error=repr(e), exc_info=True)

        finally:
            # Consumer state protection
            try:
                if not consumer.assignment():  # If the assignment failed
                    self._logger.warning("Consumer has no assigned partitions after assignment attempt")
                    consumer.unassign()
            except Exception as e:  # pylint: disable=broad-exception-caught
                self._logger.error("Failed to reset consumer state", error=repr(e), exc_info=True)

    def _on_revoke_sync(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Synchronous callback for partition revocation. Commits offsets and unassigns.

        Args:
            consumer (Consumer): Kafka Consumer instance.
            partitions (List[TopicPartition]): List of revoked partitions.
        """
        try:
            self._logger.info(
                "Revoked partitions", partitions=[f"{p.topic}:{p.partition}@{p.offset}" for p in partitions]
            )
            consumer.commit(offsets=partitions, asynchronous=False)
            self._logger.debug("Offsets committed for revoked partitions")
        except KafkaException as e:
            # Handling the error of missing offsets
            if e.args[0].code() == KafkaError._NO_OFFSET:
                self._logger.warning(
                    "No offsets to commit for partitions",
                    partitions=[f"{p.topic}:{p.partition}@{p.offset}" for p in partitions],
                    error=repr(e),
                    exc_info=True,
                )
            else:
                self._logger.error("Commit error on revoke", error=repr(e), exc_info=True)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self._logger.error("Unexpected error during revoke", error=repr(e), exc_info=True)
        finally:
            try:
                consumer.unassign()
            except Exception as e:  # pylint: disable=broad-exception-caught
                self._logger.error("Error unassigning partitions", error=repr(e))
