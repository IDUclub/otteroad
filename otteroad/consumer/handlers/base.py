"""
Abstract base classes for Kafka message handling infrastructure.

This module provides:
- BaseMessageHandler: Abstract class defining message processing pipeline
- Generic event type handling with Pydantic/Avro models
- Built-in hooks for preprocessing, error handling, and lifecycle management
"""

import logging
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Generic, TypeVar, get_args, get_origin

from confluent_kafka import Message
from pydantic import BaseModel

from otteroad.avro.model import AvroEventModel
from otteroad.utils import LoggerAdapter, LoggerProtocol

# Generic type for supported event models (Pydantic BaseModel or AvroEventModel)
EventT = TypeVar("EventT", bound=[BaseModel, AvroEventModel])


class BaseMessageHandler(ABC, Generic[EventT]):
    """
    Abstract base class for implementing Kafka message handlers.

    Provides a structured framework for processing Kafka messages with:
    - Type-safe event parsing
    - Pre-/post-processing hooks
    - Error handling infrastructure
    - Lifecycle management (startup/shutdown)

    Implementation Requirements:
    1. Subclasses must implement abstract methods
    2. Event type must be specified via event_type property
    3. Handler methods should be thread-safe
    """

    _event_type: type | None = None

    def __init__(self, logger: LoggerProtocol | None = None):
        """
        Initialize message handler instance.

        Args:
            logger: Custom logger implementing LoggerProtocol.
                    Uses default logger if not provided.
        """
        self._logger = LoggerAdapter(logger or logging.getLogger(__name__))

    def __init_subclass__(cls, **kwargs):
        """
        Automatically called when a subclass of BaseMessageHandler is created.

        Purpose:
        - Automatically extract and set the concrete EventT type for the subclass.
        - Ensure that every handler correctly declares its event model type by inheriting with [EventT].

        Args:
            **kwargs: Additional keyword arguments passed during subclass initialization.
        """
        super().__init_subclass__(**kwargs)

        # If _event_type is already set (e.g., manually assigned), skip further processing
        if cls._event_type is not None:
            return

        # Traverse the method resolution order (MRO) to find the original BaseMessageHandler[...] base
        for base in cls.__mro__:
            # Check if the base class has explicitly declared generic bases
            if hasattr(base, "__orig_bases__"):
                for orig_base in base.__orig_bases__:
                    # Look for BaseMessageHandler[EventT] specifically
                    if get_origin(orig_base) is BaseMessageHandler:
                        # Extract the EventT type argument
                        (event_type,) = get_args(orig_base)
                        cls._event_type = event_type
                        return

        # If no appropriate generic type found, raise a descriptive error
        raise TypeError(
            f"Cannot determine EventT for {cls.__name__}. " f"Please inherit from BaseMessageHandler[YourEventModel]."
        )

    @cached_property
    def event_type(self) -> type[EventT]:
        """Event model type processed by this handler.

        Returns:
            Type[EventT]: Concrete event model class
        """
        if self._event_type is None:
            raise NotImplementedError("event_type could not be inferred automatically.")
        return self._event_type

    @abstractmethod
    async def on_startup(self):
        """Initialize handler resources.

        Typical use cases:
        - Database connection pooling
        - External service clients
        - Resource warmup
        """

    @abstractmethod
    async def on_shutdown(self):
        """Clean up handler resources.

        Typical use cases:
        - Closing network connections
        - Flushing buffers
        - Finalizing transactions
        """

    @abstractmethod
    async def handle(self, event: EventT, ctx: Message):
        """Process validated message with business logic.

        Args:
            event: Parsed event model instance
            ctx: Raw Kafka message
        """

    async def pre_process(  # pylint: disable=missing-param-doc
        self, event: EventT, ctx: Message, *args, **kwargs  # pylint: disable=unused-argument
    ) -> tuple[EventT, Message]:
        """
        Preprocessing hook for raw messages.

        Typical use cases:
        - Validation enrichment
        - Metadata injection
        - Schema version checks

        Args:
            event: Initial parsed event
            ctx: Original Kafka message context

        Returns:
            tuple[EventT, Message]: Modified event and context
        """
        self._logger.debug(
            "Pre-processing message",
            event_model=str(event),
            topic=ctx.topic(),
            partition=ctx.partition(),
            offset=ctx.offset(),
        )

        return event, ctx

    async def post_process(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Post-processing hook for successful handling.

        Typical use cases:
        - Metrics collection
        - Audit logging
        - Transaction finalization
        """
        self._logger.debug("Post-processing completed successfully")

    async def handle_error(  # pylint: disable=missing-param-doc
        self, error: Exception, event: EventT, ctx: Message, *args, **kwargs  # pylint: disable=unused-argument
    ) -> None:
        """
        Error handling hook for processing failures.

        Args:
            error: Exception that occurred
            event: Event instance that failed processing
            ctx: Associated Kafka message context
        """
        self._logger.error(
            "Error processing message",
            event_model=str(event),
            topic=ctx.topic(),
            partition=ctx.partition(),
            offset=ctx.offset(),
            error=str(error),
            exc_info=True,
        )
        raise error

    async def process(self, event: EventT, ctx: Message):
        """
        Execute complete message processing pipeline.

        Flow:
        1. pre_process -> 2. handle -> 3. post_process
                          ↳ handle_error on failure

        Args:
            event: Parsed event instance
            ctx: Kafka message context
        """
        try:
            processed_event, processed_ctx = await self.pre_process(event, ctx)

            self._logger.info(
                "Processing message",
                event_model=str(processed_event),
                topic=processed_ctx.topic(),
                partition=processed_ctx.partition(),
                offset=processed_ctx.offset(),
            )

            await self.handle(processed_event, processed_ctx)
            await self.post_process()

        except Exception as e:  # pylint: disable=broad-except
            await self.handle_error(e, event, ctx)
