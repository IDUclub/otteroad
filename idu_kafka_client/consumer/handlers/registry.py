"""Module for registering and retrieving event handlers based on AvroEventModel types."""

from idu_kafka_client.avro.model import AvroEventModel
from idu_kafka_client.consumer.handlers.base import BaseMessageHandler


class EventHandlerRegistry:
    """
    Registry for associating event types with handlers.

    Allows:
    - Register handlers for specific event types.
    - Unregister handlers.
    - Retrieve handlers by event instance or class.
    - Check handler existence.

    Usage examples:

    1. Registering a handler:
    >>>>>> registry = EventHandlerRegistry()
    >>>>>> registry.register(UserCreatedHandler())

    2. Getting a handler:
    >>>>>> handler = registry.get_handler(user-created event)
    """

    def __init__(self) -> None:
        """
        Initializes the EventHandlerRegistry.

        Sets up the internal mapping between event type keys and handler instances.
        """
        self._handlers: dict[str, BaseMessageHandler] = {}

    def register(self, handler: BaseMessageHandler, overwrite: bool = False) -> None:
        """
        Registers a handler for a specific event type.

        Args:
            handler (BaseMessageHandler): Handler instance to register. Must define 'event_type' attribute.
            overwrite (bool): Whether to overwrite an existing handler for the same event type.

        Raises:
            TypeError: If handler is not an instance of BaseMessageHandler.
            ValueError: If a handler is already registered for the event type and overwrite is False.
        """
        if not isinstance(handler, BaseMessageHandler):
            raise TypeError("Handler must be a subclass of BaseMessageHandler")

        event_type = handler.event_type
        event_key = str(event_type)

        if event_key in self._handlers and not overwrite:
            raise ValueError(f"Handler for {event_key} already registered")

        self._handlers[event_key] = handler

    def unregister(self, event_type: type[AvroEventModel] | str) -> None:
        """
        Unregisters the handler for the specified event type.

        Args:
            event_type (type[AvroEventModel] or str): Event class or its string representation.

        Raises:
            KeyError: If no handler is found for the specified event type.
        """
        event_key = event_type if isinstance(event_type, str) else str(event_type)

        if event_key in self._handlers:
            del self._handlers[event_key]
        else:
            raise KeyError(f"No handler for {event_key}")

    def get_handler(self, event: AvroEventModel | type[AvroEventModel]) -> BaseMessageHandler | None:
        """
        Retrieves the handler for a given event instance or class.

        Args:
            event (AvroEventModel or type[AvroEventModel]): Event instance or class for which to get the handler.

        Returns:
            BaseMessageHandler: The registered handler instance.

        Raises:
            typeError: If the provided argument is not an AvroEventModel instance or subclass.
            KeyError: If no handler is registered for the event type.
        """
        event_type = event if isinstance(event, type) else type(event)
        event_key = str(event_type)

        if handler := self._handlers.get(event_key):
            return handler

        return None

    def has_handler(self, event_type: type[AvroEventModel] | str) -> bool:
        """
        Checks if a handler is registered for the given event type.

        Args:
            event_type (type[AvroEventModel] or str): Event class or its string representation.

        Returns:
            bool: True if a handler is registered, False otherwise.
        """
        event_key = event_type if isinstance(event_type, str) else str(event_type)
        return event_key in self._handlers

    @property
    def handlers(self) -> dict[str, BaseMessageHandler]:
        """
        Returns a copy of the internal handler registry.

        Returns:
            dict[str, BaseMessageHandler]: Mapping of event type keys to handler instances.
        """
        return self._handlers.copy()

    def __contains__(self, event_type: type[AvroEventModel] | str) -> bool:
        """
        Enables 'in' operator for checking handler existence.

        Args:
            event_type (type[AvroEventModel] or str): Event class or its string representation.

        Returns:
            bool: True if a handler is registered, False otherwise.
        """
        return self.has_handler(event_type)
