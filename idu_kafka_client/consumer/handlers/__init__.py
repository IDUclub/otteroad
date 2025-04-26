"""
This package provides a structured framework for Kafka message handling within the consumer side of the application.

Exports:
    - `BaseMessageHandler`: Abstract handler with pre-/post-processing and error management.
    - `EventHandlerRegistry`: Registry for associating event types with handler instances.
"""

from .base import BaseMessageHandler
from .registry import EventHandlerRegistry

__all__ = [
    "BaseMessageHandler",
    "EventHandlerRegistry",
]
