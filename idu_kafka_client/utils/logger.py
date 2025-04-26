"""
This module defines a runtime-checkable protocol for logging interfaces,
ensuring type-safe injection of custom logger implementations throughout
the Kafka client library.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class LoggerProtocol(Protocol):
    """
    Protocol for a standard logging interface, compatible with Python's logging.Logger.

    Methods correspond to standard logging levels for debug, info, warning,
    error, exception, and critical messages.
    """

    def debug(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
    def info(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
    def warning(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
    def error(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
    def exception(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
    def critical(self, msg: str, *args, **kwargs) -> None: ...  # pylint: disable=missing-function-docstring
