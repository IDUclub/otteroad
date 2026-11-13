"""
This module defines a runtime-checkable protocol for logging interfaces,
ensuring type-safe injection of custom logger implementations throughout
the Kafka client library.
"""

import logging
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


class LoggerAdapter:
    """
    A universal logger adapter that provides a unified interface for:
    - built-in `logging` module
    - `loguru` logger
    - `structlog` logger

    This allows writing logger.info("message", key1=value1, ...) syntax
    and ensures structured fields are passed correctly depending on the backend.

    Attributes:
        _logger (Any): The wrapped logger instance
        _is_structlog (bool): Whether the logger is structlog
        _is_loguru (bool): Whether the logger is loguru

    Example:
        logger = LoggerAdapter(actual_logger)
        logger.info("Something happened", key1="value", error="details")
    """

    def __init__(self, logger: LoggerProtocol):
        self._logger = logger
        self._is_structlog = self._detect_structlog()
        self._is_loguru = self._detect_loguru()

    def _detect_structlog(self) -> bool:
        """
        Detect whether the logger is a structlog logger
        (uses .bind() and structured event_dict).
        """
        return self._logger.__class__.__module__.startswith("structlog")

    def _detect_loguru(self) -> bool:
        """
        Detect whether the logger is from the `loguru` library.
        """
        return self._logger.__class__.__module__.startswith("loguru")

    def _log(self, level: str, message: str, **kwargs):
        """
        Unified logging method that dispatches to the correct backend.

        Args:
            level (str): Log level (e.g., 'info', 'error')
            message (str): Log message string
            **kwargs: Structured data or special fields (e.g., exc_info=True)
        """
        # Extract exc_info if present (used for exception logging)
        exc_info = kwargs.pop("exc_info", None)

        if self._is_structlog:
            # structlog supports structured fields directly
            log_method = getattr(self._logger, level)
            if exc_info:
                kwargs["exc_info"] = exc_info
            log_method(message, **kwargs)

        elif self._is_loguru:
            # loguru uses `extra` for structured fields; handles exc_info separately
            log_method = getattr(self._logger, level)
            log_msg = message
            if kwargs:
                log_msg += " (" + ", ".join([f"{k} = {i}" for k, i in kwargs.items()]) + ")"
            log_method(log_msg)

        else:
            # Standard logging.Logger (or compatible): use `extra`
            log_method = getattr(self._logger, level, None)
            log_msg = message
            if kwargs:
                log_msg += " (" + ", ".join([f"{k} = {i}" for k, i in kwargs.items()]) + ")"
            if log_method is not None:
                log_method(log_msg, exc_info=exc_info)
            else:
                # Fallback to .info()
                self._logger.info(log_msg, exc_info=exc_info)

    # Public level methods

    def debug(self, msg: str, **kwargs):  # pylint: disable=missing-function-docstring
        self._log("debug", msg, **kwargs)

    def info(self, msg: str, **kwargs):  # pylint: disable=missing-function-docstring
        self._log("info", msg, **kwargs)

    def warning(self, msg: str, **kwargs):  # pylint: disable=missing-function-docstring
        self._log("warning", msg, **kwargs)

    def error(self, msg: str, **kwargs):  # pylint: disable=missing-function-docstring
        self._log("error", msg, **kwargs)

    def critical(self, msg: str, **kwargs):  # pylint: disable=missing-function-docstring
        self._log("critical", msg, **kwargs)

    def log(self, level: int, msg: str, *args, **kwargs):
        """
        Log with a numeric `logging` level and %-style arguments, as `logging.Logger.log` does.

        confluent-kafka sends the internal librdkafka logs through this method of the `logger` it is given.
        """
        if args:
            msg = msg % args
        if level >= logging.CRITICAL:
            self.critical(msg, **kwargs)
        elif level >= logging.ERROR:
            self.error(msg, **kwargs)
        elif level >= logging.WARNING:
            self.warning(msg, **kwargs)
        elif level >= logging.INFO:
            self.info(msg, **kwargs)
        else:
            self.debug(msg, **kwargs)


# librdkafka `log_level` is a syslog level, 0 (emergency) to 7 (debug); 6 (info) is the librdkafka default.
KAFKA_LOG_LEVEL_SILENT = 0
KAFKA_LOG_LEVEL_DEFAULT = 6


def kafka_logger_config(logger: LoggerAdapter, disable_internal_kafka_logs: bool) -> dict:
    """
    Settings that route the internal librdkafka logs to `logger`.

    Args:
        logger: Adapter that receives the logs.
        disable_internal_kafka_logs: Keep emergencies only instead of the librdkafka default level.

    Returns:
        dict: `logger` and `log_level` entries of a confluent-kafka configuration.
    """
    level = KAFKA_LOG_LEVEL_SILENT if disable_internal_kafka_logs else KAFKA_LOG_LEVEL_DEFAULT
    return {"logger": logger, "log_level": level}
