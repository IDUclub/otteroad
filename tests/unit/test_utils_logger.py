"""Unit tests for LoggerAdapter class are defined here."""

from unittest.mock import Mock

from otteroad.utils.logger import LoggerAdapter


class TestLoggerAdapter:
    """Tests for LoggerAdapter functionality across multiple logger types."""

    def test_standard_logging_info(self):
        """Should format structured fields and call standard logger.info()"""
        logger_mock = Mock()
        adapter = LoggerAdapter(logger_mock)

        adapter.info("Test message", user="john", action="login")

        logger_mock.info.assert_called_once_with("Test message (user = john, action = login)", exc_info=None)

    def test_standard_logging_error_with_exc_info(self):
        """Should pass exc_info to standard logger.error()"""
        logger_mock = Mock()
        adapter = LoggerAdapter(logger_mock)

        adapter.error("Something failed", code=500, exc_info=True)

        logger_mock.error.assert_called_once_with("Something failed (code = 500)", exc_info=True)

    def test_structlog_logging(self):
        """Should call structlog's method with structured fields directly"""
        logger_mock = Mock()
        logger_mock.__class__.__module__ = "structlog.stdlib"
        adapter = LoggerAdapter(logger_mock)

        adapter.warning("Warning issued", reason="timeout")

        logger_mock.warning.assert_called_once_with("Warning issued", reason="timeout")

    def test_structlog_with_exc_info(self):
        """Should pass exc_info correctly to structlog"""
        logger_mock = Mock()
        logger_mock.__class__.__module__ = "structlog.stdlib"
        adapter = LoggerAdapter(logger_mock)

        adapter.critical("Critical error", system="auth", exc_info=True)

        logger_mock.critical.assert_called_once_with("Critical error", system="auth", exc_info=True)

    def test_loguru_logging(self):
        """Should format structured fields into the message string for loguru"""
        logger_mock = Mock()
        logger_mock.__class__.__module__ = "loguru.logger"
        adapter = LoggerAdapter(logger_mock)

        adapter.debug("Debugging", step=3, status="ok")

        logger_mock.debug.assert_called_once_with("Debugging (step = 3, status = ok)")

    def test_loguru_logging_no_kwargs(self):
        """Should not add extra info if no kwargs are passed to loguru"""
        logger_mock = Mock()
        logger_mock.__class__.__module__ = "loguru.logger"
        adapter = LoggerAdapter(logger_mock)

        adapter.info("Simple info")

        logger_mock.info.assert_called_once_with("Simple info")

    def test_fallback_to_info_if_level_missing(self):
        """Should fall back to logger.info() if method for level is missing"""
        logger_mock = Mock()
        adapter = LoggerAdapter(logger_mock)

        # Simulate a missing 'warning' method
        logger_mock.warning = None
        adapter.warning("Fallback case", detail="none")

        logger_mock.info.assert_called_once_with("Fallback case (detail = none)", exc_info=None)
