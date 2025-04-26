"""
This module provides KafkaConsumerSettings, extending KafkaBaseSettings with consumer-specific
configuration options for Kafka consumers, including group management, offset handling,
auto-commit settings, performance tuning, and transaction isolation.
"""

from typing import Literal

from pydantic import Field, model_validator

from idu_kafka_client.settings.base import KafkaBaseSettings


class KafkaConsumerSettings(KafkaBaseSettings):
    """
    Configuration model for Kafka consumers, including settings inherited from KafkaBaseSettings
    and additional consumer-specific parameters.

    Attributes:
        All attributes from KafkaBaseSettings
        group_id (str): Consumer group ID (required)
        auto_offset_reset (str): Policy when no initial offset exists
        enable_auto_commit (bool): Whether offsets are committed automatically
        auto_commit_interval_ms (int): Interval between auto-commit operations (ms)
        max_poll_interval_ms (int): Maximum interval between poll calls before considered failed (ms)
        session_timeout_ms (int): Consumer session timeout (ms)
        isolation_level (str): Transaction isolation level for reading records

    Raises:
        ValueError: On invalid combination of auto_commit settings or missing group_id
    """

    # Required
    group_id: str = Field(..., description="Consumer group ID (required for consumer)")

    # Offset management
    auto_offset_reset: Literal["earliest", "latest", "beginning", "smallest", "largest", "end", "error"] = Field(
        default="latest",
        description="Offset reset policy when no initial offset exists",
    )
    enable_auto_commit: bool = Field(
        default=False,
        description="Automatically commit offsets periodically",
    )
    auto_commit_interval_ms: int = Field(
        default=5000,
        ge=0,
        description="Frequency of offset commits when auto-commit enabled",
    )

    # Performance
    max_poll_interval_ms: int = Field(
        default=300000,
        gt=0,
        description="Max time between poll() calls before leaving group",
    )
    session_timeout_ms: int = Field(
        default=45000,
        ge=1000,
        description="Timeout for consumer session",
    )

    # Transactions
    isolation_level: Literal["read_uncommitted", "read_committed"] = Field(
        default="read_committed", description="Transaction isolation level"
    )

    @model_validator(mode="after")
    def validate_group_settings(self) -> "KafkaConsumerSettings":
        """
        Ensure auto_commit_interval_ms is positive when auto-commit is enabled.

        Returns:
            KafkaConsumerSettings: Validated settings instance.

        Raises:
            ValueError: If auto_commit_interval_ms is not positive when enable_auto_commit is True.
        """
        if self.enable_auto_commit and self.auto_commit_interval_ms <= 0:
            raise ValueError("auto_commit_interval_ms must be positive when auto-commit enabled")
        return self
