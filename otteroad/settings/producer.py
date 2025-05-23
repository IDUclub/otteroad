"""
This module provides KafkaProducerSettings, extending KafkaBaseSettings with producer-specific
configuration options for Kafka producers, including reliability, batching policies,
performance tuning, and exactly-once semantics controls.
"""

from typing import Literal

from pydantic import Field, model_validator

from otteroad.settings.base import KafkaBaseSettings


class KafkaProducerSettings(KafkaBaseSettings):
    """
    Configuration model for Kafka producers, including settings inherited from KafkaBaseSettings
    and additional producer-specific parameters.

    Attributes:
        All attributes from KafkaBaseSettings
        acks (str): Broker acknowledgments required: '0', '1', or 'all'.
        queue_buffering_max_messages (int): Max number of messages in the producer queue.
        queue_buffering_max_kbytes (int): Max total message size (KB) in the producer queue.
        linger_ms (int): Delay in milliseconds to wait for batching.
        compression_type (str): Compression codec for message batches ('none', 'gzip', 'snappy', 'lz4', 'zstd').
        retries (int): Number of retries for produce/fetch operations.
        enable_idempotence (bool): Enable exactly-once delivery semantics.
        transactional_id (str | None): Transactional ID for atomic writes across partitions.

    Raises:
        ValueError: On invalid idempotence or transactional configuration.
    """

    # Reliability
    acks: Literal["0", "1", "all"] = Field(
        default="all",
        description="Broker acknowledgments required:\n"
        "- 0: No wait\n"
        "- 1: Leader only\n"
        "- all: Full ISR replication",
    )

    # Batching
    queue_buffering_max_messages: int = Field(
        default=100000, description="Maximum number of messages allowed on the producer queue"
    )
    queue_buffering_max_kbytes: int = Field(
        default=1048576, description="Maximum total message size sum allowed on the producer queue"
    )
    linger_ms: int = Field(default=5, ge=0, description="Wait time for batching")

    # Performance
    compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = Field(
        default="none",
        description="Compression codec for message batches",
    )
    retries: int = Field(
        default=3,
        description="Number of retries for produce/fetch operations",
    )

    # Exactly-once semantics
    enable_idempotence: bool = Field(default=False, description="Enable exactly-once delivery semantics")
    transactional_id: str | None = Field(
        default=None, description="Transactional ID for atomic writes across partitions"
    )

    @model_validator(mode="after")
    def validate_idempotence(self) -> "KafkaProducerSettings":
        """
        Ensure idempotence-related settings are correct when enabled.

        Returns:
            KafkaProducerSettings: Validated settings instance.

        Raises:
            ValueError: If 'acks' is not 'all' or max_in_flight > 1 when idempotence is enabled.
        """
        if self.enable_idempotence:
            if self.acks != "all":
                raise ValueError("Idempotence requires acks='all'")
            if self.max_in_flight > 5:
                raise ValueError("Idempotence requires max_in_flight=5 or less")
        return self

    @model_validator(mode="after")
    def validate_transactions(self) -> "KafkaProducerSettings":
        """
        Ensure transactional configuration is consistent with idempotence setting.

        Returns:
            KafkaProducerSettings: Validated settings instance.

        Raises:
            ValueError: If transactional_id is set but enable_idempotence is False.
        """
        if self.transactional_id and not self.enable_idempotence:
            raise ValueError("Transactional producer requires idempotence=True")
        return self
