"""Unit tests for Kafka settings classes are defined here."""

from typing import Any

import pytest
from pydantic import ValidationError

from otteroad.settings import KafkaConsumerSettings, KafkaProducerSettings


class TestKafkaConsumerSettings:
    def test_required_group_id(self):
        """Test the required 'group_id' field in KafkaConsumerSettings."""
        with pytest.raises(ValidationError) as exc:
            KafkaConsumerSettings(
                bootstrap_servers=["localhost:9092"],
                # group_id is missing
            )
        assert "group_id" in str(exc.value)

    def test_auto_offset_reset_validation(self):
        """Test the validation of the 'auto_offset_reset' field in KafkaConsumerSettings."""
        valid_values = ["earliest", "latest", "error"]
        invalid_value = "invalid"

        for value in valid_values:
            settings = KafkaConsumerSettings(group_id="test-group", auto_offset_reset=value)
            assert settings.auto_offset_reset == value

        with pytest.raises(ValidationError):
            KafkaConsumerSettings(group_id="test-group", auto_offset_reset=invalid_value)

    def test_auto_commit_validation(self):
        """Test the validation of the 'auto_commit_interval_ms' field in KafkaConsumerSettings."""
        # Valid case
        settings = KafkaConsumerSettings(group_id="test-group", enable_auto_commit=True, auto_commit_interval_ms=100)
        assert settings.enable_auto_commit is True

        # Invalid case
        with pytest.raises(ValidationError) as exc:
            KafkaConsumerSettings(group_id="test-group", enable_auto_commit=True, auto_commit_interval_ms=0)
        assert "auto_commit_interval_ms must be positive" in str(exc.value)

    def test_default_values(self):
        """Test the default values in KafkaConsumerSettings."""
        settings = KafkaConsumerSettings(group_id="test-group")
        assert settings.enable_auto_commit is False
        assert settings.isolation_level == "read_committed"
        assert settings.session_timeout_ms == 45000

    def test_inheritance_from_base(self):
        """Test inheritance of common settings from the base class KafkaBaseSettings."""
        settings = KafkaConsumerSettings(
            group_id="test-group", schema_registry_url="http://custom:8081", custom_properties={"max.poll.records": 100}
        )
        assert settings.schema_registry_url == "http://custom:8081"
        assert settings.custom_properties["max.poll.records"] == 100


class TestKafkaProducerSettings:
    @pytest.mark.parametrize("acks,expected_error", [("0", False), ("1", False), ("all", False), ("invalid", True)])
    def test_acks_validation(self, acks: Any, expected_error: bool):
        """Test the validation of the 'acks' field in KafkaProducerSettings."""
        if expected_error:
            with pytest.raises(ValidationError):
                KafkaProducerSettings(acks=acks)
        else:
            settings = KafkaProducerSettings(acks=acks)
            assert settings.acks == acks

    def test_idempotence_validation(self):
        """Test the validation of idempotence-related fields in KafkaProducerSettings."""
        # Valid case
        settings = KafkaProducerSettings(enable_idempotence=True, acks="all", max_in_flight=1)
        assert settings.enable_idempotence is True

        # Invalid acks
        with pytest.raises(ValidationError) as exc:
            KafkaProducerSettings(enable_idempotence=True, acks="1")
        assert "acks='all'" in str(exc.value)

        # Invalid max_in_flight
        with pytest.raises(ValidationError) as exc:
            KafkaProducerSettings(enable_idempotence=True, max_in_flight=6)
        assert "max_in_flight=5" in str(exc.value)

    def test_transactional_id_validation(self):
        """Test the validation of 'transactional_id' in KafkaProducerSettings."""
        # Valid case
        settings = KafkaProducerSettings(enable_idempotence=True, max_in_flight=1, transactional_id="tx-1")
        assert settings.transactional_id == "tx-1"

        # Invalid case
        with pytest.raises(ValidationError) as exc:
            KafkaProducerSettings(transactional_id="tx-1", enable_idempotence=False)
        assert "requires idempotence=True" in str(exc.value)

    @pytest.mark.parametrize("compression_type,valid", [("gzip", True), ("snappy", True), ("invalid", False)])
    def test_compression_type_validation(self, compression_type: Any, valid: bool):
        """Test the validation of the 'compression_type' field in KafkaProducerSettings."""
        if valid:
            settings = KafkaProducerSettings(compression_type=compression_type)
            assert settings.compression_type == compression_type
        else:
            with pytest.raises(ValidationError):
                KafkaProducerSettings(compression_type=compression_type)

    def test_default_values(self):
        """Test the default values in KafkaProducerSettings."""
        settings = KafkaProducerSettings()
        assert settings.acks == "all"
        assert settings.retries == 3
        assert settings.enable_idempotence is False

    def test_inheritance_from_base(self):
        """Test inheritance of common settings from the base class KafkaBaseSettings."""
        settings = KafkaProducerSettings(schema_registry_url="http://custom:8081", custom_properties={"linger.ms": 10})
        assert settings.schema_registry_url == "http://custom:8081"
        assert settings.custom_properties["linger.ms"] == 10
