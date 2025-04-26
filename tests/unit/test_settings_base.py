"""Unit tests for Kafka settings base class are defined here."""

import os
import tempfile

import pytest
import yaml
from pydantic import ValidationError

from otteroad.settings.base import KafkaBaseSettings


class TestKafkaBaseSettings:
    @pytest.fixture
    def default_settings(self):
        """Fixture to create an instance of KafkaBaseSettings with default values."""
        return KafkaBaseSettings()

    def test_default_values(self, default_settings):
        """Test the default values of KafkaBaseSettings."""
        assert default_settings.bootstrap_servers == "localhost:9092"
        assert default_settings.security_protocol == "PLAINTEXT"
        assert default_settings.reconnect_backoff_ms == 100
        assert default_settings.max_in_flight == 5
        assert default_settings.schema_registry_url == "http://localhost:8081"

    def test_validation(self):
        """Test the validation of KafkaBaseSettings fields."""
        with pytest.raises(ValidationError):
            KafkaBaseSettings(reconnect_backoff_ms=-1)

        with pytest.raises(ValidationError):
            KafkaBaseSettings(max_in_flight=0)

        with pytest.raises(ValidationError):
            KafkaBaseSettings(security_protocol="INVALID")

    def test_bootstrap_servers_parsing(self):
        """Test the parsing and validation of the bootstrap_servers field."""
        # Test string input
        settings = KafkaBaseSettings(bootstrap_servers=["host1:9092", "host2:9092"])
        assert settings.bootstrap_servers == "host1:9092,host2:9092"

        # Test list input
        settings = KafkaBaseSettings(bootstrap_servers=["host3:9092"])
        assert settings.bootstrap_servers == "host3:9092"

        # Test invalid type
        with pytest.raises(ValidationError):
            KafkaBaseSettings(bootstrap_servers=123)

    def test_from_env(self, monkeypatch):
        """Test loading KafkaBaseSettings from environment variables."""
        # Setup environment variables
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "envhost:9092")
        monkeypatch.setenv("KAFKA_CLIENT_ID", "test-client")
        monkeypatch.setenv("KAFKA_CUSTOM_PROP", "value")

        # Test without .env file
        settings = KafkaBaseSettings.from_env()
        assert settings.bootstrap_servers == "envhost:9092"
        assert settings.client_id == "test-client"
        assert settings.custom_properties == {"custom_prop": "value"}

        # Test with .env file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("KAFKA_SECURITY_PROTOCOL=SSL\n")
            f.write("KAFKA_ANOTHER_PROP=123\n")
            f.flush()

        settings = KafkaBaseSettings.from_env(env_file=f.name)
        assert settings.security_protocol == "SSL"
        assert settings.custom_properties == {"custom_prop": "value", "another_prop": "123"}
        os.remove(f.name)

        # Test file not found
        with pytest.raises(FileNotFoundError):
            KafkaBaseSettings.from_env(env_file="nonexistent.env")

    def test_from_yaml(self):
        """Test loading KafkaBaseSettings from a YAML file."""
        # Create temporary YAML file
        yaml_content = """
        bootstrap_servers: 
          - "yamlhost:9092"
        security_protocol: "SASL_SSL"
        custom_prop: "yaml-value"
        """

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(yaml_content)
            f.flush()

        settings = KafkaBaseSettings.from_yaml(f.name)
        assert settings.bootstrap_servers == "yamlhost:9092"
        assert settings.security_protocol == "SASL_SSL"
        assert settings.custom_properties == {"custom_prop": "yaml-value"}
        os.remove(f.name)

        # Test nested key
        nested_yaml = {"kafka": yaml.safe_load(yaml_content)}
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            yaml.dump(nested_yaml, f)
            f.flush()

        settings = KafkaBaseSettings.from_yaml(f.name, key="kafka")
        assert settings.bootstrap_servers == "yamlhost:9092"

        # Test error handling
        with pytest.raises(FileNotFoundError):
            KafkaBaseSettings.from_yaml("nonexistent.yaml")

        with pytest.raises(ValueError):
            KafkaBaseSettings.from_yaml(f.name, key="invalid")

        os.remove(f.name)

    def test_from_custom_config(self):
        """Test loading KafkaBaseSettings from a custom dictionary or object."""
        # Test dict input
        config = {"bootstrap_servers": ["dicthost:9092"], "unknown_prop": "test"}
        settings = KafkaBaseSettings.from_custom_config(config)
        assert settings.bootstrap_servers == "dicthost:9092"
        assert settings.custom_properties == {"unknown_prop": "test"}

        # Test object input
        class CustomConfig:
            def __init__(self):
                self.kafka_bootstrap_servers = "objhost:9092"
                self.kafka_other_prop = 123

        settings = KafkaBaseSettings.from_custom_config(CustomConfig(), prefix="kafka_")
        assert settings.bootstrap_servers == "objhost:9092"
        assert settings.custom_properties == {"other_prop": 123}

    def test_config_generation(self):
        """Test the generation of Kafka and Schema Registry configurations from KafkaBaseSettings."""
        settings = KafkaBaseSettings(
            bootstrap_servers=["host:9092"],
            schema_registry_url="http://registry:8081",
            custom_properties={"socket.timeout.ms": 1000},
        )

        # Test Kafka config
        kafka_config = settings.get_config()
        assert kafka_config["bootstrap.servers"] == "host:9092"
        assert "url" not in kafka_config
        assert kafka_config["socket.timeout.ms"] == 1000

        # Test Schema Registry config
        sr_config = settings.get_schema_registry_config()
        assert sr_config["url"] == "http://registry:8081"
        assert "bootstrap.servers" not in sr_config

    def test_edge_cases(self):
        """Test edge cases for KafkaBaseSettings."""
        # Empty custom properties
        settings = KafkaBaseSettings(custom_properties={})
        assert settings.custom_properties == {}

        # None values
        settings = KafkaBaseSettings(client_id=None)
        assert settings.client_id is None

        # Maximum values
        settings = KafkaBaseSettings(message_max_bytes=10485760, schema_registry_cache_capacity=None)
        assert settings.message_max_bytes == 10485760
        assert settings.schema_registry_cache_capacity is None

    def test_security_protocols(self):
        """Test the validation of security protocols in KafkaBaseSettings."""
        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]:
            settings = KafkaBaseSettings(security_protocol=protocol)
            assert settings.security_protocol == protocol

        with pytest.raises(ValidationError):
            KafkaBaseSettings(security_protocol="INVALID")
