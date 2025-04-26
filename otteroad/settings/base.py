"""
This module provides KafkaBaseSettings, a Pydantic-based configuration class
for managing Kafka client settings and Schema Registry integration.

It supports loading configuration from environment variables, .env files,
YAML files, or custom objects, with robust validation and extension via
custom properties.
"""

import os
from pathlib import Path
from typing import Any, Literal, TypeVar

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, Field, field_validator

SettingsT = TypeVar("SettingsT", bound="KafkaBaseSettings")


class KafkaBaseSettings(BaseModel):
    """
    Base settings model for Kafka clients, including bootstrap servers,
    security, message constraints, monitoring, Schema Registry, and
    custom properties.

    Attributes:
        bootstrap_servers (List[AnyUrl]): List of Kafka brokers in host:port format.
        client_id (Optional[str]): Identifier for this Kafka client, appears in broker logs.
        reconnect_backoff_ms (int): Backoff time between reconnect attempts (ms).
        security_protocol (str): Security protocol (PLAINTEXT, SSL, SASL_SSL, etc.).
        max_in_flight (int): Max concurrent unacknowledged requests per connection.
        message_max_bytes (int): Max allowed message size (bytes, including headers).
        statistics_interval_ms (int): Metrics collection interval (ms).
        schema_registry_url (AnyUrl): URL of the Schema Registry server.
        schema_registry_timeout (int): Timeout for registry API requests (ms).
        schema_registry_max_retries (int): Maximum retries for a request.
        schema_registry_retries_wait_ms (int): Maximum time to wait for the first retry (ms).
        schema_registry_cache_capacity (Optional[int]): Max schemas to cache locally.
        custom_properties (Dict[str, Any]): Additional client properties.
    """

    # Common client settings
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="List of Kafka brokers in host:port format",
    )
    client_id: str | None = Field(
        default=None,
        description="Identifier for this Kafka client, appears in broker logs",
    )
    reconnect_backoff_ms: int = Field(
        default=100,
        ge=0,
        description="Backoff time in milliseconds between connection attempts",
    )
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = Field(
        default="PLAINTEXT",
        description="Security protocol (PLAINTEXT, SSL, SASL_SSL, etc.)",
    )

    # Messages constraints
    max_in_flight: int = Field(
        default=5,
        ge=1,
        description="Max concurrent unacknowledged requests (1=ordering guarantee)",
    )
    message_max_bytes: int = Field(
        default=1048576,
        ge=0,
        description="Max allowed message size (including headers)",
    )

    # Monitoring
    statistics_interval_ms: int = Field(default=0, ge=0, description="Metrics collection interval (0=disabled)")

    # Schema Registry
    schema_registry_url: str = Field(default="http://localhost:8081", description="Schema Registry server URL")
    schema_registry_timeout: int = Field(
        default=10000,
        ge=1000,
        description="Timeout for Registry API requests in milliseconds",
    )
    schema_registry_max_retries: int = Field(
        default=2,
        gt=0,
        description="Maximum retries for a request",
    )
    schema_registry_retries_wait_ms: int = Field(
        default=1000,
        gt=0,
        description="Maximum time to wait for the first retry. When jitter is applied, the actual wait may be less.",
    )
    schema_registry_cache_capacity: int | None = Field(
        default=1000,
        ge=0,
        description="Max number of schemas to cache locally (None=unlimited)",
    )

    custom_properties: dict[str, Any] = Field(
        default_factory=dict, description="Additional client properties for fine-tuning"
    )

    @classmethod
    def from_env(
        cls: type[SettingsT],
        env_file: str | Path | None = None,
        env_prefix: str = "KAFKA_",
    ) -> SettingsT:
        """
        Load configuration from environment variables and optional .env file.

        Args:
            env_file (str | Path | None): Path to .env file (optional).
            env_prefix (str): Prefix for environment variables (default: 'KAFKA_').

        Returns:
            SettingsT: Instance of KafkaBaseSettings with loaded values.

        Raises:
            FileNotFoundError: If provided env_file does not exist.
        """
        if env_file is not None and not Path(env_file).exists():
            raise FileNotFoundError(f"Environment file not found: {env_file}")

        env_vars = dotenv_values(env_file)
        system_env = dict(os.environ)
        combined_vars = {**system_env, **env_vars}

        config_data = {}
        custom_props = {}
        prefix = env_prefix.upper()

        allowed_fields = cls.model_fields.keys()

        for env_key, env_value in combined_vars.items():
            if env_key.startswith(prefix):
                config_key = env_key[len(prefix) :].lower()
                if config_key in allowed_fields:
                    config_data[config_key] = env_value
                else:
                    custom_props[config_key] = env_value

        config_data["custom_properties"] = custom_props

        return cls.model_validate(config_data)

    @classmethod
    def from_yaml(
        cls: type[SettingsT],
        file_path: str | Path,
        key: str | None = None,
    ) -> SettingsT:
        """
        Load configuration from a YAML file, optionally under a nested key.

        Args:
            file_path (str | Path): Path to the YAML config file.
            key (str | None): Optional key for nested config section.

        Returns:
            SettingsT: Instance of KafkaBaseSettings with loaded values.

        Raises:
            FileNotFoundError: If the YAML file is not found.
            ValueError: If the specified key is missing in the file.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"YAML configuration file not found: {file_path}")

        with open(path, encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        if key:
            if key not in config_data:
                raise ValueError(f"Key '{key}' not found in YAML file")
            config_data = config_data[key]

        allowed_fields = cls.model_fields.keys()
        core_data = {k: v for k, v in config_data.items() if k in allowed_fields}
        custom_props = {k: v for k, v in config_data.items() if k not in allowed_fields}

        core_data["custom_properties"] = custom_props
        return cls.model_validate(core_data)

    @classmethod
    def from_custom_config(
        cls: type[SettingsT],
        config: Any,
        prefix: str = "",
    ) -> SettingsT:
        """
        Load configuration from a custom object, Pydantic model, or dict.

        Args:
            config (Any): Object with attributes or dict-like config.
            prefix (str): Prefix to strip from attribute names.

        Returns:
            SettingsT: Instance of KafkaBaseSettings with loaded values.

        Raises:
            ValueError: If processing a config attribute fails.
        """
        if isinstance(config, BaseModel):
            config_dict = config.model_dump()
        elif hasattr(config, "__dict__"):
            config_dict = vars(config)
        elif isinstance(config, dict):
            config_dict = config
        else:
            config_dict = {
                k: getattr(config, k) for k in dir(config) if not k.startswith("_") and not callable(getattr(config, k))
            }

        allowed_fields = cls.model_fields.keys()
        filtered = {}
        custom_props = {}

        for key, value in config_dict.items():
            try:
                if prefix and key.startswith(prefix):
                    key = key.removeprefix(prefix)

                if key in allowed_fields:
                    filtered[key] = value
                else:
                    custom_props[key] = value

            except Exception as e:
                raise ValueError(f"Error processing config attribute {key}") from e

        filtered["custom_properties"] = custom_props
        return cls.model_validate(filtered)

    @field_validator("bootstrap_servers", mode="before")
    @staticmethod
    def parse_bootstrap_servers(v) -> str:
        """
        Validate and split bootstrap_servers if provided as a comma-separated string.

        Args:
            v (str | list[str]): Bootstrap servers input value.

        Returns:
            List[str]: Parsed list of server addresses.

        Raises:
            ValueError: If the value is not a string or list.
        """
        if isinstance(v, list):
            return ",".join(v)
        if isinstance(v, str):
            return v
        raise ValueError("Incorrect servers was provided")

    def _build_config(self, for_schema_registry: bool = False) -> dict[str, Any]:
        """
        Generate configuration dictionary.

        Args:
            for_schema_registry (bool): If True, build for Schema Registry Client.
            Otherwise, build for Kafka Client.

        Returns:
            Dict[str, Any]: Flat config mapping dotted keys to values.
        """
        config: dict[str, Any] = {}

        for name in self.__class__.model_fields.keys():
            if name.startswith("schema_registry") == for_schema_registry and name:
                value = getattr(self, name)
                if name == "custom_properties":
                    for k, v in value.items():
                        if for_schema_registry:
                            k = k[16:]
                        config[k.replace("_", ".")] = v
                else:
                    if for_schema_registry:
                        name = name[16:]
                    config[name.replace("_", ".")] = value

        custom_props = getattr(self, "custom_properties", {}) or {}
        for name, value in custom_props.items():
            if (
                name.startswith("schema_registry") == for_schema_registry
                and name not in self.__class__.model_fields.keys()
            ):
                if for_schema_registry:
                    name = name[:15]
                config[name.replace("_", ".")] = value

        return config

    def get_config(self) -> dict[str, Any]:
        """
        Generate Kafka client configuration dictionary, excluding Schema Registry settings.

        Returns:
            Dict[str, Any]: Flat config mapping dotted keys to values.
        """
        return self._build_config(for_schema_registry=False)

    def get_schema_registry_config(self) -> dict[str, Any]:
        """
        Generate Schema Registry-specific configuration dictionary.

        Returns:
            Dict[str, Any]: Flat config mapping dotted keys to values under 'schema_registry'.
        """
        return self._build_config(for_schema_registry=True)
