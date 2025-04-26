"""
Provides base class for defining Avro-serializable event models with:
- Automatic Avro schema generation from Python types
- Schema Registry integration
- Bidirectional serialization/deserialization
- Compatibility checking
- Pydantic validation
"""

import io
import json
import struct
from datetime import date, datetime
from enum import Enum
from types import UnionType
from typing import Any, ClassVar, Union, get_args, get_origin
from uuid import UUID

import fastavro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, SchemaRegistryError
from fastavro.schema import parse_schema
from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined


class AvroEventModel(BaseModel):
    """
    Base model for Avro-serializable events with Schema Registry integration.

    Features:
    - Automatic Avro schema generation from type hints
    - Pydantic validation with frozen instances
    - Schema evolution compatibility checking
    - Confluent Schema Registry support
    - Efficient binary serialization/deserialization

    Class Variables:
        topic: Kafka topic name (required)
        namespace: Schema Registry namespace (required)
        schema_version: Schema version (default: 1)
        schema_compatibility: Compatibility mode (default: "BACKWARD")

    Example:
        class UserEvent(AvroEventModel):
            topic: ClassVar[str] = "user.events"
            user_id: UUID
            action: str
            timestamp: datetime = datetime.now()
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    # Schema Registry metadata
    topic: ClassVar[str]
    namespace: ClassVar[str]
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    # Cached schema data
    _schema_subject: ClassVar[str | None] = None
    _avro_schema: ClassVar[dict | None] = None
    _schema_id: ClassVar[int | None] = None
    _writer_schema: ClassVar[str | None] = None

    @classmethod
    def avro_schema(cls) -> dict:
        """
        Get generated Avro schema for this model.

        Returns:
            dict: Avro schema in JSON format

        Raises:
            fastavro.SchemaParseError: If generated schema is invalid
        """
        if cls._avro_schema is None:
            cls._avro_schema = cls._generate_avro_schema()
            parse_schema(cls._avro_schema)  # Validate schema structure
        return cls._avro_schema

    @classmethod
    def _generate_avro_schema(cls) -> dict:
        """
        Generate Avro schema from model fields.

        Returns:
            dict: Complete Avro schema dictionary

        Note:
            Automatically handles nested models, enums, and complex types
        """
        fields = []
        for field_name, field_info in cls.model_fields.items():
            avro_type = cls._convert_type(field_info.annotation, field_info)
            field_schema = {
                "name": field_name,
                "type": avro_type,
            }

            if field_info.default is not None and field_info.default is not PydanticUndefined:
                field_schema["default"] = cls._process_default_value(field_info.default)
            elif not field_info.is_required():
                field_schema["default"] = None

            if field_info.description is not None:
                field_schema["doc"] = field_info.description

            fields.append(field_schema)

        result = {
            "type": "record",
            "name": cls.__name__,
            "namespace": f"{cls.topic}.{cls.namespace}",
            "fields": fields,
        }

        if cls.__doc__ is not None:
            result["doc"] = cls.__doc__

        return result

    @classmethod
    def _convert_type(cls, py_type: Any, field: FieldInfo) -> Any:
        """
        Convert Python type to Avro schema type.

        Args:
            py_type: Python type annotation
            field: Pydantic field metadata

        Returns:
            Any: Corresponding Avro type definition

        Raises:
            TypeError: For unsupported types

        Note:
            Handles nested models, enums, and standard type mappings
        """
        type_map = {
            str: "string",
            int: "long",
            float: "double",
            bool: "boolean",
            bytes: "bytes",
            datetime: {"type": "long", "logicalType": "timestamp-millis"},
            date: {"type": "int", "logicalType": "date"},
            UUID: {"type": "string", "logicalType": "uuid"},
        }

        origin = get_origin(py_type)

        # Handle Optional[T] or Union[T, None]
        if origin is Union or origin is UnionType:  # pylint: disable=consider-alternative-union-syntax
            args = get_args(py_type)
            non_null_types = [t for t in args if t is not type(None)]
            if len(non_null_types) == 1:
                return ["null", cls._convert_type(non_null_types[0], field)]
            return ["null"] + [cls._convert_type(t, field) for t in non_null_types]

        # Handle List[T]
        if origin is list:
            return {
                "type": "array",
                "items": cls._convert_type(get_args(py_type)[0], field),
            }

        # Handle Dict[K, V]
        if origin is dict:
            return {
                "type": "map",
                "values": cls._convert_type(get_args(py_type)[1], field),
            }

        # Handle nested models and enums
        if isinstance(py_type, type):
            if issubclass(py_type, AvroEventModel):
                return py_type.avro_schema()
            if issubclass(py_type, Enum):
                return {
                    "type": "enum",
                    "name": py_type.__name__,
                    "symbols": [e.name for e in py_type],
                    "doc": py_type.__doc__,
                }

        return type_map.get(py_type, "string")  # Fallback to string

    @classmethod
    def _process_default_value(cls, default_value: Any) -> Any:
        """
        Process default values for the Avro schema.

        Args:
            default_value: The default value

        Returns:
            Processed default value
        """
        # Dictionary of types and their conversions
        type_conversions = {
            datetime: lambda value: int(value.timestamp() * 1000),  # Convert datetime to timestamp-millis
            UUID: str,  # Convert UUID to string
            date: lambda value: (value - date(1970, 1, 1)).days,  # Convert date to days since epoch
        }

        if default_value is not None and default_value is not PydanticUndefined:
            # Convert the default value according to its type
            for py_type, conversion in type_conversions.items():
                if isinstance(default_value, py_type):
                    return conversion(default_value)
            return default_value
        return None

    @classmethod
    def register_schema(cls, registry: SchemaRegistryClient) -> int:
        """
        Register schema in Schema Registry.

        Args:
            registry: Schema Registry client instance

        Returns:
            int: Registered schema ID

        Raises:
            SchemaRegistryError: On registration failure
        """
        if cls._schema_id is not None:
            return cls._schema_id

        schema_str = json.dumps(cls.avro_schema())
        schema_obj = Schema(schema_str, schema_type="AVRO")
        cls._schema_id = registry.register_schema(
            subject_name=cls.schema_subject(),
            schema=schema_obj,
        )
        return cls._schema_id

    @classmethod
    def is_compatible_with(cls, registry: SchemaRegistryClient) -> bool:
        """
        Check schema compatibility with Registry.

        Args:
            registry: Schema Registry client instance

        Returns:
            bool: True if compatible with latest version
        """
        try:
            registry.set_compatibility(subject_name=cls.schema_subject(), level=cls.schema_compatibility)

            schema_str = json.dumps(cls.avro_schema())
            schema_obj = Schema(schema_str, schema_type="AVRO")
            return registry.test_compatibility(
                subject_name=cls.schema_subject(),
                schema=schema_obj,
                version="latest",
            )
        except SchemaRegistryError:
            return False

    def to_avro_dict(self) -> dict:
        """
        Convert model instance to Avro-compatible dictionary.

        Returns:
            dict: Data ready for Avro serialization

        Note:
            Recursively converts dates, UUIDs, and nested models
        """

        def convert(value: Any) -> Any:
            result = value

            if isinstance(value, datetime):
                result = int(value.timestamp() * 1000)
            elif isinstance(value, date):
                result = (value - date(1970, 1, 1)).days
            elif isinstance(value, UUID):
                result = str(value)
            elif isinstance(value, list):
                result = [convert(v) for v in value]
            elif isinstance(value, dict):
                result = {k: convert(v) for k, v in value.items()}
            elif isinstance(value, AvroEventModel):
                result = value.to_avro_dict()
            elif isinstance(value, Enum):
                result = value.name

            return result

        return {k: convert(v) for k, v in self.model_dump().items()}

    def serialize(self, registry: SchemaRegistryClient) -> bytes:
        """
        Serialize instance to Confluent Avro format.

        Args:
            registry: Schema Registry client instance

        Returns:
            bytes: Serialized message with header

        Raises:
            ValueError: On serialization failure
        """
        schema_id = self.register_schema(registry)

        with io.BytesIO() as buffer:
            # Write Confluent wire format header
            buffer.write(struct.pack(">bI", 0, schema_id))

            # Write Avro payload
            fastavro.schemaless_writer(buffer, self.avro_schema(), self.to_avro_dict())
            return buffer.getvalue()

    @classmethod
    def deserialize(cls, data: bytes, registry: SchemaRegistryClient) -> "AvroEventModel":
        """
        Deserialize Confluent Avro message to model instance.

        Args:
            data: Serialized message bytes
            registry: Schema Registry client instance

        Returns:
            AvroEventModel: Reconstructed model instance

        Raises:
            ValueError: On deserialization failure
        """
        if len(data) < 5:
            raise ValueError("Invalid message format: missing header")

        try:
            # Extract Confluent header
            magic, schema_id = struct.unpack(">bI", data[:5])
            if magic != 0:
                raise ValueError("Invalid magic byte in header")

            # Get writer schema from Registry
            writer_schema = cls._writer_schema or json.loads(registry.get_schema(schema_id).schema_str)
            reader_schema = cls.avro_schema()

            # Deserialize payload
            with io.BytesIO(data[5:]) as buffer:
                record = fastavro.schemaless_reader(buffer, parse_schema(reader_schema), parse_schema(writer_schema))

            return cls.model_validate(record)

        except (SchemaRegistryError, json.JSONDecodeError, fastavro.read.SchemaResolutionError) as e:
            raise ValueError(f"Deserialization failed: {str(e)}") from e

    @classmethod
    def schema_subject(cls) -> str:
        """String representation of schema subject."""
        if cls._schema_subject is None:
            cls._schema_subject = f"{cls.topic}.{cls.namespace}.{cls.__name__}"
        return cls._schema_subject

    def __str__(self) -> str:  # pylint: disable=invalid-str-returned
        """String representation showing model origin."""
        return self.__class__.schema_subject()

    def __repr__(self) -> str:
        """Official string representation for debugging."""
        return f"<{self.__class__.__name__} {super().__repr__()}>"
