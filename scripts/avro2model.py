"""This module provides helper script to generate AvroEventModel classes from .avsc AVRO files."""

import argparse
import json
import os
from typing import Any

TEMPLATE = '''from typing import ClassVar
from pydantic import Field
from otteroad.avro import AvroEventModel


class {class_name}(AvroEventModel):
    """{model_doc}"""
    
    topic: ClassVar[str] = "{topic}"
    namespace: ClassVar[str] = "{namespace}"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"
    
{fields}
'''


def parse_avro_schema(file_path: str) -> dict[str, Any]:
    """Parse AVRO schema from .avsc file"""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def convert_avro_type(avro_type: Any) -> str:
    """Convert AVRO type to Python type annotation"""
    if isinstance(avro_type, str):
        return {
            "int": "int",
            "long": "int",
            "string": "str",
            "boolean": "bool",
            "bytes": "bytes",
            "float": "float",
            "double": "float",
        }.get(avro_type, "Any")

    if isinstance(avro_type, dict):
        if avro_type["type"] == "array":
            items_type = convert_avro_type(avro_type["items"])
            return f"list[{items_type}]"
        if avro_type["type"] == "map":
            values_type = convert_avro_type(avro_type["values"])
            return f"dict[str, {values_type}]"
        if "logicalType" in avro_type:
            return {"timestamp-millis": "datetime", "date": "date", "uuid": "UUID"}[avro_type["logicalType"]]

    if isinstance(avro_type, list):
        types = [convert_avro_type(t) for t in avro_type]
        if "null" in types:
            non_null = [t for t in types if t != "null"]
            return f"{non_null[0]} | None" if len(non_null) == 1 else " | ".join(types)
        return " | ".join(types)

    return "Any"


def generate_python_class(schema: dict[str, Any], output_dir: str):
    """Generate Python class from parsed AVRO schema"""
    # Extract basic schema information
    class_name = schema["name"]
    namespace = schema.get("namespace", "")
    fields = schema["fields"]
    model_doc = schema.get("doc", f"Auto-generated model for {class_name} event")

    # Determine topic from namespace (first two components)
    topic_parts, namespace = namespace.split(".")[:2], namespace.split(".")[-1]
    topic = ".".join(topic_parts) if len(topic_parts) >= 2 else "default.topic"

    # Process fields and collect imports
    imports = set()
    field_lines = []

    for field in fields:
        field_name = field["name"]
        field_type = convert_avro_type(field["type"])
        field_doc = field.get("doc", f"Field representing {field_name}")

        # Detect special types for imports
        if "datetime" in field_type:
            imports.add("datetime")
        if "date" in field_type:
            imports.add("date")
        if "UUID" in field_type:
            imports.add("UUID")

        field_lines.append(f'    {field_name}: {field_type} = Field(..., description="{field_doc}")')

    # Prepare final template
    imports_str = ", ".join(sorted(imports)) if imports else ""
    class_content = TEMPLATE.format(
        imports=imports_str,
        class_name=class_name,
        topic=topic,
        namespace=namespace,
        model_doc=model_doc,
        fields="\n".join(field_lines),
    )

    # Create output directory if needed
    output_dir = os.path.join(output_dir, "_".join(topic.split(".")), namespace)
    os.makedirs(output_dir, exist_ok=True)

    # Write generated class to file
    output_path = os.path.join(output_dir, f"{class_name}.py")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(class_content)

    print(f"Generated: {output_path}")


def process_avro_schemas(input_dir: str, output_dir: str):
    """Process all .avsc files in input directory"""
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".avsc"):
                file_path = os.path.join(root, file)
                try:
                    schema = parse_avro_schema(file_path)
                    generate_python_class(schema, output_dir)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"Error processing {file_path}: {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Python models from AVRO schemas")
    parser.add_argument("input_dir", help="Directory containing .avsc files")
    parser.add_argument("output_dir", help="Output directory for generated models")

    args = parser.parse_args()

    print(f"🔍 Processing AVRO schemas in: {args.input_dir}")
    process_avro_schemas(args.input_dir, args.output_dir)
    print("\n✅ Generation completed!")
