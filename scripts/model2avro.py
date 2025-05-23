"""This module provides helper script to generate .avsc AVRO files from AvroEventModel classes."""

import argparse
import importlib.util
import inspect
import json
import os
import sys
from pathlib import Path

from otteroad.avro import AvroEventModel


def load_models_from_dir(models_dir: str) -> list[type[AvroEventModel]]:
    """Discover all AvroEventModel subclasses in directory"""
    models = []
    models_dir_path = Path(models_dir)

    # Add directory to Python path
    sys.path.insert(0, str(models_dir_path.parent))

    for root, _, files in os.walk(models_dir):  # pylint: disable=too-many-nested-blocks
        for file in files:
            if file.endswith(".py") and not file.startswith("__"):
                module_path = Path(root) / file
                module_name = module_path.with_suffix("").relative_to(models_dir_path).as_posix().replace("/", ".")

                try:
                    # Load module dynamically
                    spec = importlib.util.spec_from_file_location(module_name, module_path)
                    if spec and spec.loader:
                        module = importlib.util.module_from_spec(spec)
                        spec.loader.exec_module(module)

                        # Find all AvroEventModel subclasses
                        for _, obj in inspect.getmembers(module):
                            if inspect.isclass(obj) and issubclass(obj, AvroEventModel) and obj != AvroEventModel:
                                models.append(obj)
                except Exception as e:  # pylint: disable=broad-exception-caught
                    print(f"Error loading module {module_path}: {str(e)}")

    return models


def save_avro_schema(model: type[AvroEventModel], output_dir: str):
    """Save AVRO schema to .avsc file using model's metadata"""
    try:
        schema = model.avro_schema()
        namespace = schema.get("namespace", "").rsplit(".", maxsplit=1)
        namespace[0] = namespace[0].replace(".", "_")

        # Create directory structure based on namespace
        schema_dir = os.path.join(output_dir, *namespace)
        os.makedirs(schema_dir, exist_ok=True)

        file_path = os.path.join(schema_dir, f"{model.__name__}.avsc")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)

        print(f"Generated: {file_path}")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error generating schema for {model.__name__}: {str(e)}")


def process_python_models(input_dir: str, output_dir: str):
    """Process all Python models in input directory"""
    models = load_models_from_dir(input_dir)

    for model in models:
        save_avro_schema(model, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate AVRO schemas from AvroEventModel classes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_dir", help="Directory containing Python models hierarchy")
    parser.add_argument("output_dir", help="Output directory for generated .avsc files")

    args = parser.parse_args()

    print(f"🔍 Searching for AvroEventModel classes in: {args.input_dir}")
    process_python_models(args.input_dir, args.output_dir)
    print("\n✅ AVRO schema generation completed!")
