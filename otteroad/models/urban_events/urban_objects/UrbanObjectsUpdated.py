from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class UrbanObjectsUpdated(AvroEventModel):
    """Model for message indicates that urban objects have been updated for territory."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "urban_objects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="territory identifier where objects have been updated")
    service_types: list[int] = Field(..., description="list of service types identifiers which have been updated")
    physical_object_types: list[int] = Field(
        ..., description="list of physical object types identifiers which have been updated"
    )
