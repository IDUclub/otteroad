from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class FunctionalZonesUpdated(AvroEventModel):
    """Model for message indicates that a functional zones have been updated for territory."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "functional_zones"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="territory identifier where zones have been updated")
