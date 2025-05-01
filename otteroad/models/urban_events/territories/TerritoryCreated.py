from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class TerritoryCreated(AvroEventModel):
    """Model for message indicates that a territory has been created."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "territories"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="new territory identifier")
