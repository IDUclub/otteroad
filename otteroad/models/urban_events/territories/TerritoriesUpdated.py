from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class TerritoriesUpdated(AvroEventModel):
    """Model for message indicates that a territories has been created or updated."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "territories"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_ids: list[int] = Field(..., description="list of territories identifiers which were created or updated")
