from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class TerritoriesDeleted(AvroEventModel):
    """Model for message indicates that a territories has been deleted."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "territories"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    parent_ids: list[int] = Field(
        ..., description="list of region territories identifiers for which child territory has been deleted"
    )
