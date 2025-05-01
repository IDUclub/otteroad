from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class TerritoryDeleted(AvroEventModel):
    """Model for message indicates that a territory has been deleted."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "territories"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    parent_id: int = Field(..., description="parent territory identifier for which child territory has been deleted")
