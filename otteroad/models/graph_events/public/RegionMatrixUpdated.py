from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionMatrixUpdated(AvroEventModel):
    """Model for message indicates that transport matrix have been updated for region."""

    topic: ClassVar[str] = "graph.events"
    namespace: ClassVar[str] = "public"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="region territory identifier for which matrix has been updated")
