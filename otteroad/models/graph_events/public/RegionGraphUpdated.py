from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionGraphUpdated(AvroEventModel):
    """Model for message indicates that the graph of a region has been updated (a new version is active)."""

    topic: ClassVar[str] = "graph.events"
    namespace: ClassVar[str] = "public"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="region territory identifier whose graph has been updated")
    graph_kind: str = Field(..., description="graph kind: walk, drive, bike, pt_osm or pt_gtfs")
    version_id: int = Field(..., description="identifier of the graph version that became active")
