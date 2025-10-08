from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionalScenarioMatrixUpdated(AvroEventModel):
    """Model for message indicates that transport matrix have been updated for regional scenario."""

    topic: ClassVar[str] = "graph.events"
    namespace: ClassVar[str] = "scenarios"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="regional scenario identifier for which matrix have been updated")
    territory_id: int = Field(..., description="region territory identifier for which scenario has been created")
