from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionalScenarioCreated(AvroEventModel):
    """Model for message indicates that a regional scenario has been created."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "regional_scenarios"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="new regional scenario identifier")
    territory_id: int = Field(..., description="region territory identifier for which scenario has been created")
