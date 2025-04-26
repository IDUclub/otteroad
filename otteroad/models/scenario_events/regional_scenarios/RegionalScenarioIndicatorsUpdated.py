from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionalScenarioIndicatorsUpdated(AvroEventModel):
    """Model for message indicates that an indicator value has been updated for regional scenario."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "regional_scenarios"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="regional scenario identifier for which indicator value has been updated")
    territory_id: int = Field(..., description="region territory identifier for which scenario has been created")
    indicator_id: int = Field(..., description="updated indicator identifier for which value has been updated")
