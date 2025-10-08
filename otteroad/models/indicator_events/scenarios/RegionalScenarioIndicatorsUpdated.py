from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionalScenarioIndicatorsUpdated(AvroEventModel):
    """Model for message indicates that indicator values have been updated for all hexagons in regional scenario."""

    topic: ClassVar[str] = "indicator.events"
    namespace: ClassVar[str] = "scenarios"
    schema_version: ClassVar[int] = 2
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="regional scenario identifier for which values have been updated")
    territory_id: int = Field(..., description="region territory identifier for which scenario has been created")
    indicator_id: int = Field(..., description="updated indicator identifier for which value has been updated")
