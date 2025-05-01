from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class RegionalScenarioObjectsUpdated(AvroEventModel):
    """Model for message indicates that urban objects have been updated for regional scenario."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "regional_scenarios"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="regional scenario identifier for which urban objects have been updated")
    territory_id: int = Field(..., description="region territory identifier for which scenario has been created")
    service_types: list[int] = Field(..., description="list of service types identifiers which have been updated")
    physical_object_types: list[int] = Field(
        ..., description="list of physical object types identifiers which have been updated"
    )
