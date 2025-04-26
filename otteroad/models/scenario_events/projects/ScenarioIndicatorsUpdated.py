from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class ScenarioIndicatorsUpdated(AvroEventModel):
    """Model for message indicates that an indicator value has been updated for project scenario."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "projects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    project_id: int = Field(..., description="project identifier where scenario has been updated")
    scenario_id: int = Field(..., description="scenario identifier for which indicator value has been updated")
    indicator_id: int = Field(..., description="updated indicator identifier for which value has been updated")
