from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class ScenarioZonesUpdated(AvroEventModel):
    """Model for message indicates that functional zones have been updated for project scenario."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "projects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    project_id: int = Field(..., description="project identifier where scenario has been updated")
    scenario_id: int = Field(..., description="scenario identifier where zones have been updated")
