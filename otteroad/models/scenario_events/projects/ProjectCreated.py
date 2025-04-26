from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class ProjectCreated(AvroEventModel):
    """Model for message indicates that a project has been created."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "projects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    project_id: int = Field(..., description="new project identifier")
    base_scenario_id: int = Field(..., description="new project base scenario identifier")
    territory_id: int = Field(..., description="region territory identifier where project has been created")
