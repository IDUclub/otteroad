from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class BaseScenarioCreated(AvroEventModel):
    """Model for message indicates that a project's base scenario has been created."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "projects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    project_id: int = Field(..., description="unique project identifier")
    base_scenario_id: int = Field(..., description="new project base scenario identifier")
    regional_scenario_id: int = Field(
        ..., description="unique regional scenario identifier for which base scenario was created"
    )
