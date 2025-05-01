from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class ScenarioObjectsUpdated(AvroEventModel):
    """Model for message indicates that urban objects have been updated for project scenario."""

    topic: ClassVar[str] = "scenario.events"
    namespace: ClassVar[str] = "projects"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    project_id: int = Field(..., description="project identifier where scenario has been updated")
    scenario_id: int = Field(..., description="scenario identifier for which urban objects have been updated")
    service_types: list[int] = Field(..., description="list of service types identifiers which have been updated")
    physical_object_types: list[int] = Field(
        ..., description="list of physical object types identifiers which have been updated"
    )
