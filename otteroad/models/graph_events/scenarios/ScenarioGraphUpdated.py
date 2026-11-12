from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class ScenarioGraphUpdated(AvroEventModel):
    """Model for message indicates that the graph edits of a scenario have been changed."""

    topic: ClassVar[str] = "graph.events"
    namespace: ClassVar[str] = "scenarios"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    scenario_id: int = Field(..., description="scenario identifier whose graph edits have been changed")
    project_id: int = Field(..., description="project identifier of the scenario")
    graph_kinds: list[str] = Field(..., description="graph kinds whose edits changed: walk, drive or bike")
