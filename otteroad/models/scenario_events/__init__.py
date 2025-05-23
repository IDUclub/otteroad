"""All models for topic `scenario.events` are defined here."""

from .projects.BaseScenarioCreated import BaseScenarioCreated
from .projects.ProjectCreated import ProjectCreated
from .projects.ScenarioObjectsUpdated import ScenarioObjectsUpdated
from .projects.ScenarioZonesUpdated import ScenarioZonesUpdated
from .regional_scenarios.RegionalScenarioCreated import RegionalScenarioCreated

__all__ = [
    "BaseScenarioCreated",
    "ProjectCreated",
    "ScenarioZonesUpdated",
    "ScenarioObjectsUpdated",
    "RegionalScenarioCreated",
]
