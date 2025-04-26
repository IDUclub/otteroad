"""All models for topic `scenario.events` are defined here."""

from .projects.ProjectCreated import ProjectCreated
from .projects.ScenarioObjectsUpdated import ScenarioObjectsUpdated
from .projects.ScenarioZonesUpdated import ScenarioZonesUpdated
from .regional_scenarios.RegionalScenarioCreated import RegionalScenarioCreated
from .regional_scenarios.RegionalScenarioIndicatorsUpdated import RegionalScenarioIndicatorsUpdated
from .regional_scenarios.RegionalScenarioObjectsUpdated import RegionalScenarioObjectsUpdated

__all__ = [
    "ProjectCreated",
    "ScenarioZonesUpdated",
    "ScenarioObjectsUpdated",
    "RegionalScenarioCreated",
    "RegionalScenarioIndicatorsUpdated",
    "RegionalScenarioObjectsUpdated",
]
