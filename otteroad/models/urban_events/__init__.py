"""All models for topic `scenario.events` are defined here."""

from .functional_zones.FunctionalZonesUpdated import FunctionalZonesUpdated
from .soc_groups.SocGroupsUpdated import SocGroupsUpdated
from .territories.TerritoriesDeleted import TerritoriesDeleted
from .territories.TerritoriesUpdated import TerritoriesUpdated
from .urban_objects.UrbanObjectsUpdated import UrbanObjectsUpdated

__all__ = [
    "FunctionalZonesUpdated",
    "SocGroupsUpdated",
    "TerritoriesDeleted",
    "TerritoriesUpdated",
    "UrbanObjectsUpdated",
]
