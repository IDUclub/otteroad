"""All models for topic `scenario.events` are defined here."""

from .functional_zones.FunctionalZonesUpdated import FunctionalZonesUpdated
from .indicators.IndicatorValuesUpdated import IndicatorValuesUpdated
from .soc_groups.SocGroupsUpdated import SocGroupsUpdated
from .territories.TerritoryCreated import TerritoryCreated
from .territories.TerritoryDeleted import TerritoryDeleted
from .territories.TerritoryUpdated import TerritoryUpdated
from .urban_objects.UrbanObjectsUpdated import UrbanObjectsUpdated

__all__ = [
    "FunctionalZonesUpdated",
    "IndicatorValuesUpdated",
    "SocGroupsUpdated",
    "TerritoryCreated",
    "TerritoryDeleted",
    "TerritoryUpdated",
    "UrbanObjectsUpdated",
]
