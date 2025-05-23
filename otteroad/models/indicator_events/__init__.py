"""All models for topic `indicator.events` are defined here."""

from .public.IndicatorValuesUpdated import IndicatorValuesUpdated
from .scenarios.RegionalScenarioIndicatorsUpdated import RegionalScenarioIndicatorsUpdated
from .scenarios.ScenarioIndicatorsUpdated import ScenarioIndicatorsUpdated

__all__ = [
    "IndicatorValuesUpdated",
    "RegionalScenarioIndicatorsUpdated",
    "ScenarioIndicatorsUpdated",
]
