from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class IndicatorValuesUpdated(AvroEventModel):
    """Model for message indicates that an indicator value has been updated for territory."""

    topic: ClassVar[str] = "indicator.events"
    namespace: ClassVar[str] = "public"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="territory identifier where indicator value has been updated")
    indicator_id: int = Field(..., description="indicator identifier for which value has been updated")
    indicator_value_id: int = Field(..., description="updated indicator value identifier")
